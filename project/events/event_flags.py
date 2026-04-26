from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import numpy as np
import pandas as pd

from project.core.column_registry import ColumnRegistry
from project.core.feature_schema import feature_dataset_dir_name
from project.core.validation import ts_ns_utc
from project.events.event_specs import (
    EVENT_REGISTRY_SPECS,
    REGISTRY_BACKED_SIGNALS,
)
from project.io.utils import (
    choose_partition_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)


def _active_signal_column(signal_column: str) -> str:
    signal = str(signal_column).strip()
    if signal.endswith("_event"):
        return f"{signal[:-6]}_active"
    return f"{signal}_active"


def _signal_ts_column(signal_column: str) -> str:
    signal = str(signal_column).strip()
    if signal.endswith("_event"):
        return f"{signal[:-6]}_signal"
    return f"{signal}_signal"


def _direction_signal_column(signal_column: str) -> str:
    signal = str(signal_column).strip()
    for event_type, spec in EVENT_REGISTRY_SPECS.items():
        if str(spec.signal_column).strip() == signal:
            return ColumnRegistry.event_direction_cols(event_type)[0]
    if signal.endswith("_event"):
        return f"{signal[:-6]}_direction"
    return f"{signal}_direction"


def _direction_to_sign(direction: object) -> int:
    token = str(direction or "").strip().lower()
    if token in {"up", "long", "buy", "pos", "positive", "1", "+1"}:
        return 1
    if token in {"down", "short", "sell", "neg", "negative", "-1"}:
        return -1
    return 0


def _load_symbol_timestamps(
    data_root: Path, run_id: str, symbol: str, timeframe: str = "5m"
) -> pd.Series:
    feature_dataset = feature_dataset_dir_name()
    candidates = [
        run_scoped_lake_path(
            data_root, run_id, "features", "perp", symbol, timeframe, feature_dataset
        ),
        Path(data_root) / "lake" / "features" / "perp" / symbol / timeframe / feature_dataset,
    ]
    src = choose_partition_dir(candidates)
    files = list_parquet_files(src) if src else []
    if not files:
        return pd.Series(dtype="datetime64[ns, UTC]")
    frame = read_parquet(files)
    if frame.empty or "timestamp" not in frame.columns:
        return pd.Series(dtype="datetime64[ns, UTC]")
    ts = ts_ns_utc(frame["timestamp"])
    if ts.empty:
        return pd.Series(dtype="datetime64[ns, UTC]")
    return pd.Series(sorted(pd.DatetimeIndex(ts).unique()))


def build_event_flags(
    *,
    events: pd.DataFrame,
    symbols: Sequence[str],
    data_root: Path,
    run_id: str,
    timeframe: str = "5m",
    _ts_loader=None,
) -> pd.DataFrame:
    if _ts_loader is None:
        _ts_loader = _load_symbol_timestamps
    symbols_clean = [str(s).strip().upper() for s in symbols if str(s).strip()]
    symbols_clean = list(dict.fromkeys(symbols_clean))
    if not symbols_clean and not events.empty:
        symbols_clean = sorted(
            set(events["symbol"].dropna().astype(str).str.upper().tolist()) - {"ALL"}
        )

    grids = []
    for symbol in symbols_clean:
        ts = _ts_loader(data_root=data_root, run_id=run_id, symbol=symbol, timeframe=timeframe)
        if ts.empty:
            event_ts = pd.Series(dtype="datetime64[ns, UTC]")
            if not events.empty:
                local = events[(events["symbol"] == symbol) | (events["symbol"] == "ALL")]
                event_ts = pd.to_datetime(
                    local.get("timestamp", pd.Series(dtype=object)),
                    utc=True,
                    errors="coerce",
                ).dropna()
            ts = (
                pd.Series(sorted(pd.DatetimeIndex(event_ts).unique())) if not event_ts.empty else ts
            )

        if not ts.empty:
            grids.append(pd.DataFrame({"timestamp": ts_ns_utc(ts), "symbol": symbol}))

    all_bool_cols: list[str] = []
    all_direction_cols: list[str] = []
    for sig in sorted(REGISTRY_BACKED_SIGNALS):
        all_bool_cols.append(sig)
        all_bool_cols.append(_active_signal_column(sig))
        all_bool_cols.append(_signal_ts_column(sig))
        all_direction_cols.append(_direction_signal_column(sig))

    if not grids:
        return pd.DataFrame(columns=["timestamp", "symbol", *all_bool_cols, *all_direction_cols])

    full_grid = pd.concat(grids, ignore_index=True)

    bool_part = pd.DataFrame(
        np.zeros((len(full_grid), len(all_bool_cols)), dtype=bool),
        columns=all_bool_cols,
        index=full_grid.index,
    )
    direction_part = pd.DataFrame(
        np.full((len(full_grid), len(all_direction_cols)), np.nan, dtype=float),
        columns=all_direction_cols,
        index=full_grid.index,
    )
    full_grid = pd.concat([full_grid, bool_part, direction_part], axis=1)

    if events.empty:
        return full_grid

    ev = events.copy()
    ev["timestamp"] = ts_ns_utc(ev["timestamp"])
    ev["enter_ts"] = ts_ns_utc(ev["enter_ts"])
    ev["exit_ts"] = ts_ns_utc(ev["exit_ts"])
    ev["signal_ts"] = ts_ns_utc(ev.get("signal_ts", ev["timestamp"]))
    if "detected_ts" in ev.columns:
        ev["_detected_ts"] = ts_ns_utc(ev["detected_ts"])
    else:
        ev["_detected_ts"] = ev["enter_ts"]
    if "direction" not in ev.columns:
        ev["direction"] = "non_directional"

    ev = ev[ev["signal_column"].isin(REGISTRY_BACKED_SIGNALS)].copy()
    if ev.empty:
        return full_grid

    all_events = ev[ev["symbol"] == "ALL"]
    specific_events = ev[ev["symbol"] != "ALL"]

    expanded_rows = []
    if not all_events.empty:
        for symbol in symbols_clean:
            temp = all_events.copy()
            temp["symbol"] = symbol
            expanded_rows.append(temp)

    if expanded_rows:
        ev = pd.concat([specific_events] + expanded_rows, ignore_index=True)
    else:
        ev = specific_events

    for symbol, group in full_grid.groupby("symbol"):
        sym_events = ev[ev["symbol"] == symbol]
        if sym_events.empty:
            continue

        grid_ts_naive = group["timestamp"].dt.tz_localize(None).values
        grid_indices = group.index

        # 1. Detection signal (at detection bar t)
        sig_ts_naive = sym_events["signal_ts"].dt.tz_localize(None).values
        idx_detection = np.searchsorted(grid_ts_naive, sig_ts_naive, side="right") - 1

        valid_det = (idx_detection >= 0) & (idx_detection < len(grid_ts_naive))
        if valid_det.any():
            target_indices = grid_indices[idx_detection[valid_det]]
            target_signals = sym_events["signal_column"].values[valid_det]
            target_directions = (
                sym_events["direction"]
                .map(_direction_to_sign)
                .astype(float)
                .values[valid_det]
            )
            for sig_col in np.unique(target_signals):
                hits = target_indices[target_signals == sig_col]
                full_grid.loc[hits, sig_col] = True
            for idx, sig_col, sign in zip(
                target_indices, target_signals, target_directions, strict=False
            ):
                dir_col = _direction_signal_column(sig_col)
                existing = full_grid.at[idx, dir_col]
                if pd.isna(existing):
                    full_grid.at[idx, dir_col] = float(sign)
                elif float(existing) != float(sign):
                    full_grid.at[idx, dir_col] = 0.0

        # 2. Tradable signal and Active window start (at t+1)
        # We use side="right" to find the open time of the bar AFTER detection.
        # This ensures earliest tradable bar is t+1.
        idx_tradable = np.searchsorted(grid_ts_naive, sig_ts_naive, side="right")

        # 3. Active window end
        exit_ts_naive = sym_events["exit_ts"].dt.tz_localize(None).values
        idx_exit = np.searchsorted(grid_ts_naive, exit_ts_naive, side="right") - 1

        for sig_col in np.unique(sym_events["signal_column"]):
            sig_mask = sym_events["signal_column"] == sig_col

            # Set Tradable Signal (_signal column)
            tradable_col = _signal_ts_column(sig_col)
            valid_sig = idx_tradable[sig_mask] < len(grid_ts_naive)
            if valid_sig.any():
                sig_hits = grid_indices[idx_tradable[sig_mask][valid_sig]]
                full_grid.loc[sig_hits, tradable_col] = True

            # Set Active Window (_active column)
            # Starts at tradable bar (idx_tradable), ends at exit bar.
            # This ensures no lookahead relative to the tradability contract.
            active_col = _active_signal_column(sig_col)
            diff = np.zeros(len(grid_ts_naive) + 1, dtype=int)
            starts = idx_tradable[sig_mask]
            exits = idx_exit[sig_mask]

            valid_ranges = (starts < len(grid_ts_naive)) & (exits >= 0) & (starts <= exits)
            if valid_ranges.any():
                starts_v = np.maximum(0, starts[valid_ranges])
                exits_v = np.minimum(len(grid_ts_naive) - 1, exits[valid_ranges])
                np.add.at(diff, starts_v, 1)
                np.add.at(diff, exits_v + 1, -1)
                active_mask = np.cumsum(diff)[:-1] > 0
                full_grid.loc[grid_indices[active_mask], active_col] = True

    return full_grid.sort_values(["timestamp", "symbol"]).reset_index(drop=True)


def load_registry_flags(data_root: Path, run_id: str, symbol: str | None = None) -> pd.DataFrame:
    from project.events.event_repository import _read_registry_stem

    flags = _read_registry_stem(data_root=data_root, run_id=run_id, stem="event_flags")
    if flags.empty:
        cols = ["timestamp", "symbol"]
        for signal in sorted(REGISTRY_BACKED_SIGNALS):
            cols.extend(
                [
                    signal,
                    _active_signal_column(signal),
                    _signal_ts_column(signal),
                    _direction_signal_column(signal),
                ]
            )
        return pd.DataFrame(columns=cols)

    flags["timestamp"] = pd.to_datetime(flags.get("timestamp"), utc=True, errors="coerce")
    flags = flags.dropna(subset=["timestamp"]).copy()
    if symbol is not None:
        symbol_norm = str(symbol).strip().upper()
        flags = flags[flags["symbol"].astype(str).str.upper() == symbol_norm].copy()

    cols = ["timestamp", "symbol"]
    for signal in sorted(REGISTRY_BACKED_SIGNALS):
        cols.extend(
            [
                signal,
                _active_signal_column(signal),
                _signal_ts_column(signal),
                _direction_signal_column(signal),
            ]
        )

    missing = [c for c in cols if c not in flags.columns]
    if missing:
        fill = pd.DataFrame(index=flags.index)
        for c in missing:
            fill[c] = np.nan if c.startswith("evt_direction_") or c.endswith("_direction") else False
        flags = pd.concat([flags, fill], axis=1)
    flags = flags[cols].copy()

    for signal in sorted(REGISTRY_BACKED_SIGNALS):
        flags[signal] = flags[signal].where(flags[signal].notna(), False).astype(bool)
        active_col = _active_signal_column(signal)
        flags[active_col] = flags[active_col].where(flags[active_col].notna(), False).astype(bool)
        ts_col = _signal_ts_column(signal)
        flags[ts_col] = flags[ts_col].where(flags[ts_col].notna(), False).astype(bool)
        dir_col = _direction_signal_column(signal)
        flags[dir_col] = pd.to_numeric(flags[dir_col], errors="coerce").astype(float)

    return flags.sort_values(["timestamp", "symbol"]).reset_index(drop=True)


def merge_event_flags_for_selected_event_types(
    *,
    existing_flags: pd.DataFrame,
    recomputed_flags: pd.DataFrame,
    selected_event_types: Sequence[str],
) -> pd.DataFrame:
    selected = [
        str(event_type).strip() for event_type in selected_event_types if str(event_type).strip()
    ]
    selected_signal_cols: list[str] = []
    for event_type in selected:
        spec = EVENT_REGISTRY_SPECS.get(event_type)
        if spec is None:
            continue
        selected_signal_cols.append(spec.signal_column)
        selected_signal_cols.append(_active_signal_column(spec.signal_column))
        selected_signal_cols.append(_signal_ts_column(spec.signal_column))
        selected_signal_cols.append(_direction_signal_column(spec.signal_column))
    selected_signal_cols = list(dict.fromkeys(selected_signal_cols))

    keys = ["timestamp", "symbol"]
    left = existing_flags.copy() if existing_flags is not None else pd.DataFrame(columns=keys)
    right = recomputed_flags.copy() if recomputed_flags is not None else pd.DataFrame(columns=keys)

    if "timestamp" in left.columns:
        left["timestamp"] = pd.to_datetime(left["timestamp"], utc=True, errors="coerce")
    if "timestamp" in right.columns:
        right["timestamp"] = pd.to_datetime(right["timestamp"], utc=True, errors="coerce")
    left = (
        left.dropna(subset=["timestamp"])
        if "timestamp" in left.columns
        else pd.DataFrame(columns=keys)
    )
    right = (
        right.dropna(subset=["timestamp"])
        if "timestamp" in right.columns
        else pd.DataFrame(columns=keys)
    )

    if left.empty:
        merged = right.copy()
    else:
        if right.empty:
            merged = left.copy()
        else:
            keep_right_cols = [c for c in [*keys, *selected_signal_cols] if c in right.columns]
            merged = left.merge(
                right[keep_right_cols],
                on=keys,
                how="outer",
                suffixes=("", "__recomputed"),
            )
            for col in selected_signal_cols:
                new_col = f"{col}__recomputed"
                if new_col in merged.columns:
                    merged[col] = merged[new_col]
                    merged.drop(columns=[new_col], inplace=True)

    if "symbol" not in merged.columns:
        merged["symbol"] = "ALL"
    merged["symbol"] = merged["symbol"].fillna("").astype(str).str.upper()
    merged = merged[merged["symbol"].str.len() > 0].copy()

    out_cols = ["timestamp", "symbol"]
    for signal in sorted(REGISTRY_BACKED_SIGNALS):
        out_cols.extend(
            [
                signal,
                _active_signal_column(signal),
                _signal_ts_column(signal),
                _direction_signal_column(signal),
            ]
        )

    missing = [c for c in out_cols if c not in merged.columns]
    if missing:
        fill = pd.DataFrame(
            {
                c: (
                    pd.Series(np.nan, index=merged.index)
                    if c.startswith("evt_direction_") or c.endswith("_direction")
                    else pd.Series(False, index=merged.index)
                )
                for c in missing
            },
            index=merged.index,
        )
        merged = pd.concat([merged, fill], axis=1)
    merged = merged[out_cols].copy()

    for signal in sorted(REGISTRY_BACKED_SIGNALS):
        merged[signal] = merged[signal].where(merged[signal].notna(), False).astype(bool)
        active_col = _active_signal_column(signal)
        merged[active_col] = (
            merged[active_col].where(merged[active_col].notna(), False).astype(bool)
        )
        ts_col = _signal_ts_column(signal)
        merged[ts_col] = merged[ts_col].where(merged[ts_col].notna(), False).astype(bool)
        dir_col = _direction_signal_column(signal)
        merged[dir_col] = pd.to_numeric(merged[dir_col], errors="coerce").astype(float)

    return merged.sort_values(["timestamp", "symbol"]).reset_index(drop=True)
