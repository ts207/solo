from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from project.core.column_registry import ColumnRegistry
from project.events.detectors.registry import get_detector_class
from project.events.event_flags import load_registry_flags
from project.events.event_repository import load_registry_events
from project.events.event_specs import EVENT_REGISTRY_SPECS
from project.events.shared import direction_to_sign
from project.research.phase2 import load_features as load_features_impl
from project.research.validation import assign_split_labels
from project.specs.ontology import MATERIALIZED_STATE_COLUMNS_BY_ID

log = logging.getLogger(__name__)

_DIRECTION_HINT_SIGN_CACHE: dict[str, float | None] = {}


def _get_event_direction_hint_sign(event_id: str) -> float | None:
    """Return constant sign (-1.0 or 1.0) for fixed-direction events, None for bidirectional."""
    if event_id in _DIRECTION_HINT_SIGN_CACHE:
        return _DIRECTION_HINT_SIGN_CACHE[event_id]
    sign: float | None = None
    try:
        from project.domain.compiled_registry import get_domain_registry
        import yaml
        event_def = get_domain_registry().get_event(event_id)
        if event_def is not None and event_def.spec_path:
            spec_path = Path(event_def.spec_path)
            if spec_path.is_file():
                raw = yaml.safe_load(spec_path.read_text())
                hint = (raw.get("directionality") or {}).get("direction_hint", "")
                if hint == "short":
                    sign = -1.0
                elif hint == "long":
                    sign = 1.0
    except Exception:
        pass
    _DIRECTION_HINT_SIGN_CACHE[event_id] = sign
    return sign


def _materialize_event_flags_from_detectors(
    features: pd.DataFrame,
    *,
    symbol: str,
    expected_event_ids: Iterable[str],
) -> pd.DataFrame:
    if features.empty:
        return features
    if "timestamp" not in features.columns:
        return features

    out = features
    ts = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    if ts.isna().all():
        return out

    sym = str(symbol).strip().upper()
    for raw_event_id in expected_event_ids:
        event_id = str(raw_event_id or "").strip().upper()
        if not event_id:
            continue
        detector_cls = get_detector_class(event_id)
        if detector_cls is None:
            continue
        spec = EVENT_REGISTRY_SPECS.get(event_id)
        signal_col = spec.signal_column if spec is not None else None
        event_cols = ColumnRegistry.event_cols(event_id, signal_col=signal_col)
        if not event_cols:
            continue
        col = event_cols[0]
        if col not in out.columns:
            out[col] = False

        try:
            detector = detector_cls()
            events = detector.detect(out.copy(), symbol=sym)
        except Exception:
            continue
        if events is None or events.empty or "timestamp" not in events.columns:
            continue
        ev_ts = pd.to_datetime(events["timestamp"], utc=True, errors="coerce").dropna().drop_duplicates()
        if ev_ts.empty:
            continue
        ev_ts_set = set(ev_ts.tolist())
        out.loc[ts.isin(ev_ts_set), col] = True

        dir_col = ColumnRegistry.event_direction_cols(event_id)[0]
        if dir_col not in out.columns:
            out[dir_col] = float("nan")
        hint_sign = _get_event_direction_hint_sign(event_id)
        if hint_sign is not None:
            out.loc[ts.isin(ev_ts_set), dir_col] = hint_sign
    return out


def _load_features_wrapper(
    run_id: str,
    symbol: str,
    timeframe: str = "5m",
    data_root: Path | None = None,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    from project.core.config import get_data_root

    return load_features_impl(
        data_root=data_root or get_data_root(),
        run_id=run_id,
        symbol=symbol,
        timeframe=timeframe,
        start=start,
        end=end,
    )


def normalize_search_feature_columns(features: pd.DataFrame, *, copy: bool = True) -> pd.DataFrame:
    if features.empty:
        return features

    out = features.copy() if copy else features

    for state_id, source_col in MATERIALIZED_STATE_COLUMNS_BY_ID.items():
        canonical_col = str(state_id).strip().lower()
        if canonical_col in out.columns or source_col not in out.columns:
            continue
        out[canonical_col] = pd.to_numeric(out[source_col], errors="coerce").fillna(0.0)

    if "carry_state_code" in out.columns:
        carry_code = pd.to_numeric(out["carry_state_code"], errors="coerce").fillna(0.0)
        if "funding_positive" not in out.columns:
            out["funding_positive"] = (carry_code > 0).astype(float)
        if "funding_negative" not in out.columns:
            out["funding_negative"] = (carry_code < 0).astype(float)

    if "chop_state" not in out.columns and "chop_regime" in out.columns:
        out["chop_state"] = pd.to_numeric(out["chop_regime"], errors="coerce").fillna(0.0)
    if "trending_state" not in out.columns:
        bull_source = (
            out["bull_trend_regime"]
            if "bull_trend_regime" in out.columns
            else pd.Series(0.0, index=out.index)
        )
        bear_source = (
            out["bear_trend_regime"]
            if "bear_trend_regime" in out.columns
            else pd.Series(0.0, index=out.index)
        )
        bull = pd.to_numeric(bull_source, errors="coerce").fillna(0.0)
        bear = pd.to_numeric(bear_source, errors="coerce").fillna(0.0)
        out["trending_state"] = ((bull > 0) | (bear > 0)).astype(float)

    return out


def _normalize_event_direction_sign(events: pd.DataFrame) -> pd.Series:
    sign = pd.to_numeric(events.get("sign"), errors="coerce")
    if sign is not None:
        sign = sign.where(sign.isin([-1, 1]))
    else:
        sign = pd.Series(index=events.index, dtype=float)
    if sign.isna().any():
        fallback = events.get("direction")
        if fallback is not None:
            mapped = fallback.map(direction_to_sign).astype(float)
            sign = sign.fillna(mapped)
    return sign


def _build_event_direction_frame(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(columns=["timestamp", "symbol"])

    out = events.copy()
    ts = pd.to_datetime(out.get("timestamp"), utc=True, errors="coerce")
    if ts.isna().all() and "signal_ts" in out.columns:
        ts = pd.to_datetime(out.get("signal_ts"), utc=True, errors="coerce")
    out["timestamp"] = ts
    out["direction_sign"] = _normalize_event_direction_sign(out)
    out = out.dropna(subset=["timestamp", "symbol", "event_type", "direction_sign"]).copy()
    if out.empty:
        return pd.DataFrame(columns=["timestamp", "symbol"])

    out["symbol"] = out["symbol"].astype(str).str.upper()
    out["event_type"] = out["event_type"].astype(str).str.upper()
    out = out.sort_values(["timestamp", "symbol", "event_type"]).drop_duplicates(
        subset=["timestamp", "symbol", "event_type"], keep="last"
    )
    pivot = (
        out.pivot(index=["timestamp", "symbol"], columns="event_type", values="direction_sign")
        .reset_index()
    )
    pivot.columns.name = None

    rename_map = {
        event_type: ColumnRegistry.event_direction_cols(str(event_type))[0]
        for event_type in pivot.columns
        if event_type not in {"timestamp", "symbol"}
    }
    return pivot.rename(columns=rename_map)


def ensure_expected_event_columns(
    features: pd.DataFrame,
    *,
    expected_event_ids: Iterable[str] | None = None,
    copy: bool = True,
) -> pd.DataFrame:
    if features.empty or expected_event_ids is None:
        return features.copy() if copy else features

    out = features.copy() if copy else features
    for raw_event_id in expected_event_ids:
        event_id = str(raw_event_id or "").strip().upper()
        if not event_id:
            continue
        event_spec = EVENT_REGISTRY_SPECS.get(event_id)
        signal_col = event_spec.signal_column if event_spec is not None else None
        for column in ColumnRegistry.event_cols(event_id, signal_col=signal_col):
            if column not in out.columns:
                out[column] = False
    return out


def prepare_search_features_for_symbol(
    *,
    run_id: str,
    symbol: str,
    timeframe: str,
    data_root: Path,
    start: str | None = None,
    end: str | None = None,
    expected_event_ids: Iterable[str] | None = None,
    load_features_fn=_load_features_wrapper,
    event_registry_override: str | None = None,
) -> pd.DataFrame:
    features = load_features_fn(
        run_id=run_id,
        symbol=symbol,
        timeframe=timeframe,
        data_root=data_root,
        start=start,
        end=end,
    )
    if features.empty:
        return features

    features = normalize_search_feature_columns(features, copy=False)

    log.debug(
        "prepare_search_features_for_symbol: event_registry_override=%s",
        event_registry_override,
    )

    if event_registry_override:
        fixture_events = pd.read_parquet(event_registry_override)
        ts_col = "timestamp" if "timestamp" in fixture_events.columns else "ts"
        event_flags = _build_flags_from_fixture(fixture_events, symbol, ts_col)
    else:
        event_flags = load_registry_flags(data_root=data_root, run_id=run_id)

    sym_flags = pd.DataFrame()
    if not event_flags.empty:
        sym_flags = event_flags[event_flags["symbol"] == str(symbol).upper()].copy()
        if not sym_flags.empty:
            features = pd.merge(features, sym_flags, on=["timestamp", "symbol"], how="left")
            flag_cols = [c for c in sym_flags.columns if c not in ["timestamp", "symbol"]]
            direction_cols = [column for column in flag_cols if column.startswith("evt_direction_")]
            bool_cols = [column for column in flag_cols if column not in direction_cols]
            if bool_cols:
                features[bool_cols] = features[bool_cols].fillna(False).astype(bool)
            for column in direction_cols:
                features[column] = pd.to_numeric(features[column], errors="coerce")

    # Fallback: cell discovery often runs without a run-scoped event registry. When that's the
    # case, materialize minimal event flag columns directly from detectors so event triggers
    # aren't silently all-false (which yields n=0 and invalid metrics everywhere).
    if event_registry_override is None and event_flags.empty and expected_event_ids is not None:
        features = _materialize_event_flags_from_detectors(
            features,
            symbol=str(symbol).upper(),
            expected_event_ids=expected_event_ids,
        )

    if event_registry_override:
        fixture_events = pd.read_parquet(event_registry_override)
        registry_events = fixture_events[fixture_events["symbol"] == str(symbol).upper()].copy()
    else:
        registry_events = load_registry_events(
            data_root=data_root,
            run_id=run_id,
            symbols=[str(symbol).upper()],
        )
    direction_frame = _build_event_direction_frame(registry_events)
    if not direction_frame.empty:
        overlap_cols = [
            column
            for column in direction_frame.columns
            if column not in {"timestamp", "symbol"} and column in features.columns
        ]
        if overlap_cols:
            renamed = {column: f"{column}__dir" for column in overlap_cols}
            features = pd.merge(
                features,
                direction_frame.rename(columns=renamed),
                on=["timestamp", "symbol"],
                how="left",
            )
            for column in overlap_cols:
                merged_col = renamed[column]
                features[column] = pd.to_numeric(features[column], errors="coerce").combine_first(
                    pd.to_numeric(features[merged_col], errors="coerce")
                )
                features = features.drop(columns=[merged_col])
        else:
            features = pd.merge(features, direction_frame, on=["timestamp", "symbol"], how="left")

    features = ensure_expected_event_columns(
        features,
        expected_event_ids=expected_event_ids,
        copy=False,
    )

    if "split_label" not in features.columns:
        features = assign_split_labels(features, time_col="timestamp")

    return features


_ensure_expected_event_columns = ensure_expected_event_columns


def load_search_feature_frame(
    *,
    run_id: str,
    symbols: Iterable[str],
    timeframe: str,
    data_root: Path,
    expected_event_ids: Iterable[str] | None = None,
    event_registry_override: str | None = None,
) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for raw_symbol in symbols:
        symbol = str(raw_symbol).strip().upper()
        if not symbol:
            continue
        features = prepare_search_features_for_symbol(
            run_id=run_id,
            symbol=symbol,
            timeframe=timeframe,
            data_root=data_root,
            expected_event_ids=expected_event_ids,
            event_registry_override=event_registry_override,
        )
        if not features.empty:
            parts.append(features)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def _build_flags_from_fixture(
    fixture_events: pd.DataFrame,
    symbol: str,
    ts_col: str,
) -> pd.DataFrame:
    """Build event flag columns from a frozen fixture parquet.

    Produces the same shape as load_registry_flags: one row per
    (timestamp, symbol) with boolean event_type columns using
    lowercase signal_column names (e.g. vol_spike_event).
    Also produces _active and _signal columns.
    """
    sym = str(symbol).upper()
    sub = fixture_events[fixture_events["symbol"] == sym].copy()
    if sub.empty or "event_type" not in sub.columns:
        return pd.DataFrame()

    # Convert timestamps - handle both int64 milliseconds and datetime
    if sub[ts_col].dtype == "int64":
        sub[ts_col] = pd.to_datetime(sub[ts_col], unit="ms", utc=True, errors="coerce")
    else:
        sub[ts_col] = pd.to_datetime(sub[ts_col], utc=True, errors="coerce")
    sub = sub.dropna(subset=[ts_col])

    sig_col = "signal_column" if "signal_column" in sub.columns else None
    if sig_col:
        sub["flag_base"] = sub[sig_col].str.lower()
    else:
        sub["flag_base"] = sub["event_type"].str.lower() + "_event"

    parts = []
    for suffix in ["_event", "_active", "_signal"]:
        p = sub[[ts_col, "symbol", "flag_base", "event_score"]].copy()
        p["flag_col"] = p["flag_base"].str.replace("_event", "") + suffix
        p = p.pivot_table(
            index=[ts_col, "symbol"],
            columns="flag_col",
            values="event_score",
            aggfunc="max",
        ).reset_index()
        parts.append(p)

    if not parts:
        return pd.DataFrame()

    merged = parts[0]
    for p in parts[1:]:
        merged = merged.merge(p, on=[ts_col, "symbol"], how="outer")

    flag_cols = [c for c in merged.columns if c not in (ts_col, "symbol")]
    merged[flag_cols] = merged[flag_cols].fillna(False).astype(bool)

    if "sign" in sub.columns and "event_type" in sub.columns:
        for event_type in sub["event_type"].unique():
            event_type_str = str(event_type).strip().upper()
            event_type_lower = event_type_str.lower()
            dir_col = f"evt_direction_{event_type_lower}"
            event_type_mask = sub["event_type"] == event_type
            dir_df = sub[event_type_mask][[ts_col, "symbol", "sign"]].copy()
            dir_df[dir_col] = pd.to_numeric(dir_df["sign"], errors="coerce").astype(float)
            dir_pivot = dir_df.pivot_table(
                index=[ts_col, "symbol"],
                values=dir_col,
                aggfunc="first",
            ).reset_index()
            merged = merged.merge(dir_pivot, on=[ts_col, "symbol"], how="left")

    return merged
