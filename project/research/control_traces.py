from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from project.research import candidate_traces
from project.research.year_split import _safe_name

SCHEMA_VERSION = "control_traces_v1"
CONTROL_COLUMNS = [
    "run_id",
    "candidate_id",
    "control_type",
    "symbol",
    "event_id",
    "template_id",
    "context_key",
    "context_value",
    "direction",
    "horizon_bars",
    "event_ts",
    "entry_ts",
    "exit_ts",
    "entry_price",
    "exit_price",
    "gross_return_bps",
    "cost_bps",
    "net_return_bps",
    "entry_lag_bars",
    "context_pass",
    "event_pass",
    "source_artifact",
]


@dataclass(frozen=True)
class ControlTraceResult:
    status: str
    run_id: str
    candidate_id: str
    parquet_path: Path
    json_path: Path
    rows_by_control: dict[str, int]
    reason: str


def control_trace_paths(
    data_root: Path | str | None, run_id: str, candidate_id: str
) -> tuple[Path, Path]:
    base = Path(data_root or "data") / "reports" / "candidate_traces" / run_id
    safe = _safe_name(candidate_id)
    return base / f"{safe}_control_traces.parquet", base / f"{safe}_control_traces.json"


def load_base_candidate_traces(*, data_root: Path, run_id: str, candidate_id: str) -> pd.DataFrame:
    parquet_path, _ = candidate_traces.trace_paths(data_root, run_id, candidate_id)
    if not parquet_path.exists():
        return pd.DataFrame(columns=candidate_traces.TRACE_COLUMNS)
    return candidate_traces._read_table(parquet_path)


def load_market_bars(*, data_root: Path, run_id: str, symbol: str) -> pd.DataFrame:
    bars = candidate_traces._load_feature_frame(data_root, run_id, symbol)
    if bars.empty:
        return bars
    out = bars.copy()
    out["_timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out["_close"] = pd.to_numeric(out["close"], errors="coerce")
    return out.dropna(subset=["_timestamp", "_close"]).sort_values("_timestamp").reset_index(drop=True)


def _base_profile(base: pd.DataFrame, run_id: str, candidate_id: str) -> dict[str, Any]:
    row = base.iloc[0].to_dict()
    return {
        "run_id": run_id,
        "candidate_id": candidate_id,
        "symbol": str(row.get("symbol", "") or ""),
        "event_id": str(row.get("event_id", "") or ""),
        "template_id": str(row.get("template_id", "") or ""),
        "context_key": str(row.get("context_key", "") or ""),
        "context_value": str(row.get("context_value", "") or ""),
        "direction": str(row.get("direction", "") or ""),
        "horizon_bars": int(row.get("horizon_bars") or 0),
        "cost_bps": float(row.get("cost_bps") or 0.0),
    }


def _normalize_base(base: pd.DataFrame, *, run_id: str, candidate_id: str) -> pd.DataFrame:
    out = base.copy()
    out["run_id"] = run_id
    out["candidate_id"] = candidate_id
    out["control_type"] = "base"
    out["entry_lag_bars"] = 0
    out["event_pass"] = True
    out["context_pass"] = True
    for column in CONTROL_COLUMNS:
        if column not in out.columns:
            out[column] = None
    return out[CONTROL_COLUMNS]


def build_opposite_direction(base: pd.DataFrame) -> pd.DataFrame:
    out = base.copy()
    out["control_type"] = "opposite_direction"
    out["gross_return_bps"] = -pd.to_numeric(out["gross_return_bps"], errors="coerce")
    out["cost_bps"] = pd.to_numeric(out["cost_bps"], errors="coerce").fillna(0.0)
    out["net_return_bps"] = out["gross_return_bps"] - out["cost_bps"]
    out["direction"] = out["direction"].map(
        lambda value: "short" if str(value).lower() == "long" else "long" if str(value).lower() == "short" else value
    )
    out["entry_lag_bars"] = 0
    out["event_pass"] = True
    out["context_pass"] = True
    out["source_artifact"] = out["source_artifact"].astype(str) + "|opposite_direction_control"
    return out[CONTROL_COLUMNS]


def compute_directional_returns(
    *,
    event_ts: pd.Timestamp,
    bars: pd.DataFrame,
    direction: str,
    horizon_bars: int,
    entry_lag_bars: int,
    cost_bps: float,
) -> dict[str, Any] | None:
    timestamps = bars["_timestamp"]
    event_pos = int(timestamps.searchsorted(event_ts, side="left"))
    entry_pos = event_pos + int(entry_lag_bars)
    exit_pos = entry_pos + int(horizon_bars)
    if event_pos >= len(bars) or entry_pos >= len(bars) or exit_pos >= len(bars):
        return None
    entry_price = candidate_traces._to_float(bars["_close"].iloc[entry_pos])
    exit_price = candidate_traces._to_float(bars["_close"].iloc[exit_pos])
    if entry_price is None or exit_price is None or entry_price == 0.0:
        return None
    sign = -1.0 if str(direction).lower() == "short" else 1.0
    gross = ((exit_price / entry_price) - 1.0) * 10_000.0 * sign
    return {
        "event_ts": timestamps.iloc[event_pos],
        "entry_ts": timestamps.iloc[entry_pos],
        "exit_ts": timestamps.iloc[exit_pos],
        "entry_price": float(entry_price),
        "exit_price": float(exit_price),
        "gross_return_bps": float(gross),
        "cost_bps": float(cost_bps),
        "net_return_bps": float(gross - cost_bps),
    }


def _event_flag(frame: pd.DataFrame, event_id: str) -> pd.Series | None:
    event_norm = str(event_id or "").strip().lower()
    if not event_norm:
        return None
    direct_columns = [event_norm, event_norm.upper(), event_norm.lower()]
    for column in direct_columns:
        if column in frame.columns:
            return frame[column].astype(bool)
    if event_norm == "price_down_oi_down" and "price_oi_quadrant" in frame.columns:
        return frame["price_oi_quadrant"].map(candidate_traces._norm) == event_norm
    if "event_type" in frame.columns:
        return frame["event_type"].map(candidate_traces._norm) == event_norm
    if "event_id" in frame.columns:
        return frame["event_id"].map(candidate_traces._norm) == event_norm
    return None


def load_event_timestamps(*, data_root: Path, run_id: str, profile: dict[str, Any], bars: pd.DataFrame) -> pd.DataFrame:
    event_mask = _event_flag(bars, str(profile.get("event_id", "")))
    if event_mask is not None:
        return pd.DataFrame(
            {
                "event_ts": bars.loc[event_mask, "_timestamp"],
                "source_artifact": "lake_market_context:event_flag",
            }
        )
    paths = [data_root / "reports" / "phase2" / run_id / "phase2_candidate_event_timestamps.parquet"]
    frames = []
    for path in paths:
        if not path.exists():
            continue
        frame = candidate_traces._read_table(path)
        if frame.empty:
            continue
        event_col = candidate_traces._timestamp_column(frame)
        if event_col is None:
            continue
        filtered = frame
        if "symbol" in filtered.columns and profile.get("symbol"):
            filtered = filtered[filtered["symbol"].astype(str) == str(profile["symbol"])]
        if "event_type" in filtered.columns and profile.get("event_id"):
            filtered = filtered[
                filtered["event_type"].map(candidate_traces._norm)
                == candidate_traces._norm(profile["event_id"])
            ]
        frames.append(
            pd.DataFrame(
                {
                    "event_ts": pd.to_datetime(filtered[event_col], utc=True, errors="coerce"),
                    "source_artifact": str(path),
                }
            ).dropna(subset=["event_ts"])
        )
    return pd.concat(frames, ignore_index=True).drop_duplicates("event_ts") if frames else pd.DataFrame()


def _rows_from_events(
    *,
    events: pd.DataFrame,
    bars: pd.DataFrame,
    profile: dict[str, Any],
    control_type: str,
    entry_lag_bars: int,
    context_pass: bool,
    event_pass: bool,
    limit_per_year: int | None = None,
) -> pd.DataFrame:
    rows = []
    selected = events.copy()
    selected["event_ts"] = pd.to_datetime(selected["event_ts"], utc=True, errors="coerce")
    selected = selected.dropna(subset=["event_ts"]).sort_values("event_ts")
    if limit_per_year is not None:
        selected["_year"] = selected["event_ts"].dt.year
        selected = selected.groupby("_year", group_keys=False).head(limit_per_year)
    for _, event in selected.iterrows():
        ret = compute_directional_returns(
            event_ts=event["event_ts"],
            bars=bars,
            direction=str(profile["direction"]),
            horizon_bars=int(profile["horizon_bars"]),
            entry_lag_bars=entry_lag_bars,
            cost_bps=float(profile["cost_bps"]),
        )
        if ret is None:
            continue
        rows.append(
            {
                **{key: profile[key] for key in ("run_id", "candidate_id", "symbol", "event_id", "template_id", "context_key", "context_value", "direction", "horizon_bars")},
                "control_type": control_type,
                **ret,
                "entry_lag_bars": int(entry_lag_bars),
                "context_pass": bool(context_pass),
                "event_pass": bool(event_pass),
                "source_artifact": str(event.get("source_artifact", "")),
            }
        )
    return pd.DataFrame(rows, columns=CONTROL_COLUMNS)


def build_lagged_entries(base: pd.DataFrame, bars: pd.DataFrame, profile: dict[str, Any], lags: tuple[int, ...]) -> pd.DataFrame:
    events = pd.DataFrame(
        {
            "event_ts": pd.to_datetime(base["event_ts"], utc=True, errors="coerce"),
            "source_artifact": base["source_artifact"].astype(str),
        }
    ).dropna(subset=["event_ts"])
    frames = [
        _rows_from_events(
            events=events,
            bars=bars,
            profile=profile,
            control_type=f"entry_lag_{lag}",
            entry_lag_bars=lag,
            context_pass=True,
            event_pass=True,
        )
        for lag in lags
    ]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=CONTROL_COLUMNS)


def build_event_only(events: pd.DataFrame, bars: pd.DataFrame, profile: dict[str, Any]) -> pd.DataFrame:
    return _rows_from_events(
        events=events,
        bars=bars,
        profile=profile,
        control_type="event_only",
        entry_lag_bars=0,
        context_pass=False,
        event_pass=True,
    )


def build_context_only(base: pd.DataFrame, bars: pd.DataFrame, profile: dict[str, Any]) -> pd.DataFrame:
    event_mask = _event_flag(bars, str(profile.get("event_id", "")))
    if event_mask is None:
        return pd.DataFrame(columns=CONTROL_COLUMNS)
    context_mask = bars.apply(
        lambda row: candidate_traces._context_pass(
            row,
            str(profile.get("context_key", "") or ""),
            str(profile.get("context_value", "") or ""),
        ),
        axis=1,
    )
    excluded: set[pd.Timestamp] = set()
    horizon = int(profile["horizon_bars"])
    timestamps = bars["_timestamp"].reset_index(drop=True)
    for ts in pd.to_datetime(base["event_ts"], utc=True, errors="coerce").dropna():
        pos = int(timestamps.searchsorted(ts, side="left"))
        start = max(0, pos - horizon)
        end = min(len(timestamps), pos + horizon + 1)
        excluded.update(timestamps.iloc[start:end].tolist())
    candidates = bars.loc[context_mask & ~event_mask & ~bars["_timestamp"].isin(excluded), ["_timestamp"]].copy()
    candidates = candidates.rename(columns={"_timestamp": "event_ts"})
    candidates["source_artifact"] = "lake_market_context:context_only"
    base_year_counts = pd.to_datetime(base["event_ts"], utc=True, errors="coerce").dt.year.value_counts()
    rows = []
    for year, group in candidates.assign(_year=candidates["event_ts"].dt.year).groupby("_year", sort=True):
        limit = int(base_year_counts.get(year, 0))
        if limit <= 0:
            continue
        rows.append(group.head(limit))
    sampled = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["event_ts", "source_artifact"])
    return _rows_from_events(
        events=sampled,
        bars=bars,
        profile=profile,
        control_type="context_only",
        entry_lag_bars=0,
        context_pass=True,
        event_pass=False,
    )


def write_control_trace_outputs(
    *,
    traces: pd.DataFrame,
    result: ControlTraceResult,
    missing: list[str] | None = None,
) -> None:
    result.parquet_path.parent.mkdir(parents=True, exist_ok=True)
    if not traces.empty:
        traces = traces.copy()
        for column in ("event_ts", "entry_ts", "exit_ts"):
            traces[column] = pd.to_datetime(traces[column], utc=True, errors="coerce")
        for column in (
            "entry_price",
            "exit_price",
            "gross_return_bps",
            "cost_bps",
            "net_return_bps",
            "horizon_bars",
            "entry_lag_bars",
        ):
            traces[column] = pd.to_numeric(traces[column], errors="coerce")
        traces["context_pass"] = traces["context_pass"].astype(bool)
        traces["event_pass"] = traces["event_pass"].astype(bool)
        traces.to_parquet(result.parquet_path, index=False)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "status": result.status,
        "run_id": result.run_id,
        "candidate_id": result.candidate_id,
        "rows_by_control": result.rows_by_control,
        "reason": result.reason,
        "parquet_path": str(result.parquet_path) if result.parquet_path.exists() else "",
        "json_path": str(result.json_path),
    }
    if missing:
        payload["missing"] = missing
    base_n = int(result.rows_by_control.get("base", 0) or 0)
    event_only_n = int(result.rows_by_control.get("event_only", 0) or 0)
    if base_n > 0 and event_only_n > base_n:
        payload["support_notes"] = {
            "event_only": (
                "event_only support exceeds base support; compare mean net bps, t-stat, "
                "or matched/bootstrap samples rather than raw total PnL"
            )
        }
    result.json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_control_traces(
    *,
    run_id: str,
    candidate_id: str,
    data_root: Path,
    lags: tuple[int, ...] = (0, 1, 2, 3),
) -> ControlTraceResult:
    resolved_data_root = Path(data_root)
    parquet_path, json_path = control_trace_paths(resolved_data_root, run_id, candidate_id)
    base_raw = load_base_candidate_traces(
        data_root=resolved_data_root, run_id=run_id, candidate_id=candidate_id
    )
    if base_raw.empty:
        result = ControlTraceResult("blocked", run_id, candidate_id, parquet_path, json_path, {}, "missing_base_trace_source")
        write_control_trace_outputs(traces=pd.DataFrame(columns=CONTROL_COLUMNS), result=result, missing=["base"])
        return result

    profile = _base_profile(base_raw, run_id, candidate_id)
    bars = load_market_bars(data_root=resolved_data_root, run_id=run_id, symbol=str(profile["symbol"]))
    if bars.empty:
        result = ControlTraceResult("blocked", run_id, candidate_id, parquet_path, json_path, {}, "missing_market_bar_source")
        write_control_trace_outputs(
            traces=pd.DataFrame(columns=CONTROL_COLUMNS),
            result=result,
            missing=["entry_lag", "event_only", "context_only"],
        )
        return result

    base = _normalize_base(base_raw, run_id=run_id, candidate_id=candidate_id)
    events = load_event_timestamps(data_root=resolved_data_root, run_id=run_id, profile=profile, bars=bars)
    if events.empty:
        result = ControlTraceResult("blocked", run_id, candidate_id, parquet_path, json_path, {}, "missing_event_timestamp_source")
        write_control_trace_outputs(traces=pd.DataFrame(columns=CONTROL_COLUMNS), result=result, missing=["event_only"])
        return result

    frames = [
        base,
        build_opposite_direction(base),
        build_lagged_entries(base, bars, profile, lags),
        build_event_only(events, bars, profile),
        build_context_only(base, bars, profile),
    ]
    traces = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True)
    traces = traces[CONTROL_COLUMNS].sort_values(["control_type", "event_ts"]).reset_index(drop=True)
    rows_by_control = {str(key): int(value) for key, value in traces["control_type"].value_counts().sort_index().items()}
    missing = [name for name in ("event_only", "context_only") if rows_by_control.get(name, 0) == 0]
    status = "blocked" if missing else "pass"
    reason = "ok" if status == "pass" else "missing_control_rows"
    result = ControlTraceResult(status, run_id, candidate_id, parquet_path, json_path, rows_by_control, reason)
    write_control_trace_outputs(traces=traces, result=result, missing=missing)
    return result


def result_to_jsonable(result: ControlTraceResult) -> dict[str, Any]:
    payload = asdict(result)
    payload["parquet_path"] = str(result.parquet_path)
    payload["json_path"] = str(result.json_path)
    return payload
