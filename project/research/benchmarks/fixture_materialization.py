from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from project.core.config import get_data_root
from project.events.event_specs import EVENT_REGISTRY_SPECS


DATA_ROOT = get_data_root()
FIXTURES_DIR = DATA_ROOT / "reports" / "benchmarks" / "fixtures"

_EVENT_SOURCE_RELATIVE_PATHS: dict[str, tuple[str, ...]] = {
    "VOL_SPIKE": (
        "data/reports/volatility_transition/broad_vol_spike_20260416T210045Z_68e0020707/vol_spike_edge_events.parquet",
    ),
    "VOL_SHOCK": (
        "data/reports/vol_shock_relaxation/broad_vol_shock_20260416T202825Z_c5cd86c72e/vol_shock_edge_events.parquet",
    ),
    "FUNDING_PERSISTENCE_TRIGGER": (
        "data/reports/funding_events/broad_vol_spike_20260416T210045Z_68e0020707/funding_persistence_trigger_edge_events.parquet",
    ),
    "TREND_DECELERATION": (
        "data/reports/trend_structure/broad_vol_spike_20260416T210045Z_68e0020707/trend_deceleration_edge_events.parquet",
    ),
    "PULLBACK_PIVOT": (
        "data/reports/trend_structure/broad_vol_spike_20260416T210045Z_68e0020707/pullback_pivot_edge_events.parquet",
    ),
}


def _parse_date(value: str) -> pd.Timestamp:
    return pd.Timestamp(value, tz="UTC")


def _candidate_source_paths(event_type: str, *, data_root: Path | None = None) -> list[Path]:
    resolved_root = Path(data_root) if data_root is not None else DATA_ROOT
    candidates: list[Path] = []
    for relative in _EVENT_SOURCE_RELATIVE_PATHS.get(str(event_type or "").strip().upper(), ()):
        path = Path(relative)
        if not path.is_absolute():
            path = resolved_root.parent / path
        candidates.append(path)
    return candidates


def load_events_for_type(
    event_type: str,
    *,
    start: str,
    end: str,
    data_root: Path | None = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    start_dt = _parse_date(start)
    end_dt = _parse_date(end)

    for source_path in _candidate_source_paths(event_type, data_root=data_root):
        if not source_path.exists():
            continue
        frame = pd.read_parquet(source_path)
        if frame.empty:
            continue
        ts_col = "timestamp" if "timestamp" in frame.columns else "signal_ts"
        if ts_col not in frame.columns:
            continue
        ts = pd.to_datetime(frame[ts_col], utc=True, errors="coerce")
        filtered = frame[(ts >= start_dt) & (ts <= end_dt)].copy()
        if not filtered.empty:
            frames.append(filtered)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    if "event_id" in combined.columns:
        combined = combined.drop_duplicates(subset=["event_id"], keep="last")
    return combined


def format_fixture_events(events: pd.DataFrame, *, event_type: str) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(
            columns=["timestamp", "symbol", "event_type", "event_score", "signal_column", "sign"]
        )

    event_id = str(event_type or "").strip().upper()
    event_spec = EVENT_REGISTRY_SPECS.get(event_id)
    signal_column = event_spec.signal_column if event_spec is not None else f"{event_id.lower()}_event"

    result = pd.DataFrame()
    result["timestamp"] = pd.to_datetime(events.get("timestamp"), utc=True, errors="coerce")
    result["symbol"] = events.get("symbol", "BTCUSDT")
    result["event_type"] = event_id
    result["event_score"] = pd.to_numeric(events.get("event_score"), errors="coerce").fillna(1.0)
    result["signal_column"] = signal_column
    result["sign"] = pd.to_numeric(events.get("sign"), errors="coerce").fillna(0).astype(int)
    if "detector_name" in events.columns:
        result["detector_name"] = events["detector_name"]
    else:
        result["detector_name"] = event_id
    return result.dropna(subset=["timestamp", "symbol"]).reset_index(drop=True)


def materialize_benchmark_fixture(
    *,
    slice_id: str,
    symbols: Iterable[str],
    start: str,
    end: str,
    event_types: Iterable[str],
    output_path: Path,
    data_root: Path | None = None,
) -> int:
    all_events: list[pd.DataFrame] = []
    requested_symbols = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]

    for raw_event_type in event_types:
        event_type = str(raw_event_type or "").strip().upper()
        if not event_type:
            continue
        events = load_events_for_type(event_type, start=start, end=end, data_root=data_root)
        if events.empty:
            continue
        for symbol in requested_symbols:
            symbol_events = events[events["symbol"].astype(str).str.upper() == symbol].copy()
            if not symbol_events.empty:
                all_events.append(format_fixture_events(symbol_events, event_type=event_type))

    if all_events:
        fixture = pd.concat(all_events, ignore_index=True)
        if "event_type" in fixture.columns:
            fixture = fixture.sort_values(["timestamp", "symbol", "event_type"]).reset_index(drop=True)
    else:
        fixture = pd.DataFrame(
            columns=["timestamp", "symbol", "event_type", "event_score", "signal_column", "sign"]
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fixture.to_parquet(output_path, index=False)
    return int(len(fixture))
