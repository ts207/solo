from __future__ import annotations

import logging
import warnings
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

_LOG = logging.getLogger(__name__)

import pandas as pd

from project.events.event_normalizer import (
    _empty_registry_events,
    normalize_phase1_events,
    normalize_registry_events_frame,
)
from project.events.event_specs import (
    EVENT_REGISTRY_SPECS,
    REGISTRY_EVENT_COLUMNS,
    EventRegistrySpec,
)
from project.io.utils import (
    ensure_dir,
    read_parquet,
    write_parquet,
)


def _registry_root(data_root: Path, run_id: str) -> Path:
    return Path(data_root) / "events" / str(run_id)


def _registry_file(root: Path, stem: str) -> Path:
    return root / f"{stem}.parquet"


def _read_phase1_events(data_root: Path, run_id: str, spec: EventRegistrySpec) -> pd.DataFrame:
    # Check both data_root/reports and data_root/data/reports
    primary_candidates = [
        Path(data_root) / "reports" / spec.reports_dir / str(run_id) / spec.events_file,
        Path(data_root) / "data" / "reports" / spec.reports_dir / str(run_id) / spec.events_file,
    ]

    candidates: List[Path] = []
    for p in primary_candidates:
        _LOG.debug("checking candidate path: %s", p)
        candidates.append(p)
        if p.suffix.lower() == ".csv":
            candidates.append(p.with_suffix(".parquet"))
        elif p.suffix.lower() == ".parquet":
            candidates.append(p.with_suffix(".csv"))

    for path in candidates:
        if not path.exists():
            continue
        _LOG.debug("found valid path: %s", path)
        try:
            if path.suffix.lower() == ".parquet":
                return pd.read_parquet(path)
            try:
                return pd.read_csv(path)
            except Exception:
                return pd.read_parquet(path)
        except Exception as exc:
            _LOG.warning("Failed to read phase1 events from %s: %s", path, exc)
            continue

    return pd.DataFrame()


def collect_registry_events(
    data_root: Path, run_id: str, event_types: Iterable[str] | None = None
) -> pd.DataFrame:
    selected = list(event_types) if event_types is not None else sorted(EVENT_REGISTRY_SPECS.keys())
    rows: List[pd.DataFrame] = []
    for event_type in selected:
        spec = EVENT_REGISTRY_SPECS.get(str(event_type))
        if spec is None:
            continue
        events = _read_phase1_events(data_root=data_root, run_id=run_id, spec=spec)
        normalized = normalize_phase1_events(events=events, spec=spec, run_id=run_id)
        if not normalized.empty:
            rows.append(normalized)

    if not rows:
        return _empty_registry_events()
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.*",
            category=FutureWarning,
        )
        out = pd.concat(rows, ignore_index=True)
    out = out.sort_values(["timestamp", "symbol", "event_type", "event_id"]).reset_index(drop=True)
    return out[REGISTRY_EVENT_COLUMNS]


def merge_registry_events(
    *,
    existing: pd.DataFrame,
    incoming: pd.DataFrame,
    selected_event_types: Iterable[str] | None,
) -> pd.DataFrame:
    selected = {
        str(event_type).strip()
        for event_type in (selected_event_types or [])
        if str(event_type).strip()
    }
    existing_norm = normalize_registry_events_frame(existing)
    incoming_norm = normalize_registry_events_frame(incoming)
    if selected:
        existing_kept = existing_norm[~existing_norm["event_type"].isin(selected)].copy()
        incoming_replacement = incoming_norm[incoming_norm["event_type"].isin(selected)].copy()
    else:
        existing_kept = _empty_registry_events()
        incoming_replacement = incoming_norm
    existing_kept = existing_kept.dropna(axis=1, how="all")
    incoming_replacement = incoming_replacement.dropna(axis=1, how="all")
    if existing_kept.empty:
        merged = incoming_replacement.copy()
    elif incoming_replacement.empty:
        merged = existing_kept.copy()
    else:
        merged = pd.concat([existing_kept, incoming_replacement], ignore_index=True)
    return normalize_registry_events_frame(merged)


def write_event_registry_artifacts(
    data_root: Path, run_id: str, events: pd.DataFrame, event_flags: pd.DataFrame
) -> Dict[str, str]:
    root = _registry_root(data_root=data_root, run_id=run_id)
    ensure_dir(root)

    events_path, _ = write_parquet(events, _registry_file(root, "events"))
    flags_path, _ = write_parquet(event_flags, _registry_file(root, "event_flags"))
    return {
        "events_path": str(events_path),
        "event_flags_path": str(flags_path),
        "registry_root": str(root),
    }


def _read_registry_stem(data_root: Path, run_id: str, stem: str) -> pd.DataFrame:
    root = _registry_root(data_root=data_root, run_id=run_id)
    parquet_path = root / f"{stem}.parquet"
    csv_path = root / f"{stem}.csv"

    if parquet_path.exists():
        return read_parquet([parquet_path])
    if csv_path.exists():
        return pd.read_csv(csv_path)
    return pd.DataFrame()


def load_registry_events(
    *,
    data_root: Path,
    run_id: str,
    event_type: str | None = None,
    symbols: Sequence[str] | None = None,
) -> pd.DataFrame:
    from project.core.validation import coerce_to_ns_int

    events = _read_registry_stem(data_root=data_root, run_id=run_id, stem="events")
    if not events.empty:
        events["enter_ts"] = pd.to_datetime(
            coerce_to_ns_int(events["enter_ts"]), utc=True, errors="coerce"
        )
        events["exit_ts"] = pd.to_datetime(
            coerce_to_ns_int(events["exit_ts"]), utc=True, errors="coerce"
        )
        for col in ("phenom_enter_ts", "eval_bar_ts", "detected_ts", "signal_ts", "timestamp"):
            if col in events.columns:
                events[col] = pd.to_datetime(
                    coerce_to_ns_int(events[col]), utc=True, errors="coerce"
                )
    events = normalize_registry_events_frame(events)
    if events.empty:
        return _empty_registry_events()
    if event_type is not None:
        events = events[events["event_type"].astype(str) == str(event_type)].copy()
    if symbols is not None:
        symbol_set = {str(s).strip().upper() for s in symbols if str(s).strip()}
        if symbol_set:
            events = events[events["symbol"].astype(str).str.upper().isin(symbol_set)].copy()
    return events.sort_values(["timestamp", "symbol", "event_type", "event_id"]).reset_index(
        drop=True
    )


def write_registry_file(data_root: Path, run_id: str, name: str, df: pd.DataFrame) -> str:
    root = _registry_root(data_root=data_root, run_id=run_id)
    ensure_dir(root)
    path, _ = write_parquet(df, _registry_file(root, name))
    return str(path)


def load_registry_episode_anchors(
    *,
    data_root: Path,
    run_id: str,
    event_type: str | None = None,
    symbols: Sequence[str] | None = None,
) -> pd.DataFrame:
    from project.core.validation import coerce_to_ns_int

    root = _registry_root(data_root=data_root, run_id=run_id)
    ep_path = _registry_file(root, "episode_anchors")
    if not Path(ep_path).exists():
        return load_registry_events(
            data_root=data_root, run_id=run_id, event_type=event_type, symbols=symbols
        )

    events = read_parquet(ep_path)
    if not events.empty:
        for col in (
            "enter_ts",
            "exit_ts",
            "phenom_enter_ts",
            "eval_bar_ts",
            "detected_ts",
            "signal_ts",
        ):
            if col in events.columns and not pd.api.types.is_datetime64_any_dtype(events[col]):
                events[col] = pd.to_datetime(
                    coerce_to_ns_int(events[col]), utc=True, errors="coerce"
                )

    events = normalize_registry_events_frame(events)
    if events.empty:
        return _empty_registry_events()

    if event_type is not None:
        events = events[events["event_type"].astype(str) == str(event_type)].copy()

    if symbols is not None:
        symbol_set = {str(s).strip().upper() for s in symbols if str(s).strip()}
        if symbol_set:
            events = events[events["symbol"].astype(str).str.upper().isin(symbol_set)].copy()

    return events.sort_values(["timestamp", "symbol", "event_type", "event_id"]).reset_index(
        drop=True
    )
