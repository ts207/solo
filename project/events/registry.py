from __future__ import annotations

import functools
import sys
from pathlib import Path

import pandas as pd
import yaml

import project.events.event_flags as _event_flags_mod
from project import PROJECT_ROOT
from project.core.config import get_data_root
from project.events.event_diagnostics import (
    build_event_feature_frame,
    calibrate_event_thresholds,
    generate_event_coverage_report,
    registry_contract_check,
    verify_index_alignment,
)
from project.events.event_flags import (
    _active_signal_column,
    _signal_ts_column,
    load_registry_flags,
    merge_event_flags_for_selected_event_types,
)
from project.events.event_normalizer import (
    filter_phase1_rows_for_event_type,
    normalize_phase1_events,
    normalize_registry_events_frame,
)
from project.events.event_prerequisites import check_event_prerequisites
from project.events.event_repository import (
    collect_registry_events,
    load_registry_episode_anchors,
    load_registry_events,
    merge_registry_events,
    write_event_registry_artifacts,
    write_registry_file,
)
from project.events.event_specs import (
    AGGREGATE_EVENT_TYPE_UNIONS,
    EVENT_REGISTRY_SPECS,
    REGISTRY_BACKED_SIGNALS,
    REGISTRY_EVENT_COLUMNS,
    SIGNAL_TO_EVENT_TYPE,
    VALID_DIRECTIONS,
    EventRegistrySpec,
    _load_event_specs,
    assert_event_specs_available,
    expected_event_types_for_spec,
)

__all__ = [
    "AGGREGATE_EVENT_TYPE_UNIONS",
    "EVENT_REGISTRY_SPECS",
    "REGISTRY_BACKED_SIGNALS",
    "REGISTRY_EVENT_COLUMNS",
    "SIGNAL_TO_EVENT_TYPE",
    "VALID_DIRECTIONS",
    "EventRegistrySpec",
    "_active_signal_column",
    "_load_event_specs",
    "_load_symbol_timestamps",
    "_signal_ts_column",
    "assert_event_specs_available",
    "build_event_feature_frame",
    "build_event_flags",
    "calibrate_event_thresholds",
    "check_event_prerequisites",
    "collect_registry_events",
    "expected_event_types_for_spec",
    "filter_phase1_rows_for_event_type",
    "generate_event_coverage_report",
    "get_event_definition",
    "list_events_by_family",
    "load_milestone_event_registry",
    "load_registry_episode_anchors",
    "load_registry_events",
    "load_registry_flags",
    "merge_event_flags_for_selected_event_types",
    "merge_registry_events",
    "normalize_phase1_events",
    "normalize_registry_events_frame",
    "registry_contract_check",
    "verify_index_alignment",
    "write_event_registry_artifacts",
    "write_registry_file",
]


def build_event_flags(*, events, symbols, data_root, run_id, timeframe="5m"):
    """Wrapper so monkeypatching registry._load_symbol_timestamps works in tests."""
    this_module = sys.modules[__name__]
    return _event_flags_mod.build_event_flags(
        events=events,
        symbols=symbols,
        data_root=data_root,
        run_id=run_id,
        timeframe=timeframe,
        _ts_loader=this_module._load_symbol_timestamps,
    )


def _load_symbol_timestamps(
    data_root: "Path | None" = None, run_id: str = "", symbol: str = "", timeframe: str = "5m"
) -> pd.Series:
    from project.io.utils import read_parquet

    DATA_ROOT = get_data_root()
    path = DATA_ROOT / "lake" / "bars" / symbol / f"{timeframe}.parquet"
    if path.exists():
        df = read_parquet(path)
        return df["timestamp"]
    return pd.Series(dtype="datetime64[ns, UTC]")


_UNIFIED_REGISTRY_PATH = PROJECT_ROOT.parent / "spec" / "events" / "event_registry_unified.yaml"
_LEGACY_REGISTRY_PATH = PROJECT_ROOT.parent / "spec" / "events" / "registry.yaml"
_MILESTONE_REGISTRY_PATH = _UNIFIED_REGISTRY_PATH


@functools.lru_cache(maxsize=1)
def load_milestone_event_registry() -> dict[str, dict]:
    path = _UNIFIED_REGISTRY_PATH if _UNIFIED_REGISTRY_PATH.exists() else _LEGACY_REGISTRY_PATH
    if not path.exists():
        return {}
    
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        return {}
    
    # Unified registry nests event definitions under 'events'
    events_payload = payload.get("events", payload) if path == _UNIFIED_REGISTRY_PATH else payload
    if not isinstance(events_payload, dict):
        return {}

    out: dict[str, dict] = {}
    for raw_key, value in events_payload.items():
        if isinstance(value, dict):
            row = dict(value)
            event_type = str(row.get("event_type") or raw_key).strip().upper()
            row["event_type"] = event_type
            out[event_type] = row
    return out


def get_event_definition(event_type: str) -> dict | None:
    normalized = str(event_type).strip().upper()
    registry = load_milestone_event_registry()
    row = registry.get(normalized)
    return dict(row) if isinstance(row, dict) else None


def list_events_by_family(family: str) -> list[dict]:
    normalized = str(family).strip().upper()
    rows = []
    for row in load_milestone_event_registry().values():
        family_tokens = {
            str(row.get("family", "")).strip().upper(),
            str(row.get("canonical_regime", row.get("canonical_family", ""))).strip().upper(),
        }
        if normalized in family_tokens:
            rows.append(dict(row))
    rows.sort(key=lambda item: str(item.get("event_type", "")))
    return rows
