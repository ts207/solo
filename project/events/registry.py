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
    "DetectorContract",
    "get_detector_contract",
    "list_trigger_detectors",
    "list_context_detectors",
    "list_runtime_eligible_detectors",
    "list_promotion_eligible_detectors",
    "list_governed_detectors",
    "list_legacy_detectors",
    "list_v2_detectors",
    "build_detector_version_inventory_rows",
    "resolve_event_alias",
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

from project.events.detector_contract import DetectorContract, DetectorContractError


def _normalize_role(row: dict) -> str:
    role = str(row.get("operational_role") or row.get("role") or "trigger").strip().lower()
    mapping = {
        "sequence_component": "composite",
        "filter": "context",
    }
    return mapping.get(role, role)


def _resolve_maturity(row: dict) -> str:
    tier = str(row.get("tier", "")).strip().upper()
    if str(row.get("deprecated", False)).strip().lower() == "true":
        return "deprecated"
    if tier == "A":
        return "production"
    if tier == "B":
        return "specialized"
    return "standard"



def _parameters(row: dict) -> dict:
    params = row.get("parameters")
    return params if isinstance(params, dict) else {}



def _bool_from_row(row: dict, *keys: str, default: bool = False) -> bool:
    for key in keys:
        if key in row:
            return bool(row.get(key))
    return default



def resolve_event_alias(event_name: str) -> str:
    normalized = str(event_name).strip().upper()
    registry = load_milestone_event_registry()
    for key, row in registry.items():
        aliases = row.get("aliases", [])
        if normalized == key or normalized in [str(a).strip().upper() for a in aliases]:
            return key
    return normalized



def get_detector_contract(event_name: str) -> DetectorContract:
    canonical_name = resolve_event_alias(event_name)
    row = get_event_definition(canonical_name)
    if not row:
        raise DetectorContractError(f"Event {event_name} not found in registry")
    params = _parameters(row)
    role = _normalize_role(row)
    aliases = tuple(str(alias).strip().upper() for alias in row.get("aliases", []) if str(alias).strip())
    templates = tuple(str(item).strip() for item in row.get("templates", []) if str(item).strip())
    horizons = tuple(str(item).strip() for item in row.get("horizons", []) if str(item).strip())
    required_columns = tuple(str(item).strip() for item in row.get("required_columns", []) if str(item).strip())
    optional_columns = tuple(str(item).strip() for item in row.get("optional_columns", []) if str(item).strip())
    source_dependencies = tuple(str(item).strip() for item in row.get("source_dependencies", []) if str(item).strip())
    WAVE1_V2_EVENTS = {
        "LIQUIDITY_SHOCK",
        "LIQUIDITY_STRESS_DIRECT",
        "LIQUIDITY_STRESS_PROXY",
        "DEPTH_COLLAPSE",
        "LIQUIDITY_GAP_PRINT",
        "LIQUIDITY_VACUUM",
        "LIQUIDATION_CASCADE",
        "LIQUIDATION_CASCADE_PROXY",
        "VOL_SPIKE",
        "VOL_SHOCK",
        "VOL_RELAXATION_START",
    }
    WAVE2_V2_EVENTS = {
        "BASIS_DISLOC",
        "FND_DISLOC",
        "SPOT_PERP_BASIS_SHOCK",
        "FUNDING_EXTREME_ONSET",
        "FUNDING_FLIP",
        "FUNDING_NORMALIZATION_TRIGGER",
        "FUNDING_PERSISTENCE_TRIGGER",
        "OI_FLUSH",
        "OI_SPIKE_NEGATIVE",
        "OI_SPIKE_POSITIVE",
    }
    WAVE3_V2_EVENTS = {
        "CROSS_VENUE_DESYNC",
        "CROSS_ASSET_DESYNC_EVENT",
        "CORRELATION_BREAKDOWN_EVENT",
        "INDEX_COMPONENT_DIVERGENCE",
        "LEAD_LAG_BREAK",
        "BETA_SPIKE_EVENT",
    }
    V2_EVENTS = WAVE1_V2_EVENTS | WAVE2_V2_EVENTS | WAVE3_V2_EVENTS
    DETECTOR_POLICY_OVERRIDES = {
        "LIQUIDITY_STRESS_PROXY": {"runtime_default": False, "promotion_eligible": False, "primary_anchor_eligible": False},
        "LIQUIDITY_GAP_PRINT": {"runtime_default": False, "promotion_eligible": False, "primary_anchor_eligible": False},
        "DEPTH_COLLAPSE": {"runtime_default": False, "promotion_eligible": True, "primary_anchor_eligible": False},
        "LIQUIDATION_CASCADE_PROXY": {"runtime_default": False, "promotion_eligible": False, "primary_anchor_eligible": False},
        "FUNDING_EXTREME_ONSET": {"runtime_default": False, "promotion_eligible": True, "primary_anchor_eligible": True},
        "FUNDING_FLIP": {"runtime_default": False, "promotion_eligible": True, "primary_anchor_eligible": True},
        "FUNDING_PERSISTENCE_TRIGGER": {"runtime_default": False, "promotion_eligible": True, "primary_anchor_eligible": True},
        "FUNDING_NORMALIZATION_TRIGGER": {"runtime_default": False, "promotion_eligible": True, "primary_anchor_eligible": False},
        "OI_SPIKE_POSITIVE": {"runtime_default": False, "promotion_eligible": True, "primary_anchor_eligible": True},
        "OI_SPIKE_NEGATIVE": {"runtime_default": False, "promotion_eligible": True, "primary_anchor_eligible": True},
        "OI_FLUSH": {"runtime_default": False, "promotion_eligible": True, "primary_anchor_eligible": True},
        "CROSS_ASSET_DESYNC_EVENT": {"runtime_default": True, "promotion_eligible": False, "primary_anchor_eligible": False},
    }
    DETECTOR_CLASS_OVERRIDES = {
        "BASIS_DISLOC": "BasisDislocationDetectorV2",
        "FND_DISLOC": "FndDislocDetectorV2",
        "SPOT_PERP_BASIS_SHOCK": "SpotPerpBasisShockDetectorV2",
        "FUNDING_EXTREME_ONSET": "FundingExtremeOnsetDetectorV2",
        "FUNDING_FLIP": "FundingFlipDetectorV2",
        "FUNDING_NORMALIZATION_TRIGGER": "FundingNormalizationDetectorV2",
        "FUNDING_PERSISTENCE_TRIGGER": "FundingPersistenceDetectorV2",
        "OI_FLUSH": "OIFlushDetectorV2",
        "OI_SPIKE_NEGATIVE": "OISpikeNegativeDetectorV2",
        "OI_SPIKE_POSITIVE": "OISpikePositiveDetectorV2",
        "CROSS_VENUE_DESYNC": "CrossVenueDesyncDetectorV2",
        "CROSS_ASSET_DESYNC_EVENT": "CrossAssetDesyncDetectorV2",
        "INDEX_COMPONENT_DIVERGENCE": "IndexComponentDivergenceDetectorV2",
        "LEAD_LAG_BREAK": "LeadLagBreakDetectorV2",
        "CORRELATION_BREAKDOWN_EVENT": "CorrelationBreakdownDetectorV2",
        "BETA_SPIKE_EVENT": "BetaSpikeDetectorV2",
    }

    def _load_calibration_defaults(name: str, version_hint: str) -> tuple[str | None, str | None]:
        try:
            from project.events.calibration.registry import latest_calibration_artifact
            artifact = latest_calibration_artifact(name, preferred_version=version_hint)
        except Exception:
            artifact = None
        if artifact is None:
            return None, None
        return artifact.calibration_mode, artifact.threshold_version

    version_hint = "v2" if canonical_name in V2_EVENTS else "v1"
    event_version = str(row.get("version") or row.get("event_version") or version_hint)
    context_only = _bool_from_row(row, "context_only", "is_context_tag", default=role == "context")
    composite = _bool_from_row(row, "composite", "is_composite", default=role == "composite")
    research_only = _bool_from_row(row, "research_only", default=role in {"composite", "research_only"})
    runtime_default = _bool_from_row(row, "runtime_default", "default_executable", default=False)
    planning_default = _bool_from_row(row, "planning_default", "default_executable", default=False)
    promotion_eligible = _bool_from_row(
        row,
        "promotion_eligible",
        default=(role == "trigger" and not context_only and not composite and runtime_default),
    )
    primary_anchor_eligible = _bool_from_row(
        row,
        "primary_anchor_eligible",
        default=(role == "trigger" and str(row.get("tier", "")).strip().upper() in {"A", "B"}),
    )
    if event_version != "v2":
        runtime_default = False
        promotion_eligible = False
        primary_anchor_eligible = False
    if canonical_name in DETECTOR_POLICY_OVERRIDES:
        override = DETECTOR_POLICY_OVERRIDES[canonical_name]
        runtime_default = bool(override.get("runtime_default", runtime_default))
        promotion_eligible = bool(override.get("promotion_eligible", promotion_eligible))
        primary_anchor_eligible = bool(override.get("primary_anchor_eligible", primary_anchor_eligible))
    v2_capability_default = event_version == "v2" and (runtime_default or role == "trigger")
    supports_confidence = _bool_from_row(row, "supports_confidence", default=v2_capability_default)
    supports_severity = _bool_from_row(row, "supports_severity", default=v2_capability_default)
    emits_quality_flag = _bool_from_row(row, "emits_quality_flag", default=v2_capability_default)
    calibration_mode_default, threshold_version_default = _load_calibration_defaults(canonical_name, event_version)
    try:
        return DetectorContract(
            event_name=canonical_name,
            event_version=event_version,
            detector_class=str(DETECTOR_CLASS_OVERRIDES.get(canonical_name) or row.get("detector_name", row.get("detector_class", ""))).strip(),
            canonical_family=str(row.get("canonical_family") or row.get("canonical_regime") or "").strip().upper(),
            subtype=str(row.get("subtype") or row.get("group") or canonical_name.lower()).strip(),
            phase=str(row.get("phase", "")).strip().lower() or "onset",
            evidence_mode=str(row.get("evidence_mode", "direct")).strip().lower(),
            role=role,
            maturity=_resolve_maturity(row),
            planning_default=planning_default,
            runtime_default=runtime_default,
            promotion_eligible=promotion_eligible,
            primary_anchor_eligible=primary_anchor_eligible,
            research_only=research_only,
            context_only=context_only,
            composite=composite,
            required_columns=required_columns,
            optional_columns=optional_columns,
            source_dependencies=source_dependencies,
            allowed_templates=templates,
            allowed_horizons=horizons,
            calibration_mode=str(row.get("calibration_mode") or calibration_mode_default or ("rolling_quantile" if canonical_name in V2_EVENTS else "fixed")).strip(),
            threshold_schema_version=str(row.get("threshold_schema_version") or threshold_version_default or ("2.0" if canonical_name in V2_EVENTS else "1.0")),
            merge_gap_bars=int(params.get("merge_gap_bars", row.get("merge_gap_bars", 0)) or 0),
            cooldown_bars=int(params.get("cooldown_bars", row.get("cooldown_bars", 0)) or 0),
            supports_confidence=supports_confidence,
            supports_severity=supports_severity,
            emits_quality_flag=emits_quality_flag,
            aliases=aliases,
            notes=str(row.get("notes", "")).strip(),
        )
    except Exception as exc:
        raise DetectorContractError(f"Invalid contract for {canonical_name}: {exc}") from exc



def _list_detectors_by_filter(filter_fn) -> list[DetectorContract]:
    contracts = []
    for key in load_milestone_event_registry().keys():
        contract = get_detector_contract(key)
        if filter_fn(contract):
            contracts.append(contract)
    return sorted(contracts, key=lambda c: c.event_name)



def list_trigger_detectors() -> list[DetectorContract]:
    return _list_detectors_by_filter(lambda c: c.role == "trigger")



def list_context_detectors() -> list[DetectorContract]:
    return _list_detectors_by_filter(lambda c: c.role == "context")



def list_runtime_eligible_detectors() -> list[DetectorContract]:
    return _list_detectors_by_filter(lambda c: c.runtime_default)



def list_promotion_eligible_detectors() -> list[DetectorContract]:
    return _list_detectors_by_filter(lambda c: c.promotion_eligible)


def list_governed_detectors() -> list[DetectorContract]:
    return _list_detectors_by_filter(lambda c: True)


def list_legacy_detectors() -> list[DetectorContract]:
    return _list_detectors_by_filter(lambda c: c.event_version != "v2")


def list_v2_detectors() -> list[DetectorContract]:
    return _list_detectors_by_filter(lambda c: c.event_version == "v2")


def build_detector_version_inventory_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for contract in list_governed_detectors():
        rows.append(
            {
                "event_name": contract.event_name,
                "event_version": contract.event_version,
                "role": contract.role,
                "maturity": contract.maturity,
                "planning_default": contract.planning_default,
                "runtime_default": contract.runtime_default,
                "promotion_eligible": contract.promotion_eligible,
                "primary_anchor_eligible": contract.primary_anchor_eligible,
                "context_only": contract.context_only,
                "composite": contract.composite,
                "research_only": contract.research_only,
                "supports_confidence": contract.supports_confidence,
                "supports_severity": contract.supports_severity,
                "emits_quality_flag": contract.emits_quality_flag,
                "threshold_schema_version": contract.threshold_schema_version,
                "calibration_mode": contract.calibration_mode,
                "legacy_retired_safe": contract.event_version != "v2"
                and not contract.runtime_default
                and not contract.promotion_eligible
                and not contract.primary_anchor_eligible,
            }
        )
    return rows
