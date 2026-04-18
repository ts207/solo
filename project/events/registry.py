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
from project.events.policy import DEPLOYABLE_CORE_EVENT_TYPES

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
    "build_detector_eligibility_matrix_rows",
    "build_detector_migration_ledger_rows",
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

from project.events.detector_contract import (
    DetectorContract,
    DetectorContractError,
    NormalizedDetectorMetadata,
)


def _normalize_role(row: dict) -> str:
    role = str(row.get("operational_role") or row.get("role") or "trigger").strip().lower()
    mapping = {
        "sequence_component": "composite",
        "filter": "context",
    }
    return mapping.get(role, role)


def _resolve_maturity(row: dict) -> str:
    explicit = str(row.get("maturity") or "").strip().lower()
    if explicit:
        return explicit
    tier = str(row.get("tier", "")).strip().upper()
    if str(row.get("deprecated", False)).strip().lower() == "true":
        return "deprecated"
    if tier == "A":
        return "production"
    if tier == "B":
        return "specialized"
    return "standard"


def _resolve_detector_band(row: dict, event_name: str, role: str, context_only: bool, composite: bool) -> str:
    raw = str(row.get("detector_band") or "").strip().lower()
    if raw in {"deployable_core", "research_trigger", "context_only", "composite_or_fragile"}:
        return raw
    if event_name in DEPLOYABLE_CORE_EVENT_TYPES:
        return "deployable_core"
    if role == "context" or context_only:
        return "context_only"
    if role in {"composite", "research_only", "sequence_component"} or composite or event_name.startswith("SEQ_") or "PROXY" in event_name:
        return "composite_or_fragile"
    return "research_trigger"



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


def _normalize_columns(values: object) -> tuple[str, ...]:
    if isinstance(values, (list, tuple)):
        return tuple(str(item).strip() for item in values if str(item).strip())
    return ()


def _registered_detector_metadata(
    canonical_name: str, row: dict
) -> tuple[type[object] | None, NormalizedDetectorMetadata]:
    from project.events.detectors.registry import get_detector_metadata

    detector_cls, metadata = get_detector_metadata(canonical_name, row)
    if metadata is None:
        raise DetectorContractError(
            f"Detector metadata unavailable for registered event {canonical_name}"
        )
    return detector_cls, metadata


def validate_detector_contract_implementation_parity() -> dict[str, dict[str, dict[str, object]]]:
    mismatches: dict[str, dict[str, dict[str, object]]] = {}
    for contract in list_governed_detectors():
        row = get_event_definition(contract.event_name) or {}
        _, metadata = _registered_detector_metadata(contract.event_name, row)
        contract_mismatches = {
            "required_columns": {
                "contract": contract.required_columns,
                "implementation": metadata.required_columns,
            },
            "supports_confidence": {
                "contract": contract.supports_confidence,
                "implementation": metadata.supports_confidence,
            },
            "supports_severity": {
                "contract": contract.supports_severity,
                "implementation": metadata.supports_severity,
            },
            "supports_quality_flag": {
                "contract": contract.supports_quality_flag,
                "implementation": metadata.supports_quality_flag,
            },
            "cooldown_semantics": {
                "contract": contract.cooldown_semantics,
                "implementation": metadata.cooldown_semantics,
            },
            "merge_key_strategy": {
                "contract": contract.merge_key_strategy,
                "implementation": metadata.merge_key_strategy,
            },
        }
        contract_mismatches = {
            field: payload
            for field, payload in contract_mismatches.items()
            if payload["contract"] != payload["implementation"]
        }
        if contract_mismatches:
            mismatches[contract.event_name] = contract_mismatches
    return mismatches


def validate_detector_registry_implementation_parity(
    registry_rows: dict[str, dict] | None = None,
) -> dict[str, dict[str, dict[str, object]]]:
    rows = registry_rows or load_milestone_event_registry()
    mismatches: dict[str, dict[str, dict[str, object]]] = {}
    for event_name, row in rows.items():
        _, metadata = _registered_detector_metadata(event_name, row)
        row_mismatches: dict[str, dict[str, object]] = {}

        authored_required_columns = _normalize_columns(
            row.get("required_columns")
            or (row.get("detector", {}) if isinstance(row.get("detector"), dict) else {}).get(
                "required_columns", []
            )
        )
        if authored_required_columns and authored_required_columns != metadata.required_columns:
            row_mismatches["required_columns"] = {
                "registry": authored_required_columns,
                "implementation": metadata.required_columns,
            }

        authored_fields = {
            "supports_confidence": row.get("supports_confidence"),
            "supports_severity": row.get("supports_severity"),
            "supports_quality_flag": row.get("supports_quality_flag", row.get("emits_quality_flag")),
            "cooldown_semantics": row.get("cooldown_semantics"),
            "merge_key_strategy": row.get("merge_key_strategy"),
        }
        implementation_fields = {
            "supports_confidence": metadata.supports_confidence,
            "supports_severity": metadata.supports_severity,
            "supports_quality_flag": metadata.supports_quality_flag,
            "cooldown_semantics": metadata.cooldown_semantics,
            "merge_key_strategy": metadata.merge_key_strategy,
        }
        for field, authored_value in authored_fields.items():
            if authored_value is None:
                continue
            normalized_authored = (
                str(authored_value).strip()
                if field in {"cooldown_semantics", "merge_key_strategy"}
                else bool(authored_value)
            )
            if normalized_authored != implementation_fields[field]:
                row_mismatches[field] = {
                    "registry": normalized_authored,
                    "implementation": implementation_fields[field],
                }

        if row_mismatches:
            mismatches[event_name] = row_mismatches
    return mismatches



def get_detector_contract(event_name: str) -> DetectorContract:
    canonical_name = resolve_event_alias(event_name)
    row = get_event_definition(canonical_name)
    if not row:
        raise DetectorContractError(f"Event {event_name} not found in registry")
    params = _parameters(row)
    _, detector_metadata = _registered_detector_metadata(canonical_name, row)
    role = detector_metadata.role
    aliases = tuple(str(alias).strip().upper() for alias in row.get("aliases", []) if str(alias).strip())
    templates = tuple(str(item).strip() for item in row.get("templates", []) if str(item).strip())
    horizons = tuple(str(item).strip() for item in row.get("horizons", []) if str(item).strip())
    required_columns = detector_metadata.required_columns
    optional_columns = tuple(str(item).strip() for item in row.get("optional_columns", []) if str(item).strip())
    source_dependencies = tuple(str(item).strip() for item in row.get("source_dependencies", []) if str(item).strip())

    def _load_calibration_defaults(name: str, version_hint: str) -> tuple[str | None, str | None]:
        try:
            from project.events.calibration.registry import latest_calibration_artifact
            artifact = latest_calibration_artifact(name, preferred_version=version_hint)
        except Exception:
            artifact = None
        if artifact is None:
            return None, None
        return artifact.calibration_mode, artifact.threshold_version

    event_version = detector_metadata.event_version
    context_only = _bool_from_row(row, "context_only", "is_context_tag", default=role == "context")
    composite = _bool_from_row(row, "composite", "is_composite", default=role == "composite")
    research_only = _bool_from_row(row, "research_only", default=role in {"composite", "research_only"})
    detector_band = detector_metadata.detector_band
    runtime_default = detector_metadata.runtime_default
    planning_default = detector_metadata.planning_default
    promotion_eligible = detector_metadata.promotion_eligible
    primary_anchor_eligible = detector_metadata.primary_anchor_eligible
    calibration_mode_default, threshold_version_default = _load_calibration_defaults(canonical_name, event_version)
    try:
        return DetectorContract(
            event_name=canonical_name,
            event_version=event_version,
            detector_class=detector_metadata.detector_class
            or str(row.get("detector_name", row.get("detector_class", ""))).strip(),
            canonical_family=str(row.get("canonical_family") or row.get("canonical_regime") or "").strip().upper(),
            subtype=str(row.get("subtype") or row.get("group") or canonical_name.lower()).strip(),
            phase=str(row.get("phase", "")).strip().lower() or "onset",
            evidence_mode=str(row.get("evidence_mode", "direct")).strip().lower(),
            role=role,
            maturity=_resolve_maturity(row),
            detector_band=detector_band,
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
            calibration_mode=str(row.get("calibration_mode") or calibration_mode_default or ("rolling_quantile" if event_version == "v2" else "fixed")).strip(),
            threshold_schema_version=str(row.get("threshold_schema_version") or threshold_version_default or ("2.0" if event_version == "v2" else "1.0")),
            merge_gap_bars=int(params.get("merge_gap_bars", row.get("merge_gap_bars", 0)) or 0),
            cooldown_bars=int(params.get("cooldown_bars", row.get("cooldown_bars", 0)) or 0),
            cooldown_semantics=detector_metadata.cooldown_semantics,
            merge_key_strategy=detector_metadata.merge_key_strategy,
            supports_confidence=detector_metadata.supports_confidence,
            supports_severity=detector_metadata.supports_severity,
            supports_quality_flag=detector_metadata.supports_quality_flag,
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


def build_detector_eligibility_matrix_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for contract in list_governed_detectors():
        rows.append(
            {
                "event_name": contract.event_name,
                "event_version": contract.event_version,
                "role": contract.role,
                "detector_band": contract.detector_band,
                "maturity": contract.maturity,
                "planning": contract.planning_default,
                "promotion": contract.promotion_eligible,
                "runtime": contract.runtime_default,
                "anchor": contract.primary_anchor_eligible,
            }
        )
    return rows


def _migration_policy_for_contract(contract: DetectorContract) -> dict[str, str]:
    if contract.runtime_default:
        return {
            "migration_bucket": "runtime_core_first",
            "target_state": "migrate_to_v2",
            "owner": "workstream_c",
            "rationale": "deployable core runtime detector; keep fully v2 and contract-complete",
        }
    if contract.promotion_eligible:
        return {
            "migration_bucket": "promotion_eligible_middle_layer",
            "target_state": "migrate_to_v2",
            "owner": "workstream_c",
            "rationale": "promotion-eligible detector; keep on the governed v2 migration path",
        }
    if contract.role == "context":
        return {
            "migration_bucket": "research_perimeter",
            "target_state": "wrap_v1" if contract.event_version != "v2" else "demote",
            "owner": "workstream_b",
            "rationale": "context marker; keep behind adapter boundaries and out of runtime/promotion",
        }
    if contract.role in {"composite", "research_only"}:
        return {
            "migration_bucket": "research_perimeter",
            "target_state": "wrap_v1" if contract.event_version != "v2" else "demote",
            "owner": "workstream_b",
            "rationale": "composite or research-only construct; do not expand migration scope blindly",
        }
    if contract.detector_band == "composite_or_fragile":
        return {
            "migration_bucket": "research_perimeter",
            "target_state": "wrap_v1" if contract.event_version != "v2" else "demote",
            "owner": "workstream_b",
            "rationale": "fragile or proxy-heavy detector; preserve via adapters or demotion only",
        }
    if contract.event_version != "v2" and contract.planning_default:
        return {
            "migration_bucket": "research_perimeter",
            "target_state": "keep_v1",
            "owner": "workstream_b",
            "rationale": "legacy research trigger still available for planning but not promoted to v2 yet",
        }
    if contract.event_version != "v2":
        return {
            "migration_bucket": "research_perimeter",
            "target_state": "retire",
            "owner": "workstream_b",
            "rationale": "legacy detector outside the default planning surface; retire instead of migrating",
        }
    return {
        "migration_bucket": "promotion_eligible_middle_layer",
        "target_state": "migrate_to_v2",
        "owner": "workstream_c",
        "rationale": "v2 research trigger retained on the governed migration path",
    }


def build_detector_migration_ledger_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for contract in list_governed_detectors():
        policy = _migration_policy_for_contract(contract)
        rows.append(
            {
                "event_name": contract.event_name,
                "event_version": contract.event_version,
                "role": contract.role,
                "detector_band": contract.detector_band,
                "maturity": contract.maturity,
                "planning_default": contract.planning_default,
                "promotion_eligible": contract.promotion_eligible,
                "runtime_default": contract.runtime_default,
                "primary_anchor_eligible": contract.primary_anchor_eligible,
                **policy,
            }
        )
    return rows


def build_detector_version_inventory_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for contract in list_governed_detectors():
        rows.append(
            {
                "event_name": contract.event_name,
                "event_version": contract.event_version,
                "role": contract.role,
                "maturity": contract.maturity,
                "detector_band": contract.detector_band,
                "planning_default": contract.planning_default,
                "runtime_default": contract.runtime_default,
                "promotion_eligible": contract.promotion_eligible,
                "primary_anchor_eligible": contract.primary_anchor_eligible,
                "context_only": contract.context_only,
                "composite": contract.composite,
                "research_only": contract.research_only,
                "supports_confidence": contract.supports_confidence,
                "supports_severity": contract.supports_severity,
                "supports_quality_flag": contract.supports_quality_flag,
                "emits_quality_flag": contract.emits_quality_flag,
                "cooldown_semantics": contract.cooldown_semantics,
                "merge_key_strategy": contract.merge_key_strategy,
                "threshold_schema_version": contract.threshold_schema_version,
                "calibration_mode": contract.calibration_mode,
                "legacy_retired_safe": contract.event_version != "v2"
                and not contract.runtime_default
                and not contract.promotion_eligible
                and not contract.primary_anchor_eligible,
            }
        )
    return rows
