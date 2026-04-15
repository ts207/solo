from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

import pandas as pd
import yaml

from project import PROJECT_ROOT
from project.domain.compiled_registry import get_domain_registry
from project.spec_registry import load_regime_registry

ALLOWED_BUCKETS = ("trade_generating", "trade_filtering", "context_only")
_ROUTING_SPEC_PATH = PROJECT_ROOT.parent / "spec" / "events" / "regime_routing.yaml"


@dataclass(frozen=True)
class RegimeRoutingEntry:
    canonical_regime: str
    bucket: str
    eligible_templates: tuple[str, ...]
    forbidden_templates: tuple[str, ...]
    risk_posture: str
    execution_style: str
    holding_horizon_profile: str
    stop_logic_profile: str
    profit_taking_profile: str
    overrides: Dict[str, Any]
    routing_profile_id: str
    scorecard_version: str
    scorecard_source_run: str


def routing_spec_path() -> Path:
    return _ROUTING_SPEC_PATH


def recommended_bucket_for_regime(canonical_regime: str) -> str:
    regime = str(canonical_regime or "").strip().upper()
    trade_filtering = {
        "EXECUTION_FRICTION",
        "TREND_FAILURE_EXHAUSTION",
        "VOLATILITY_RELAXATION_COMPRESSION_RELEASE",
    }
    context_only = {
        "SCHEDULED_TEMPORAL_WINDOW",
    }
    if regime in context_only:
        return "context_only"
    if regime in trade_filtering:
        return "trade_filtering"
    return "trade_generating"


@lru_cache(maxsize=4)
def load_regime_routing_spec(path: Path | None = None) -> Dict[str, Any]:
    if path is None:
        payload = load_regime_registry()
    else:
        resolved = path
        payload = yaml.safe_load(resolved.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("regime routing spec must be a mapping")
    return payload


def executable_canonical_regimes() -> tuple[str, ...]:
    registry = get_domain_registry()
    regimes: list[str] = []
    for regime, event_ids in registry.canonical_regime_rows().items():
        executable = [
            event_id
            for event_id in event_ids
            if not registry.get_event(event_id).is_composite
            and not registry.get_event(event_id).is_context_tag
            and not registry.get_event(event_id).is_strategy_construct
        ]
        if executable:
            regimes.append(regime)
    return tuple(sorted(regimes))


@lru_cache(maxsize=4)
def routing_rows(path: Path | None = None) -> Dict[str, RegimeRoutingEntry]:
    payload = load_regime_routing_spec(path)
    metadata = payload.get("metadata", {}) if isinstance(payload.get("metadata"), Mapping) else {}
    routing_profile_id = str(metadata.get("routing_profile_id", "regime_routing")).strip()
    scorecard_version = str(metadata.get("scorecard_version", "")).strip()
    scorecard_source_run = str(metadata.get("scorecard_source_run", "")).strip()
    regimes = payload.get("regimes", {})
    if not isinstance(regimes, Mapping):
        raise ValueError("regime routing spec must define a regimes mapping")
    out: Dict[str, RegimeRoutingEntry] = {}
    for canonical_regime, raw in regimes.items():
        if not isinstance(raw, Mapping):
            raise ValueError(f"routing row for {canonical_regime} must be an object")
        normalized_regime = str(canonical_regime).strip().upper()
        bucket = str(raw.get("bucket", "")).strip()
        if bucket not in ALLOWED_BUCKETS:
            raise ValueError(f"routing row for {normalized_regime} has invalid bucket '{bucket}'")
        out[normalized_regime] = RegimeRoutingEntry(
            canonical_regime=normalized_regime,
            bucket=bucket,
            eligible_templates=tuple(str(item).strip() for item in raw.get("eligible_templates", []) if str(item).strip()),
            forbidden_templates=tuple(str(item).strip() for item in raw.get("forbidden_templates", []) if str(item).strip()),
            risk_posture=str(raw.get("risk_posture", "")).strip(),
            execution_style=str(raw.get("execution_style", "")).strip(),
            holding_horizon_profile=str(raw.get("holding_horizon_profile", "")).strip(),
            stop_logic_profile=str(raw.get("stop_logic_profile", "")).strip(),
            profit_taking_profile=str(raw.get("profit_taking_profile", "")).strip(),
            overrides=dict(raw.get("overrides", {}) or {}),
            routing_profile_id=routing_profile_id,
            scorecard_version=scorecard_version,
            scorecard_source_run=scorecard_source_run,
        )
    return out


def validate_regime_routing_spec(path: Path | None = None) -> Dict[str, Any]:
    registry = get_domain_registry()
    routing = routing_rows(path)
    executable = set(executable_canonical_regimes())
    available_templates = set(registry.template_operator_definitions.keys())
    missing = sorted(executable - set(routing))
    unexpected = sorted(set(routing) - executable)
    invalid_templates: Dict[str, Dict[str, list[str]]] = {}
    non_routable_entries: list[str] = []
    eligible_templates_without_event_support: Dict[str, list[str]] = {}
    events_without_supported_templates: Dict[str, list[str]] = {}
    event_template_support: Dict[str, Dict[str, list[str]]] = {}
    empty_intersection_regimes: list[str] = []
    bucket_mismatches: Dict[str, Dict[str, str]] = {}
    for regime, entry in routing.items():
        invalid_eligible = sorted(set(entry.eligible_templates) - available_templates)
        invalid_forbidden = sorted(set(entry.forbidden_templates) - available_templates)
        if invalid_eligible or invalid_forbidden:
            invalid_templates[regime] = {
                "eligible_templates": invalid_eligible,
                "forbidden_templates": invalid_forbidden,
            }
        if regime not in executable and entry.bucket != "context_only":
            non_routable_entries.append(regime)
        recommended_bucket = recommended_bucket_for_regime(regime)
        if entry.bucket != recommended_bucket:
            bucket_mismatches[regime] = {
                "configured_bucket": entry.bucket,
                "recommended_bucket": recommended_bucket,
            }
        regime_event_support: Dict[str, list[str]] = {}
        unsupported_events: list[str] = []
        for event_id in registry.get_event_ids_for_regime(regime, executable_only=True):
            event_templates = set(registry.event_row(event_id).get("templates", []) or [])
            valid_templates = sorted(event_templates & set(entry.eligible_templates))
            regime_event_support[event_id] = valid_templates
            if not valid_templates:
                unsupported_events.append(event_id)
        event_template_support[regime] = regime_event_support
        if unsupported_events:
            events_without_supported_templates[regime] = sorted(unsupported_events)
        unsupported_templates = sorted(
            template_id
            for template_id in entry.eligible_templates
            if not any(
                template_id in templates for templates in regime_event_support.values()
            )
        )
        if unsupported_templates:
            eligible_templates_without_event_support[regime] = unsupported_templates
        if regime_event_support and not any(regime_event_support.values()):
            empty_intersection_regimes.append(regime)
    is_valid = (
        not missing
        and not unexpected
        and not invalid_templates
        and not non_routable_entries
        and not bucket_mismatches
    )
    return {
        "is_valid": is_valid,
        "routing_profile_id": next(iter(routing.values())).routing_profile_id if routing else "",
        "scorecard_version": next(iter(routing.values())).scorecard_version if routing else "",
        "scorecard_source_run": next(iter(routing.values())).scorecard_source_run if routing else "",
        "executable_regimes": sorted(executable),
        "routed_regimes": sorted(routing),
        "missing_regimes": missing,
        "unexpected_regimes": unexpected,
        "invalid_templates": invalid_templates,
        "non_routable_entries": non_routable_entries,
        "bucket_mismatches": bucket_mismatches,
        "eligible_templates_without_event_support": eligible_templates_without_event_support,
        "events_without_supported_templates": events_without_supported_templates,
        "empty_intersection_regimes": sorted(empty_intersection_regimes),
        "event_template_support": event_template_support,
    }


def routing_entry_for_regime(canonical_regime: str) -> RegimeRoutingEntry | None:
    return routing_rows().get(str(canonical_regime or "").strip().upper())


def regime_metadata_for_event(event_type: str) -> Dict[str, Any]:
    spec = get_domain_registry().get_event(event_type)
    if spec is None:
        return {
            "canonical_regime": "",
            "subtype": "",
            "phase": "",
            "evidence_mode": "",
            "recommended_bucket": "",
            "regime_bucket": "",
            "routing_profile_id": "",
        }
    routing = routing_entry_for_regime(spec.canonical_regime)
    return {
        "canonical_regime": spec.canonical_regime,
        "subtype": spec.subtype,
        "phase": spec.phase,
        "evidence_mode": spec.evidence_mode,
        "recommended_bucket": recommended_bucket_for_regime(spec.canonical_regime),
        "regime_bucket": routing.bucket if routing is not None else "",
        "routing_profile_id": routing.routing_profile_id if routing is not None else "",
    }


def annotate_regime_metadata(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "event_type" not in frame.columns:
        out = frame.copy()
        for column in (
            "canonical_regime",
            "subtype",
            "phase",
            "evidence_mode",
            "recommended_bucket",
            "regime_bucket",
            "routing_profile_id",
        ):
            if column not in out.columns:
                out[column] = pd.Series(dtype="object")
        return out
    rows = [regime_metadata_for_event(event_type) for event_type in frame["event_type"].astype(str)]
    meta = pd.DataFrame(rows, index=frame.index)
    out = frame.copy()
    for column in meta.columns:
        if column not in out.columns:
            out[column] = meta[column]
        else:
            out[column] = out[column].where(out[column].astype(str).str.strip() != "", meta[column])
    return out


def executable_regime_event_fanout(regimes: Iterable[str]) -> Dict[str, list[str]]:
    registry = get_domain_registry()
    out: Dict[str, list[str]] = {}
    for regime in regimes:
        normalized = str(regime or "").strip().upper()
        if not normalized:
            continue
        out[normalized] = list(registry.get_event_ids_for_regime(normalized, executable_only=True))
    return out
