from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

from project import PROJECT_ROOT
from project.domain.compiled_registry import get_domain_registry
from project.events.contract_registry import load_active_event_contracts
from project.events.event_aliases import EVENT_ALIASES
from project.spec_registry import load_yaml_path

ALLOWED_LAYER_VALUES: tuple[str, ...] = (
    "canonical",
    "composite",
    "context_tag",
    "strategy_construct",
    "research_placeholder",
)

ALLOWED_DISPOSITION_VALUES: tuple[str, ...] = (
    "keep",
    "merge",
    "rename",
    "demote",
    "delete",
)

ALLOWED_EVIDENCE_MODE_VALUES: tuple[str, ...] = (
    "direct",
    "proxy",
    "hybrid",
    "inferred_cross_asset",
    "sequence_confirmed",
    "contextual",
    "statistical",
)

# Legacy compatibility surface used by proposal warnings and a few audits. This
# is intentionally separate from ontology evidence_mode.
LEGACY_PROXY_TIER_EVENTS: frozenset[str] = frozenset(
    {
        "ABSORPTION_EVENT",
        "DEPTH_COLLAPSE",
        "ORDERFLOW_IMBALANCE_SHOCK",
        "SWEEP_STOPRUN",
        "FORCED_FLOW_EXHAUSTION",
    }
)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _clean_list(values: Iterable[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        token = _clean_text(value)
        if token and token not in seen:
            out.append(token)
            seen.add(token)
    return out


def _event_spec_path(event_type: str) -> Path:
    return (PROJECT_ROOT.parent / "spec" / "events" / f"{event_type}.yaml").resolve()


def _event_spec_payload(event_type: str) -> dict[str, Any]:
    path = _event_spec_path(event_type)
    if not path.exists():
        return {}
    payload = load_yaml_path(path)
    return dict(payload) if isinstance(payload, dict) else {}


def _family_for_proxy(spec_payload: Mapping[str, Any], fallback: str) -> str:
    token = _clean_text(spec_payload.get("research_family")) or _clean_text(
        spec_payload.get("canonical_family")
    )
    return token or fallback


def ontology_rows_by_event() -> Dict[str, Dict[str, Any]]:
    registry = get_domain_registry()
    out: Dict[str, Dict[str, Any]] = {}
    for event_type in registry.event_ids:
        spec = registry.event_definitions.get(event_type)
        if spec is None:
            continue
        out[event_type] = {
            "canonical_regime": spec.canonical_regime,
            "subtype": spec.subtype,
            "phase": spec.phase,
            "evidence_mode": spec.evidence_mode,
            "layer": spec.layer,
            "disposition": spec.disposition,
            "asset_scope": spec.asset_scope,
            "venue_scope": spec.venue_scope,
            "deconflict_priority": spec.deconflict_priority,
            "research_only": spec.research_only,
            "strategy_only": spec.strategy_only,
            "notes": spec.notes,
            "is_composite": spec.is_composite,
            "is_context_tag": spec.is_context_tag,
            "is_strategy_construct": spec.is_strategy_construct,
        }
    return out


def event_ontology_mapping_payload() -> Dict[str, Any]:
    return {
        "version": 1,
        "kind": "event_ontology_mapping",
        "metadata": {
            "status": "generated",
            "notes": (
                "Generated from the compiled event registry. "
                "Per-event specs are the authored source."
            ),
        },
        "allowed_values": {
            "layer": list(ALLOWED_LAYER_VALUES),
            "disposition": list(ALLOWED_DISPOSITION_VALUES),
            "evidence_mode": list(ALLOWED_EVIDENCE_MODE_VALUES),
        },
        "events": ontology_rows_by_event(),
    }


def event_contract_overrides_payload() -> Dict[str, Any]:
    contracts = load_active_event_contracts()
    events = {
        event_type: {
            "tier": _clean_text(contract.get("tier")),
            "operational_role": _clean_text(contract.get("operational_role")),
            "deployment_disposition": _clean_text(contract.get("deployment_disposition")),
            "runtime_category": _clean_text(contract.get("runtime_category")),
        }
        for event_type, contract in sorted(contracts.items())
    }
    return {
        "version": 1,
        "kind": "event_contract_overrides",
        "metadata": {
            "status": "generated",
            "notes": (
                "Generated from per-event governance fields via compiled event contracts."
            ),
        },
        "events": events,
    }


def proxy_event_types() -> set[str]:
    return set(LEGACY_PROXY_TIER_EVENTS)


def canonical_event_registry_payload() -> Dict[str, Any]:
    registry = get_domain_registry()
    event_metadata: Dict[str, Dict[str, Any]] = {}
    for event_type in sorted(proxy_event_types()):
        source_event_type = EVENT_ALIASES.get(event_type, event_type)
        spec = registry.event_definitions.get(source_event_type)
        if spec is None:
            continue
        payload = _event_spec_payload(spec.event_type)
        description = _clean_text(payload.get("description")) or (
            f"{event_type} canonical proxy compatibility entry."
        )
        family = _family_for_proxy(payload, spec.research_family or spec.canonical_family or spec.canonical_regime)
        event_metadata[event_type] = {
            "evidence_tier": "proxy",
            "family": family,
            "description": description,
        }
    return {
        "version": 1,
        "kind": "canonical_event_registry",
        "metadata": {
            "status": "generated",
            "notes": (
                "Generated compatibility registry for legacy proxy-tier event metadata."
            ),
        },
        "event_metadata": event_metadata,
    }
