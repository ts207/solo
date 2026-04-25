from __future__ import annotations

from project.domain.compiled_registry import get_domain_registry
from project.domain.hypotheses import HypothesisSpec, TriggerType

_NON_STANDALONE_ROLES = frozenset(
    {
        "context",
        "filter",
        "research_only",
        "sequence_component",
        "state_source",
        "context_tag",
        "execution_guard",
        "risk_guard",
        "diagnostic_only",
    }
)


def validate_standalone_event_role(spec: HypothesisSpec) -> list[str]:
    if spec.trigger.trigger_type != TriggerType.EVENT or not spec.trigger.event_id:
        return []

    registry = get_domain_registry()
    event_def = registry.get_event(spec.trigger.event_id)
    if event_def is None:
        return []

    raw = event_def.raw if isinstance(event_def.raw, dict) else {}
    role = str(event_def.operational_role or raw.get("operational_role", "")).strip().lower()
    detector_band = str(event_def.detector_band or raw.get("detector_band", "")).strip().lower()
    deployment_disposition = str(
        event_def.deployment_disposition or raw.get("deployment_disposition", "")
    ).strip().lower()
    event_kind = str(event_def.event_kind or raw.get("event_kind", "")).strip().lower()

    blocked = bool(
        role in _NON_STANDALONE_ROLES
        or detector_band == "context_only"
        or deployment_disposition in {"context_only", "guard_only", "diagnostic_only"}
        or event_kind in {"context_tag", "execution_guard", "risk_guard"}
        or event_def.is_context_tag
        or event_def.research_only
        or event_def.is_strategy_construct
    )
    if not blocked:
        return []

    return [
        "event role contract blocks standalone alpha hypothesis for "
        f"{event_def.event_type} (role={role or 'unspecified'}, band={detector_band or 'unspecified'})"
    ]
