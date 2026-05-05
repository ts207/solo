from __future__ import annotations

from collections.abc import Mapping
from functools import lru_cache
from dataclasses import dataclass, field
from typing import Any

from project.domain.hypotheses import HypothesisSpec, TriggerType
from project.domain.compiled_registry import get_domain_registry
from project.spec_registry.loaders import load_yaml_relative
from project.events.polarity import normalize_polarity_semantics


@lru_cache(maxsize=1)
def _event_template_matrix() -> dict[str, dict[str, dict[str, Any]]]:
    payload = load_yaml_relative("spec/compatibility/event_template_matrix.yaml")
    raw = payload.get("event_template_matrix", {}) if isinstance(payload, dict) else {}
    if not isinstance(raw, dict):
        return {}
    matrix: dict[str, dict[str, dict[str, Any]]] = {}
    for raw_event, raw_templates in raw.items():
        event = str(raw_event).strip().upper()
        if not event or not isinstance(raw_templates, dict):
            continue
        rows: dict[str, dict[str, Any]] = {}
        for raw_template, raw_rule in raw_templates.items():
            template = str(raw_template).strip()
            if template and isinstance(raw_rule, dict):
                rows[template] = dict(raw_rule)
        matrix[event] = rows
    return matrix


def _template_lookup_keys(template_id: str) -> tuple[str, ...]:
    token = str(template_id or "").strip()
    if not token:
        return ()
    keys = [token]
    if token == "continuation":
        keys.append("generic_continuation")
    elif token == "mean_reversion":
        keys.extend(["generic_mean_reversion", "unconditioned_mean_reversion"])
    elif token.startswith("generic_"):
        keys.append(token.removeprefix("generic_"))
    return tuple(dict.fromkeys(keys))


def _as_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    if value in (None, "", {}):
        return []
    return [str(value).strip()]


def _context_matches(actual: Mapping[str, str], expected: Mapping[str, Any]) -> tuple[bool, str]:
    for family, raw_allowed in expected.items():
        family_key = str(family).strip()
        allowed = _as_list(raw_allowed)
        actual_value = str(actual.get(family_key, "")).strip()
        if not actual_value:
            return False, family_key
        if allowed and actual_value not in allowed:
            return False, family_key
    return True, ""



def _data_quality_reason(context: Mapping[str, str] | None) -> str:
    if not context:
        return ""
    value = str(context.get("data_quality_state", "") or "").strip().lower()
    if value == "stale":
        return "data_quality_stale"
    if value == "missing_required_feature":
        return "data_quality_missing_required_feature"
    if value == "synthetic_only":
        return "data_quality_synthetic_only_research_only"
    return ""


@dataclass(frozen=True)
class CompatibilityVerdict:
    status: str
    required_contexts: dict[str, Any] = field(default_factory=dict)
    forbidden_contexts: dict[str, Any] = field(default_factory=dict)
    reason_codes: tuple[str, ...] = ()
    promotion_allowed: bool = True
    paper_allowed: bool = True
    live_allowed: bool = False
    polarity_semantics: str = "unknown"
    anchor_role: str = "alpha_anchor"

    @property
    def allowed_for_research(self) -> bool:
        return self.status in {"allowed", "allowed_with_required_context", "research_only"}

    @property
    def primary_reason(self) -> str:
        return self.reason_codes[0] if self.reason_codes else self.status



_PRICE_SEMANTICS = {"price_direction", "deviation_direction", "liquidity_sweep_side", "liquidation_side", "price_oi_quadrant"}
_RELATIVE_VALUE_SEMANTICS = {"basis_spread_direction"}
_GUARD_SEMANTICS = {"execution_guard", "temporal_guard", "neutral_guard", "regime_transition"}
_BASIS_TEMPLATES = {"basis_repair", "basis_convergence", "basis_funding_convergence", "desync_repair", "convergence", "lead_lag_follow", "divergence_continuation"}
_PRICE_DIRECTIONAL_TEMPLATES = {"trend_continuation", "breakout_followthrough", "volatility_expansion_follow", "pullback_entry", "momentum_fade", "false_breakout_reversal"}
_FORCED_FLOW_TEMPLATES = {"forced_flow_rebound", "long_flush_rebound", "positioning_flush_reversal", "squeeze_followthrough_confirmed", "cascade_continuation_until_cooldown"}
_REPAIR_TEMPLATES = {"overshoot_repair", "range_reversion", "stop_run_repair", "liquidity_refill_repair"}
_FILTER_TEMPLATES = {"only_if_funding", "only_if_oi", "only_if_liquidity", "only_if_regime", "only_if_highvol", "only_if_trend", "only_if_no_news_window", "slippage_aware_filter", "tail_risk_avoid", "drawdown_filter"}


def _event_row(event_id: str) -> dict[str, Any]:
    try:
        row = get_domain_registry().event_row(event_id)
    except Exception:
        row = None
    return dict(row or {})


def _semantics_verdict(event_id: str, template_id: str) -> tuple[bool, str, str, str]:
    row = _event_row(event_id)
    sem = normalize_polarity_semantics(row.get("polarity_semantics", "unknown"))
    anchor_role = str(row.get("anchor_role", row.get("operational_role", "alpha_anchor")) or "alpha_anchor").strip().lower()
    template = str(template_id or "").strip()
    if anchor_role in {"execution_guard", "temporal_guard", "context_filter", "risk_guard"}:
        if template in _FILTER_TEMPLATES:
            return True, "", sem, anchor_role
        return False, f"{anchor_role}_cannot_anchor_alpha", sem, anchor_role
    if sem in _GUARD_SEMANTICS and template not in _FILTER_TEMPLATES | {"liquidity_refill_repair", "tail_risk_avoid"}:
        return False, "guard_semantics_incompatible_with_directional_template", sem, anchor_role
    if sem in _RELATIVE_VALUE_SEMANTICS and template not in _BASIS_TEMPLATES | _FILTER_TEMPLATES:
        return False, "basis_side_is_not_price_side", sem, anchor_role
    if template in _BASIS_TEMPLATES and sem not in _RELATIVE_VALUE_SEMANTICS | {"funding_crowding_side"}:
        return False, "template_requires_basis_or_desync_semantics", sem, anchor_role
    if template in _PRICE_DIRECTIONAL_TEMPLATES and sem not in _PRICE_SEMANTICS:
        return False, "template_requires_price_direction_semantics", sem, anchor_role
    if template in _FORCED_FLOW_TEMPLATES and sem not in {"liquidation_side", "price_oi_quadrant", "funding_crowding_side"}:
        return False, "template_requires_forced_flow_or_positioning_semantics", sem, anchor_role
    return True, "", sem, anchor_role

def event_template_compatibility_verdict(spec: HypothesisSpec) -> CompatibilityVerdict:
    if spec.trigger.trigger_type != TriggerType.EVENT or not spec.trigger.event_id:
        return CompatibilityVerdict(status="allowed")

    event_id = str(spec.trigger.event_id).strip().upper()
    semantics_ok, semantics_reason, polarity_semantics, anchor_role = _semantics_verdict(event_id, spec.template_id)
    if not semantics_ok:
        return CompatibilityVerdict(
            status="forbidden",
            reason_codes=(semantics_reason,),
            promotion_allowed=False,
            paper_allowed=False,
            live_allowed=False,
            polarity_semantics=polarity_semantics,
            anchor_role=anchor_role,
        )
    rules = _event_template_matrix().get(event_id, {})
    if not rules:
        return CompatibilityVerdict(status="allowed", reason_codes=("no_event_template_rule",), polarity_semantics=polarity_semantics, anchor_role=anchor_role)

    rule: dict[str, Any] | None = None
    matched_template = ""
    for key in _template_lookup_keys(spec.template_id):
        if key in rules:
            rule = rules[key]
            matched_template = key
            break
    if rule is None:
        return CompatibilityVerdict(
            status="research_only",
            reason_codes=("missing_event_template_rule",),
            promotion_allowed=False,
            paper_allowed=False,
            live_allowed=False,
            polarity_semantics=polarity_semantics,
            anchor_role=anchor_role,
        )

    status = str(rule.get("status", "allowed")).strip().lower() or "allowed"
    required_contexts = rule.get("required_contexts", {}) if isinstance(rule.get("required_contexts", {}), Mapping) else {}
    forbidden_contexts = rule.get("forbidden_contexts", {}) if isinstance(rule.get("forbidden_contexts", {}), Mapping) else {}
    context = spec.context or {}
    reason_codes: list[str] = []

    data_quality_block = _data_quality_reason(context)
    if data_quality_block:
        return CompatibilityVerdict(
            status="research_only" if data_quality_block.endswith("research_only") else "forbidden",
            required_contexts=dict(required_contexts),
            forbidden_contexts=dict(forbidden_contexts),
            reason_codes=(data_quality_block,),
            promotion_allowed=False,
            paper_allowed=False,
            live_allowed=False,
            polarity_semantics=polarity_semantics,
            anchor_role=anchor_role,
        )

    if status == "forbidden":
        reason_codes.append("template_forbidden_for_event")
        return CompatibilityVerdict(
            status="forbidden",
            required_contexts=dict(required_contexts),
            forbidden_contexts=dict(forbidden_contexts),
            reason_codes=tuple(reason_codes),
            promotion_allowed=False,
            paper_allowed=False,
            live_allowed=False,
            polarity_semantics=polarity_semantics,
            anchor_role=anchor_role,
        )

    if required_contexts:
        ok, missing_family = _context_matches(context, required_contexts)
        if not ok:
            reason_codes.append(f"missing_required_context:{missing_family}")

    if forbidden_contexts:
        for family, raw_blocked in forbidden_contexts.items():
            family_key = str(family).strip()
            actual_value = str(context.get(family_key, "")).strip()
            if actual_value and actual_value in _as_list(raw_blocked):
                reason_codes.append(f"forbidden_context_present:{family_key}")

    verdict_status = "allowed"
    if required_contexts:
        verdict_status = "allowed_with_required_context"
    if str(status) in {"research_only", "paper_only"}:
        verdict_status = "research_only"

    return CompatibilityVerdict(
        status=verdict_status,
        required_contexts=dict(required_contexts),
        forbidden_contexts=dict(forbidden_contexts),
        reason_codes=tuple(reason_codes),
        promotion_allowed=not bool(reason_codes) and verdict_status != "research_only",
        paper_allowed=not bool(reason_codes),
        live_allowed=False,
        polarity_semantics=polarity_semantics,
        anchor_role=anchor_role,
    )


def validate_event_template_compatibility(spec: HypothesisSpec) -> list[str]:
    verdict = event_template_compatibility_verdict(spec)
    if verdict.status == "forbidden":
        event_id = str(spec.trigger.event_id or "").strip().upper()
        return [f"event-template compatibility forbids {event_id} x {spec.template_id}"]
    errors: list[str] = []
    for code in verdict.reason_codes:
        if code.startswith("missing_required_context:"):
            missing = code.split(":", 1)[1]
            errors.append(
                "event-template compatibility requires context "
                f"{missing!r} for {str(spec.trigger.event_id or '').strip().upper()} x {spec.template_id}"
            )
        elif code.startswith("forbidden_context_present:"):
            blocked = code.split(":", 1)[1]
            errors.append(
                "event-template compatibility forbids context "
                f"{blocked!r} for {str(spec.trigger.event_id or '').strip().upper()} x {spec.template_id}"
            )
    return errors
