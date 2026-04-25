from __future__ import annotations

from functools import lru_cache
from typing import Any, Mapping

from project.domain.hypotheses import HypothesisSpec, TriggerType
from project.spec_registry.loaders import load_yaml_relative


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


def validate_event_template_compatibility(spec: HypothesisSpec) -> list[str]:
    if spec.trigger.trigger_type != TriggerType.EVENT or not spec.trigger.event_id:
        return []

    event_id = str(spec.trigger.event_id).strip().upper()
    rules = _event_template_matrix().get(event_id, {})
    if not rules:
        return []

    rule: dict[str, Any] | None = None
    matched_template = ""
    for key in _template_lookup_keys(spec.template_id):
        if key in rules:
            rule = rules[key]
            matched_template = key
            break
    if rule is None:
        return []

    status = str(rule.get("status", "")).strip().lower()
    if status == "forbidden":
        return [
            f"event-template compatibility forbids {event_id} x {spec.template_id}"
        ]

    errors: list[str] = []
    context = spec.context or {}
    required_contexts = rule.get("required_contexts", {})
    if isinstance(required_contexts, Mapping) and required_contexts:
        ok, missing_family = _context_matches(context, required_contexts)
        if not ok:
            errors.append(
                "event-template compatibility requires context "
                f"{missing_family!r} for {event_id} x {matched_template or spec.template_id}"
            )

    forbidden_contexts = rule.get("forbidden_contexts", {})
    if isinstance(forbidden_contexts, Mapping) and forbidden_contexts:
        for family, raw_blocked in forbidden_contexts.items():
            family_key = str(family).strip()
            actual_value = str(context.get(family_key, "")).strip()
            if actual_value and actual_value in _as_list(raw_blocked):
                errors.append(
                    "event-template compatibility forbids context "
                    f"{family_key}={actual_value} for {event_id} x {matched_template or spec.template_id}"
                )

    return errors
