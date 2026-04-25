from __future__ import annotations

from functools import lru_cache
from typing import Any

from project.domain.compiled_registry import get_domain_registry
from project.domain.hypotheses import HypothesisSpec
from project.spec_registry.loaders import load_yaml_relative


@lru_cache(maxsize=1)
def _template_label_contracts() -> dict[str, dict[str, Any]]:
    payload = load_yaml_relative("spec/templates/template_label_contracts.yaml")
    raw = payload.get("template_labels", {}) if isinstance(payload, dict) else {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for raw_template, raw_contract in raw.items():
        template = str(raw_template).strip()
        if template and isinstance(raw_contract, dict):
            out[template] = dict(raw_contract)
    return out


def validate_template_label_contract(spec: HypothesisSpec) -> list[str]:
    template_id = str(spec.template_id or "").strip()
    contract = _template_label_contracts().get(template_id)
    if not contract:
        return []

    registry = get_domain_registry()
    operator = registry.get_operator(template_id)
    operator_raw = operator.raw if operator is not None and isinstance(operator.raw, dict) else {}
    actual_primary = str(operator_raw.get("label_target", "")).strip()
    expected_primary = str(contract.get("primary", "")).strip()

    if not expected_primary:
        return []
    if not actual_primary:
        return [f"template {template_id!r} must declare label_target {expected_primary!r}"]
    if actual_primary != expected_primary:
        return [
            f"template {template_id!r} label_target {actual_primary!r} does not match "
            f"label contract primary {expected_primary!r}"
        ]
    return []
