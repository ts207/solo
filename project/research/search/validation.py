"""
Validation for HypothesisSpec and TriggerSpec objects.
"""

from __future__ import annotations

from project.core.constants import parse_horizon_bars
from project.domain.hypotheses import HypothesisSpec, TriggerType
from project.research.context_labels import canonicalize_context_label
from project.research.search.context_gates import validate_context_overfit_gate
from project.research.search.feasibility import check_hypothesis_feasibility
from project.research.search.label_contracts import validate_template_label_contract
from project.spec_registry.loaders import load_yaml_relative
from project.strategy.templates.validation import validate_template_stack

VALID_DIRECTIONS = {"long", "short", "both"}
# Canonical labels kept for diagnostics and backward-compatible documentation.
CANONICAL_HORIZON_LABELS = {"1m", "5m", "15m", "30m", "60m", "1h", "4h", "1d"}
VALID_HORIZONS = CANONICAL_HORIZON_LABELS
VALID_OPERATORS = {">=", "<=", ">", "<", "=="}


def validate_hypothesis_spec(spec: HypothesisSpec) -> list[str]:
    """
    Validate a HypothesisSpec. Returns a list of error strings.
    Empty list means the spec is valid.
    """
    errors: list[str] = []

    if spec.direction not in VALID_DIRECTIONS:
        errors.append(
            f"Invalid direction {spec.direction!r}. Must be one of {sorted(VALID_DIRECTIONS)}"
        )

    try:
        parse_horizon_bars(spec.horizon)
    except Exception:
        errors.append(
            f"Invalid horizon {spec.horizon!r}. Must be parseable by parse_horizon_bars(), "
            f"for example one of {sorted(CANONICAL_HORIZON_LABELS)} or an arbitrary bar count like '72'/'72b'"
        )

    if not spec.template_id or not spec.template_id.strip():
        errors.append("template_id must not be empty")
    else:
        errors.extend(
            validate_template_stack(
                spec.template_id,
                filter_template_id=spec.filter_template_id,
            )
        )
        errors.extend(validate_template_label_contract(spec))

    if spec.entry_lag < 1:
        errors.append(f"entry_lag must be >= 1 to prevent same-bar entry leakage, got {spec.entry_lag}")

    try:
        spec.trigger.validate()
    except ValueError as e:
        errors.append(f"Invalid trigger: {e}")

    if spec.trigger.trigger_type == TriggerType.FEATURE_PREDICATE:
        if spec.trigger.operator not in VALID_OPERATORS:
            errors.append(
                f"Invalid trigger operator {spec.trigger.operator!r}. "
                f"Must be one of {sorted(VALID_OPERATORS)}"
            )

    if spec.feature_condition is not None:
        try:
            spec.feature_condition.validate()
        except ValueError as e:
            errors.append(f"Invalid feature_condition: {e}")
        if spec.feature_condition.trigger_type != TriggerType.FEATURE_PREDICATE:
            errors.append(
                "feature_condition must be a FEATURE_PREDICATE trigger, "
                f"got {spec.feature_condition.trigger_type!r}"
            )
        if spec.feature_condition.operator not in VALID_OPERATORS:
            errors.append(
                f"Invalid feature_condition operator {spec.feature_condition.operator!r}. "
                f"Must be one of {sorted(VALID_OPERATORS)}"
            )

    if spec.context:
        errors.extend(validate_context_overfit_gate(spec))
        try:
            allowed_contexts = (
                load_yaml_relative("project/configs/registries/contexts.yaml").get(
                    "context_dimensions", {}
                )
            )
            for family, label in spec.context.items():
                meta = allowed_contexts.get(family, {})
                labels = {
                    str(item).strip()
                    for item in list(meta.get("allowed_values", []) or [])
                    if str(item).strip()
                }
                if not labels:
                    errors.append(
                        f"Context family {family!r} not found in authoritative context registry"
                    )
                elif canonicalize_context_label(family, label) not in labels:
                    errors.append(
                        f"Context label {label!r} not found for family {family!r} in authoritative context registry"
                    )
        except Exception as e:
            errors.append(f"Failed to load authoritative context registry for context validation: {e}")

    feasibility = check_hypothesis_feasibility(spec)
    for reason in feasibility.reasons:
        errors.append(f"Feasibility: {reason}")

    return errors


def assert_valid(spec: HypothesisSpec) -> None:
    """Raise ValueError if spec is invalid."""
    errors = validate_hypothesis_spec(spec)
    if errors:
        raise ValueError(f"Invalid HypothesisSpec: {'; '.join(errors)}")
