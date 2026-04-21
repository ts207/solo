from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Mapping

import pandas as pd

from project.core.column_registry import ColumnRegistry
from project.domain.compiled_registry import get_domain_registry
from project.domain.models import DomainRegistry
from project.domain.hypotheses import HypothesisSpec, TriggerType
from project.research.search.evaluator_utils import load_context_state_map


@dataclass(frozen=True)
class FeasibilityResult:
    valid: bool
    reasons: tuple[str, ...] = ()
    details: Dict[str, Any] = field(default_factory=dict)

    @property
    def primary_reason(self) -> str:
        return self.reasons[0] if self.reasons else ""


def _existing_column(columns: Iterable[str], available: set[str]) -> str | None:
    for column in columns:
        if column in available:
            return column
    return None


def _event_family(spec: HypothesisSpec, registry: DomainRegistry) -> str:
    if spec.trigger.trigger_type != TriggerType.EVENT or not spec.trigger.event_id:
        return ""
    event_def = registry.get_event(spec.trigger.event_id)
    return (
        event_def.research_family or event_def.canonical_family
        if event_def is not None
        else ""
    )


def _template_family_reason(spec: HypothesisSpec, registry: DomainRegistry) -> str | None:
    del spec, registry
    return None


def _context_feasibility(
    context: Mapping[str, str] | None,
    available_columns: set[str] | None,
) -> tuple[list[str], Dict[str, Any]]:
    if not context:
        return [], {}

    reasons: list[str] = []
    details: Dict[str, Any] = {}
    try:
        context_map = load_context_state_map()
    except Exception:
        return ["context_registry_unavailable"], details

    for family, label in context.items():
        state_id = context_map.get((family, label))
        if state_id is None:
            reasons.append("unknown_context_mapping")
            details[f"context:{family}:{label}"] = "unmapped"
            continue
        if available_columns is None:
            continue
        column = _existing_column(ColumnRegistry.state_cols(state_id), available_columns)
        if column is None:
            reasons.append("missing_context_state_column")
            details[f"context:{family}:{label}"] = state_id
    return reasons, details


def check_hypothesis_feasibility(
    spec: HypothesisSpec,
    *,
    features: pd.DataFrame | None = None,
    registry: DomainRegistry | None = None,
) -> FeasibilityResult:
    registry = registry or get_domain_registry()
    available_columns = set(map(str, features.columns)) if features is not None else None
    reasons: list[str] = []
    details: Dict[str, Any] = {}

    t = spec.trigger
    ttype = t.trigger_type

    if ttype == TriggerType.EVENT:
        event_id = t.event_id or ""
        event_def = registry.get_event(event_id)
        if event_def is None:
            reasons.append("unknown_event")
        elif available_columns is not None:
            column = _existing_column(
                ColumnRegistry.event_cols(event_id, signal_col=event_def.signal_column),
                available_columns,
            )
            if column is None:
                reasons.append("missing_event_column")
                details["event_id"] = event_id
    elif ttype == TriggerType.STATE:
        state_id = t.state_id or ""
        if not registry.has_state(state_id):
            reasons.append("unknown_state")
        elif available_columns is not None:
            column = _existing_column(ColumnRegistry.state_cols(state_id), available_columns)
            if column is None:
                reasons.append("missing_state_column")
                details["state_id"] = state_id
    elif ttype == TriggerType.TRANSITION:
        if not registry.has_state(t.from_state or ""):
            reasons.append("unknown_from_state")
        if not registry.has_state(t.to_state or ""):
            reasons.append("unknown_to_state")
        if available_columns is not None:
            from_col = _existing_column(
                ColumnRegistry.state_cols(t.from_state or ""), available_columns
            )
            to_col = _existing_column(
                ColumnRegistry.state_cols(t.to_state or ""), available_columns
            )
            if from_col is None or to_col is None:
                reasons.append("missing_transition_state_column")
    elif ttype == TriggerType.FEATURE_PREDICATE:
        if available_columns is not None:
            column = _existing_column(
                ColumnRegistry.feature_cols(t.feature or ""), available_columns
            )
            if column is None:
                reasons.append("missing_feature_column")
                details["feature"] = t.feature or ""
    elif ttype == TriggerType.SEQUENCE:
        if available_columns is not None:
            column = _existing_column(
                ColumnRegistry.sequence_cols(t.sequence_id or ""), available_columns
            )
            if column is None:
                reasons.append("missing_sequence_column")
                details["sequence_id"] = t.sequence_id or ""
    elif ttype == TriggerType.INTERACTION:
        if available_columns is not None:
            column = _existing_column(
                ColumnRegistry.interaction_cols(t.interaction_id or ""), available_columns
            )
            if column is None:
                reasons.append("missing_interaction_column")
                details["interaction_id"] = t.interaction_id or ""
    else:
        reasons.append("unsupported_trigger_type")

    template_reason = _template_family_reason(spec, registry)
    if template_reason:
        reasons.append(template_reason)
        details["template_id"] = spec.template_id
        details["primary_event_id"] = spec.trigger.event_id or ""
        details["compat_event_family"] = _event_family(spec, registry)
        details["family"] = details["compat_event_family"]

    if spec.feature_condition is not None and available_columns is not None:
        fc_column = _existing_column(
            ColumnRegistry.feature_cols(spec.feature_condition.feature or ""),
            available_columns,
        )
        if fc_column is None:
            reasons.append("missing_feature_condition_column")
            details["feature_condition"] = spec.feature_condition.feature or ""

    context_reasons, context_details = _context_feasibility(spec.context, available_columns)
    reasons.extend(context_reasons)
    details.update(context_details)

    ordered_reasons = tuple(dict.fromkeys(reason for reason in reasons if reason))
    return FeasibilityResult(valid=not ordered_reasons, reasons=ordered_reasons, details=details)
