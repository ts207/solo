from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from project.core.column_registry import ColumnRegistry
from project.domain.compiled_registry import get_domain_registry
from project.domain.hypotheses import HypothesisSpec, TriggerType
from project.domain.models import DomainRegistry
from project.research.context_labels import canonicalize_context_label
from project.research.search.compatibility import validate_event_template_compatibility
from project.research.search.evaluator_utils import load_context_state_map
from project.research.search.role_contracts import validate_standalone_event_role


@dataclass(frozen=True)
class FeasibilityDrop:
    hypothesis_id: str
    trigger_key: str
    template_id: str
    reason: str
    reasons: tuple[str, ...] = ()
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "hypothesis_id": self.hypothesis_id,
            "trigger_key": self.trigger_key,
            "template_id": self.template_id,
            "reason": self.reason,
            "reasons": list(self.reasons),
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class FeasibilityReport:
    generated: int
    feasible: int
    dropped: tuple[FeasibilityDrop, ...] = ()

    @property
    def dropped_count(self) -> int:
        return len(self.dropped)

    def counts_by_reason(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for drop in self.dropped:
            reason = str(drop.reason or "infeasible")
            counts[reason] = counts.get(reason, 0) + 1
        return dict(sorted(counts.items()))

    def examples_by_reason(self, *, limit: int = 5) -> dict[str, list[dict[str, Any]]]:
        examples: dict[str, list[dict[str, Any]]] = {}
        for drop in self.dropped:
            reason = str(drop.reason or "infeasible")
            bucket = examples.setdefault(reason, [])
            if len(bucket) < int(limit):
                bucket.append(drop.to_dict())
        return examples

    def summary(self, *, limit: int = 5) -> dict[str, Any]:
        return {
            "generated": int(self.generated),
            "feasible": int(self.feasible),
            "dropped": int(self.dropped_count),
            "counts_by_reason": self.counts_by_reason(),
            "examples": self.examples_by_reason(limit=limit),
        }

    def to_dict(self, *, limit: int = 25) -> dict[str, Any]:
        payload = self.summary(limit=5)
        payload["dropped_examples"] = [d.to_dict() for d in self.dropped[: int(limit)]]
        return payload


class FeasibilityError(ValueError):
    def __init__(self, message: str, report: FeasibilityReport | None = None):
        super().__init__(message)
        self.report = report


def filter_hypotheses_with_report(
    hypotheses: Iterable[HypothesisSpec],
    *,
    features: pd.DataFrame | None = None,
    registry: DomainRegistry | None = None,
) -> tuple[list[HypothesisSpec], FeasibilityReport]:
    registry = registry or get_domain_registry()
    source = list(hypotheses or [])
    survivors: list[HypothesisSpec] = []
    drops: list[FeasibilityDrop] = []
    for spec in source:
        result = check_hypothesis_feasibility(spec, features=features, registry=registry)
        if result.valid:
            survivors.append(spec)
            continue
        drops.append(
            FeasibilityDrop(
                hypothesis_id=spec.hypothesis_id(),
                trigger_key=spec.trigger.label(),
                template_id=str(spec.template_id),
                reason=result.primary_reason or "infeasible",
                reasons=tuple(result.reasons),
                details=dict(result.details),
            )
        )
    return survivors, FeasibilityReport(
        generated=len(source),
        feasible=len(survivors),
        dropped=tuple(drops),
    )


@dataclass(frozen=True)
class FeasibilityResult:
    valid: bool
    reasons: tuple[str, ...] = ()
    details: dict[str, Any] = field(default_factory=dict)

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
    del registry
    errors = validate_event_template_compatibility(spec)
    if errors:
        return errors[0]
    return None


def _context_feasibility(
    context: Mapping[str, str] | None,
    available_columns: set[str] | None,
) -> tuple[list[str], dict[str, Any]]:
    if not context:
        return [], {}

    reasons: list[str] = []
    details: dict[str, Any] = {}
    try:
        context_map = load_context_state_map()
    except Exception:
        return ["context_registry_unavailable"], details

    for family, label in context.items():
        canonical_label = canonicalize_context_label(family, label)
        if available_columns is not None and str(family) in available_columns:
            continue
        if available_columns is None:
            continue
        state_id = context_map.get((family, canonical_label))
        if state_id is None:
            reasons.append("unknown_context_mapping")
            details[f"context:{family}:{label}"] = "unmapped"
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
    details: dict[str, Any] = {}

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

    role_errors = validate_standalone_event_role(spec)
    if role_errors:
        reasons.append(role_errors[0])
        details["primary_event_id"] = spec.trigger.event_id or ""

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
