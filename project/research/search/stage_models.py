from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from project.domain.hypotheses import HypothesisSpec
from project.research.search.feasibility import FeasibilityResult


def _jsonify_mapping(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    if not payload:
        return {}
    return {str(k): v for k, v in payload.items()}


@dataclass(frozen=True)
class CandidateHypothesis:
    spec: HypothesisSpec
    search_spec_name: str
    origin_stage: str = "generator"

    @property
    def hypothesis_id(self) -> str:
        return self.spec.hypothesis_id()

    @property
    def branch_hash(self) -> str:
        return self.spec.semantic_branch_hash()

    def to_record(self) -> dict[str, Any]:
        return {
            "hypothesis_id": self.spec.hypothesis_id(),
            "branch_hash": self.spec.semantic_branch_hash(),
            "branch_key": self.spec.semantic_branch_key(),
            "trigger_type": self.spec.trigger.trigger_type,
            "trigger_key": self.spec.trigger.label(),
            "direction": self.spec.direction,
            "horizon": self.spec.horizon,
            "template_id": self.spec.template_id,
            "filter_template_id": self.spec.filter_template_id,
            "entry_lag": int(self.spec.entry_lag),
            "entry_lag_bars": int(self.spec.entry_lag),
            "context": dict(self.spec.context or {}),
            "search_spec_name": self.search_spec_name,
            "origin_stage": self.origin_stage,
            "status": "candidate",
        }


@dataclass(frozen=True)
class FeasibilityCheckedHypothesis:
    candidate: CandidateHypothesis
    feasibility: FeasibilityResult

    @property
    def valid(self) -> bool:
        return bool(self.feasibility.valid)

    def to_record(self) -> dict[str, Any]:
        row = self.candidate.to_record()
        row.update(
            {
                "status": "feasible" if self.feasibility.valid else "rejected",
                "rejection_reason": None
                if self.feasibility.valid
                else self.feasibility.primary_reason,
                "rejection_reasons": list(self.feasibility.reasons),
                "rejection_details": dict(self.feasibility.details),
            }
        )
        return row


@dataclass(frozen=True)
class EvaluatedHypothesis:
    checked: FeasibilityCheckedHypothesis
    metrics: dict[str, Any] = field(default_factory=dict)
    valid: bool = False
    invalid_reason: str | None = None

    @property
    def spec(self) -> HypothesisSpec:
        return self.checked.candidate.spec

    def to_record(self) -> dict[str, Any]:
        row = self.checked.to_record()
        row.update(
            {
                "status": "evaluated",
                "valid": bool(self.valid),
                "invalid_reason": self.invalid_reason,
            }
        )
        row.update(_jsonify_mapping(self.metrics))
        return row
