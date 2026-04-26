from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class ValidationDecision:
    status: str  # validated, rejected, inconclusive
    candidate_id: str
    run_id: str
    program_id: str | None = None
    reason_codes: list[str] = field(default_factory=list)
    summary: str = ""

    def __post_init__(self):
        allowed = {"validated", "rejected", "inconclusive"}
        if self.status not in allowed:
            raise ValueError(f"status must be one of {allowed}, got {self.status}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ValidationMetrics:
    sample_count: int | None = None
    effective_sample_size: float | None = None
    expectancy: float | None = None
    net_expectancy: float | None = None
    hit_rate: float | None = None
    p_value: float | None = None
    q_value: float | None = None
    stability_score: float | None = None
    cost_sensitivity: float | None = None
    turnover: float | None = None
    regime_support_score: float | None = None
    time_slice_support_score: float | None = None
    negative_control_score: float | None = None
    max_drawdown: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ValidationArtifactRef:
    artifact_type: str
    path: str
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ValidatedCandidateRecord:
    candidate_id: str
    decision: ValidationDecision
    metrics: ValidationMetrics
    anchor_summary: str = ""
    template_id: str = ""
    direction: str = ""
    horizon_bars: int = 0
    artifact_refs: list[ValidationArtifactRef] = field(default_factory=list)
    detector_lineage: dict[str, Any] = field(default_factory=dict)
    validation_stage_version: str = "v1"

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "decision": self.decision.to_dict(),
            "metrics": self.metrics.to_dict(),
            "anchor_summary": self.anchor_summary,
            "template_id": self.template_id,
            "direction": self.direction,
            "horizon_bars": self.horizon_bars,
            "artifact_refs": [ref.to_dict() for ref in self.artifact_refs],
            "detector_lineage": dict(self.detector_lineage),
            "validation_stage_version": self.validation_stage_version,
        }


@dataclass(frozen=True)
class ValidationBundle:
    run_id: str
    created_at: str
    validated_candidates: list[ValidatedCandidateRecord] = field(default_factory=list)
    rejected_candidates: list[ValidatedCandidateRecord] = field(default_factory=list)
    inconclusive_candidates: list[ValidatedCandidateRecord] = field(default_factory=list)
    program_id: str | None = None
    summary_stats: dict[str, Any] = field(default_factory=dict)
    effect_stability_report: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "program_id": self.program_id,
            "created_at": self.created_at,
            "validated_candidates": [c.to_dict() for c in self.validated_candidates],
            "rejected_candidates": [c.to_dict() for c in self.rejected_candidates],
            "inconclusive_candidates": [c.to_dict() for c in self.inconclusive_candidates],
            "summary_stats": self.summary_stats,
            "effect_stability_report": self.effect_stability_report,
        }


# Canonical Validation Reason Codes
class ValidationReasonCodes:
    INSUFFICIENT_SAMPLE_SUPPORT = "insufficient_sample_support"
    INSUFFICIENT_EFFECTIVE_SAMPLE_SIZE = "insufficient_effective_sample_size"
    FAILED_OOS_VALIDATION = "failed_oos_validation"
    FAILED_COST_SURVIVAL = "failed_cost_survival"
    FAILED_STABILITY = "failed_stability"
    FAILED_TIME_SLICE_SUPPORT = "failed_time_slice_support"
    FAILED_REGIME_SUPPORT = "failed_regime_support"
    FAILED_NEGATIVE_CONTROLS = "failed_negative_controls"
    FAILED_MULTIPLICITY_THRESHOLD = "failed_multiplicity_threshold"
    MECHANICAL_INVALIDITY = "mechanical_invalidity"
    SEMANTIC_INVALIDITY = "semantic_invalidity"
    INSUFFICIENT_DATA = "insufficient_data"
    INCONCLUSIVE_VALIDATION = "inconclusive_validation"


# Canonical Promotion Reason Codes
class PromotionReasonCodes:
    NOT_VALIDATED = "not_validated"
    BELOW_PROMOTION_PRIORITY = "below_promotion_priority"
    INVENTORY_BUDGET_EXCEEDED = "inventory_budget_exceeded"
    REDUNDANT_WITH_EXISTING_THESIS = "redundant_with_existing_thesis"
    PACKAGING_INCOMPLETE = "packaging_incomplete"
    OBJECTIVE_MISMATCH = "objective_mismatch"
    RETAIL_PROFILE_MISMATCH = "retail_profile_mismatch"
    DEPLOYMENT_NOT_PERMITTED = "deployment_not_permitted"
