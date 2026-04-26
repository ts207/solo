from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class HypothesisRecord:
    """Typed representation of a single generated hypothesis before evaluation."""
    hypothesis_id: str
    event_type: str
    template_id: str
    direction: str
    horizon: str
    entry_lag_bars: int
    symbol: str
    run_id: str
    search_spec_name: str = ""
    context: dict[str, str] = field(default_factory=dict)
    trigger_type: str = "event"
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CandidateRecord:
    """Typed representation of an evaluated hypothesis that passed initial gates."""
    candidate_id: str
    hypothesis_id: str
    event_type: str
    symbol: str
    run_id: str
    estimate_bps: float
    t_stat: float
    robustness: float
    n_obs: int
    direction: str
    horizon: str
    template_id: str
    canonical_regime: str = ""
    promotion_eligible: bool = False
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ValidationRecord:
    """Typed record of a candidate's validation outcome."""
    candidate_id: str
    hypothesis_id: str
    event_type: str
    symbol: str
    run_id: str
    passed: bool
    rejection_reasons: tuple[str, ...] = ()
    q_value: float = float("nan")
    effective_q_value: float = float("nan")
    num_tests_scope: int = 0
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PromotionRecord:
    """Typed record of a candidate's promotion decision."""
    candidate_id: str
    event_type: str
    symbol: str
    run_id: str
    promotion_decision: str
    promotion_track: str
    policy_version: str
    bundle_version: str
    is_reduced_evidence: bool = False
    promotion_class: str = ""
    readiness_status: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResearchDecisionTrace:
    """End-to-end trace linking a hypothesis through evaluation, validation, and promotion.

    Populated by the research services as a single auditable record per candidate.
    """
    hypothesis: HypothesisRecord
    candidate: CandidateRecord | None = None
    validation: ValidationRecord | None = None
    promotion: PromotionRecord | None = None

    @property
    def reached_promotion(self) -> bool:
        return self.promotion is not None

    @property
    def final_decision(self) -> str:
        if self.promotion is not None:
            return self.promotion.promotion_decision
        if self.validation is not None:
            return "rejected" if not self.validation.passed else "not_promoted"
        if self.candidate is None:
            return "filtered"
        return "pending"
