from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def _coerce(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, BaseModel):
        return {k: _coerce(v) for k, v in value.model_dump().items()}
    if is_dataclass(value):
        return {k: _coerce(v) for k, v in asdict(value).items()}
    if isinstance(value, dict):
        return {str(k): _coerce(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_coerce(v) for v in value]
    return value


class ValidationSplit(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    label: str
    start: Any
    end: Any
    purge_bars: int = 0
    embargo_bars: int = 0
    bar_duration_minutes: int = 5

    @field_validator("start", "end", mode="before")
    @classmethod
    def coerce_timestamp(cls, v: Any) -> Any:
        if isinstance(v, str):
            return pd.Timestamp(v)
        return v

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "start": self.start.isoformat() if isinstance(self.start, pd.Timestamp) else str(self.start),
            "end": self.end.isoformat() if isinstance(self.end, pd.Timestamp) else str(self.end),
            "purge_bars": self.purge_bars,
            "embargo_bars": self.embargo_bars,
            "bar_duration_minutes": self.bar_duration_minutes,
        }


class FoldDefinition(BaseModel):
    fold_id: int
    train_split: ValidationSplit
    validation_split: ValidationSplit
    test_split: ValidationSplit

    def to_dict(self) -> dict[str, Any]:
        return {
            "fold_id": self.fold_id,
            "train_split": self.train_split.to_dict(),
            "validation_split": self.validation_split.to_dict(),
            "test_split": self.test_split.to_dict(),
        }


class EffectEstimate(BaseModel):
    estimate: float
    stderr: float
    ci_low: float
    ci_high: float
    p_value_raw: float
    n_obs: int
    n_clusters: int
    method: str
    cluster_col: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return _coerce(self.model_dump())


class MultiplicityResult(BaseModel):
    correction_family_id: str
    correction_method: str
    p_value_raw: float
    p_value_adj: float

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()


class StabilityResult(BaseModel):
    sign_consistency: float
    stability_score: float
    regime_stability_pass: bool
    timeframe_consensus_pass: bool
    delay_robustness_pass: bool
    regime_flip_flag: bool = False
    cross_symbol_sign_consistency: float = 0.0
    rolling_instability_score: float = 0.0
    worst_regime_estimate: float = 0.0
    worst_symbol_estimate: float = 0.0
    details: dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return _coerce(self.model_dump())


class FalsificationResult(BaseModel):
    shift_placebo_pass: bool
    random_placebo_pass: bool
    direction_reversal_pass: bool
    negative_control_pass: bool
    control_pass_rate: float | None = None
    empirical_exceedance: float | None = None
    null_mean: float | None = None
    null_p95: float | None = None
    passes_control: bool = False
    details: dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return _coerce(self.model_dump())


# Nested models for EvidenceBundle

class SampleDefinition(BaseModel):
    n_events: int
    validation_samples: int = 0
    test_samples: int = 0
    symbol: str = ""

    @field_validator("n_events", "validation_samples", "test_samples")
    @classmethod
    def non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("sample counts must be non-negative")
        return v


class SplitDefinition(BaseModel):
    split_scheme_id: str = ""
    purge_bars: int = 0
    embargo_bars: int = 0
    bar_duration_minutes: int = 5


class EffectEstimates(BaseModel):
    estimate: float
    estimate_bps: float
    stderr: float
    stderr_bps: float


class UncertaintyEstimates(BaseModel):
    ci_low: float
    ci_high: float
    ci_low_bps: float
    ci_high_bps: float
    p_value_raw: float
    q_value: float
    q_value_by: float
    q_value_cluster: float
    n_obs: int
    n_clusters: int

    @field_validator("q_value", "q_value_by", "q_value_cluster", mode="after")
    @classmethod
    def q_value_in_range(cls, v: float) -> float:
        import math
        if math.isfinite(v) and not (0.0 <= v <= 1.0):
            raise ValueError(f"q_value must be in [0, 1] when finite, got {v}")
        return v


class CostRobustness(BaseModel):
    model_config = ConfigDict(extra="allow")

    cost_survival_ratio: float
    net_expectancy_bps: float
    effective_cost_bps: float
    turnover_proxy_mean: float
    tob_coverage: float
    tob_coverage_pass: bool
    stressed_cost_pass: bool
    retail_net_expectancy_pass: bool
    retail_cost_budget_pass: bool
    retail_turnover_pass: bool


class MultiplicityAdjustment(BaseModel):
    correction_family_id: str = ""
    correction_method: str = "bh"
    p_value_adj: float
    p_value_adj_by: float
    p_value_adj_holm: float
    q_value_program: float
    q_value_scope: float
    effective_q_value: float
    num_tests_scope: int = 0
    multiplicity_scope_mode: str = ""
    multiplicity_scope_key: str = ""
    multiplicity_scope_version: str = ""
    multiplicity_scope_degraded: bool = False


class EvidenceMetadata(BaseModel):
    model_config = ConfigDict(extra="allow")

    hypothesis_id: str = ""
    plan_row_id: str = ""
    tob_coverage: float
    event_is_descriptive: bool = False
    event_is_trade_trigger: bool = True
    event_contract_tier: str = ""
    event_operational_role: str = ""
    event_deployment_disposition: str = ""
    event_runtime_category: str = ""
    event_requires_stronger_evidence: bool = False
    is_reduced_evidence: bool = False
    bridge_certified: bool = False
    has_realized_oos_path: bool = False
    repeated_fold_consistency: float
    structural_robustness_score: float
    robustness_panel_complete: bool = False
    num_regimes_supported: int = 0
    promotion_track_hint: str = "standard"


class SearchBurden(BaseModel):
    search_proposals_attempted: int = 0
    search_candidates_generated: int = 0
    search_candidates_scored: int = 0
    search_candidates_eligible: int = 0
    search_parameterizations_attempted: int = 0
    search_mutations_attempted: int = 0
    search_directions_tested: int = 0
    search_confirmations_attempted: int = 0
    search_trigger_variants_attempted: int = 0
    search_family_count: int = 0
    search_lineage_count: int = 0
    search_scope_version: str = "phase1_v1"
    search_burden_estimated: bool = False

    @field_validator(
        "search_proposals_attempted", "search_candidates_generated",
        "search_candidates_scored", "search_candidates_eligible",
        "search_parameterizations_attempted", "search_mutations_attempted",
        "search_directions_tested", "search_confirmations_attempted",
        "search_trigger_variants_attempted", "search_family_count",
        "search_lineage_count",
    )
    @classmethod
    def non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("search burden counts must be non-negative")
        return v


_PROMOTION_TRACKS = {"standard", "fallback_only", "restricted", ""}
_PROMOTION_STATUSES = {"promoted", "rejected", "pending", "inconclusive", ""}


class PromotionDecision(BaseModel):
    eligible: bool
    promotion_status: str
    promotion_track: str
    rank_score: float
    rejection_reasons: list[str] = Field(default_factory=list)
    gate_results: dict[str, Literal["pass", "fail", "missing_evidence"]] = Field(default_factory=dict)
    policy_version: str = "phase4_pr5_v1"
    bundle_version: str = "phase4_bundle_v1"

    @field_validator("promotion_track")
    @classmethod
    def track_in_allowed(cls, v: str) -> str:
        if v not in _PROMOTION_TRACKS:
            raise ValueError(f"promotion_track must be one of {_PROMOTION_TRACKS}, got {v!r}")
        return v

    @model_validator(mode="after")
    def status_consistent_with_eligible(self) -> PromotionDecision:
        if self.eligible and self.promotion_status == "rejected":
            raise ValueError("eligible=True but promotion_status='rejected'")
        if not self.eligible and self.promotion_status == "promoted":
            raise ValueError("eligible=False but promotion_status='promoted'")
        return self

    def to_dict(self) -> dict[str, Any]:
        return _coerce(self.model_dump())


class EvidenceBundle(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    candidate_id: str
    primary_event_id: str
    event_family: str
    event_type: str
    run_id: str
    sample_definition: SampleDefinition
    split_definition: SplitDefinition
    effect_estimates: EffectEstimates
    uncertainty_estimates: UncertaintyEstimates
    stability_tests: dict[str, Any] = Field(default_factory=dict)
    falsification_results: dict[str, Any] = Field(default_factory=dict)
    cost_robustness: CostRobustness
    multiplicity_adjustment: MultiplicityAdjustment
    metadata: EvidenceMetadata
    promotion_decision: dict[str, Any] = Field(default_factory=dict)
    rejection_reasons: list[str] = Field(default_factory=list)
    artifacts: dict[str, Any] = Field(default_factory=dict)
    search_burden: SearchBurden = Field(default_factory=SearchBurden)
    policy_version: str = "phase4_pr5_v1"
    bundle_version: str = "phase4_bundle_v1"

    @field_validator("candidate_id", "event_type", "run_id")
    @classmethod
    def non_empty(cls, v: str) -> str:
        if not str(v).strip():
            raise ValueError("must not be empty")
        return v

    @model_validator(mode="after")
    def validate_nested_consistency(self) -> EvidenceBundle:
        if self.stability_tests:
            required_stability = {"sign_consistency", "stability_score"}
            missing = required_stability - set(self.stability_tests.keys())
            if missing:
                raise ValueError(f"stability_tests missing required fields: {missing}")
        if self.falsification_results:
            required_falsification = {"shift_placebo_pass", "random_placebo_pass", "direction_reversal_pass"}
            missing = required_falsification - set(self.falsification_results.keys())
            if missing:
                raise ValueError(f"falsification_results missing required fields: {missing}")
        if self.promotion_decision:
            if "eligible" not in self.promotion_decision:
                raise ValueError("promotion_decision must have 'eligible' field")
            if "promotion_status" not in self.promotion_decision:
                raise ValueError("promotion_decision must have 'promotion_status' field")
        return self

    def to_dict(self) -> dict[str, Any]:
        return _coerce(self.model_dump())
