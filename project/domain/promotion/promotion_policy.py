from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class PromotionPolicy:
    max_q_value: float
    min_events: int
    min_stability_score: float
    min_sign_consistency: float
    min_cost_survival_ratio: float
    min_tob_coverage: float
    max_negative_control_pass_rate: float = 0.01
    promotion_profile: str = "deploy"
    require_hypothesis_audit: bool = False
    allow_missing_negative_controls: bool = False
    require_multiplicity_diagnostics: bool = False
    require_retail_viability: bool = False
    require_low_capital_viability: bool = False
    enforce_baseline_beats_complexity: bool = True
    enforce_placebo_controls: bool = True
    enforce_timeframe_consensus: bool = True
    enforce_regime_stability: bool = True
    policy_version: str = "phase4_pr5_v1"
    bundle_version: str = "phase4_bundle_v1"
    multiplicity_scope_mode: str = "campaign_lineage"
    require_scope_level_multiplicity: bool = True
    allow_multiplicity_scope_degraded: bool = True
    artifact_audit_version: str = "phase1_v1"
    use_effective_q_value: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


__all__ = ["PromotionPolicy"]
