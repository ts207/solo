from __future__ import annotations

from project.research.promotion.promotion_reporting_support import resolve_promotion_tier


def _promoted_row(**overrides):
    row = {
        "promotion_decision": "promoted",
        "promotion_track": "standard",
        "effective_q_value": 0.01,
        "test_q_value": 0.01,
        "n_events": 250,
        "validation_samples": 100,
        "test_samples": 100,
        "gate_bridge_tradable": "pass",
        "gate_promo_retail_viability": "pass",
        "gate_promo_redundancy": "pass",
        "gate_promo_dsr": "pass",
        "dsr_value": 0.8,
        "cost_survival_ratio": 1.2,
        "gate_regime_stability": True,
        "num_regimes_supported": 3,
        "gate_promo_robustness": "pass",
        "gate_promo_multiplicity_confirmatory": "pass",
        "gate_promo_multiplicity_diagnostics": "pass",
    }
    row.update(overrides)
    return row


def test_deploy_grade_promotion_has_stricter_live_eligible_tier() -> None:
    assert resolve_promotion_tier(_promoted_row()) == "live_eligible"


def test_weak_statistically_lucky_candidate_fails_live_eligibility() -> None:
    lucky = _promoted_row(
        effective_q_value=0.049,
        n_events=55,
        validation_samples=20,
        test_samples=20,
        dsr_value=0.2,
        cost_survival_ratio=0.75,
        num_regimes_supported=1,
    )

    assert resolve_promotion_tier(lucky) == "paper_eligible"
