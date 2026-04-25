from project.domain.promotion.promotion_policy import PromotionPolicy
from project.research.promotion.promotion_decisions import evaluate_row
from project.research.validation.evidence_bundle import evaluate_promotion_bundle


def test_evidence_bundle_3_state_logic():
    policy = PromotionPolicy(
        policy_version="test_v1",
        bundle_version="test_v1",
        min_events=40,
        max_q_value=0.05,
        min_stability_score=0.5,
        min_sign_consistency=0.5,
        min_cost_survival_ratio=0.5,
        min_tob_coverage=0.5,
        require_multiplicity_diagnostics=False,
        require_hypothesis_audit=False,
        require_retail_viability=False,
        require_low_capital_viability=False,
        enforce_baseline_beats_complexity=False,
        enforce_placebo_controls=False,
        enforce_timeframe_consensus=False,
        enforce_regime_stability=False,
    )

    # 1. Complete positive evidence
    row_pass = {
        "candidate_id": "test_cand",
        "q_value": 0.01,
        "n_events": 100,
        "stability_score": 0.8,
        "sign_consistency": 0.8,
        "gate_stability": True,
        "delay_robustness_pass": True,
        "cost_survival_ratio": 0.9,
        "microstructure_pass": True,
        "tob_coverage": 0.8,
        "gate_after_cost_stressed_positive": True,
        "gate_delayed_entry_stress": True,
        "gate_promo_dsr": True,
        "gate_promo_robustness": True,
        "gate_promo_regime": True,
        "gate_promo_falsification": True,
        "gate_promo_timeframe_consensus": True,
        "gate_promo_oos_validation": True,
        "gate_promo_hypothesis_audit": True,
        "gate_promo_multiplicity_confirmatory": True,
        "event_is_descriptive": False,
        "event_is_trade_trigger": True,
    }

    res_pass = evaluate_promotion_bundle(row_pass, policy)
    assert res_pass["promotion_status"] == "promoted"
    assert res_pass["gate_results"]["statistical"] == "pass"
    assert res_pass["gate_results"]["microstructure"] == "pass"

    # 2. explicit failure
    row_fail = dict(row_pass)
    row_fail["q_value"] = 0.5  # fails max_q_value
    res_fail = evaluate_promotion_bundle(row_fail, policy)
    assert res_fail["promotion_status"] == "rejected"
    assert res_fail["gate_results"]["statistical"] == "fail"

    # 3. Missing evidence
    row_missing = dict(row_pass)
    del row_missing["microstructure_pass"]
    res_missing = evaluate_promotion_bundle(row_missing, policy)
    assert res_missing["promotion_status"] == "rejected"
    assert res_missing["gate_results"]["microstructure"] == "missing_evidence"


def test_promotion_decisions_handles_string_gates():
    dummy_row = {
        "candidate_id": "test_cand_2",
        "event_type": "LIQUIDATION_CASCADE",
        "symbol": "BTCUSDT",
        "plan_row_id": "plan_x",
        "horizon": "24b",
        "q_value_family": 0.05,
        "q_value_cluster": 0.05,
        "net_expectancy_bps": 10.0,
        "turnover_pass": True,
        "effective_cost_bps": 2.0,
        "bridge_certified": True,
    }

    try:
        from project.research.promotion.promotion_decisions import _eval_pre_bundle
    except ImportError:
        pass

    # The actual integration is tested by ensure we don't crash and we get the requested strings out.

    hypothesis_index = {"plan_x": {"statuses": ["executed"]}}
    result = evaluate_row(
        row=dummy_row,
        hypothesis_index=hypothesis_index,
        negative_control_summary={},
        max_q_value=0.05,
        min_events=50,
        min_stability_score=0.5,
        min_sign_consistency=0.5,
        min_cost_survival_ratio=0.5,
        min_tob_coverage=0.5,
        require_multiplicity_diagnostics=False,
        require_hypothesis_audit=False,
        max_negative_control_pass_rate=0.5,
        allow_missing_negative_controls=True,
        require_retail_viability=False,
        require_low_capital_viability=False,
        enforce_baseline_beats_complexity=False,
        enforce_placebo_controls=False,
        enforce_timeframe_consensus=False,
        min_dsr=0.0,
        promotion_confirmatory_gates=False,
        policy_version="v1",
        bundle_version="v1",
        promotion_profile="test",
    )

    # Expect 3-state strings
    assert result["promotion_audit"]["gate_promo_statistical"] in (
        "pass",
        "fail",
        "missing_evidence",
    )
    assert result["promotion_audit"]["gate_promo_microstructure"] in (
        "pass",
        "fail",
        "missing_evidence",
    )
