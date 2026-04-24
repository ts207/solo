import pytest

from project.research.promotion import evaluate_row


def test_promotion_tob_coverage_gate():
    # Base row that passes all other gates
    row = {
        "candidate_id": "test_1",
        "event_type": "VOL_SHOCK",
        "n_events": 200,
        "q_value": 0.01,
        "gate_stability": "pass",
        "val_t_stat": 3.0,
        "oos1_t_stat": 2.5,
        "train_t_stat": 4.0,
        "std_return": 0.01,
        "expectancy": 0.001,
        "gate_after_cost_positive": "pass",
        "gate_after_cost_stressed_positive": "pass",
        "gate_bridge_after_cost_positive_validation": "pass",
        "gate_bridge_after_cost_stressed_positive_validation": "pass",
        "gate_delay_robustness": "pass",
        "validation_samples": 100,
        "tob_coverage": 0.9,  # High coverage
        "baseline_expectancy_bps": 5.0,
        "bridge_validation_after_cost_bps": 20.0,
        "pass_shift_placebo": "pass",
        "pass_random_entry_placebo": "pass",
        "pass_direction_reversal_placebo": "pass",
        "event_is_descriptive": False,
        "event_is_trade_trigger": True,
        "gate_delayed_entry_stress": "pass",
        "gate_bridge_microstructure": "pass",
        "net_expectancy_bps": 20.0,
    }  # 1. High coverage -> standard promotion
    res = evaluate_row(
        row=row,
        hypothesis_index={},
        negative_control_summary={},
        max_q_value=0.1,
        min_events=100,
        min_stability_score=0.0,
        min_sign_consistency=0.0,
        min_cost_survival_ratio=0.0,
        max_negative_control_pass_rate=1.0,
        min_tob_coverage=0.8,
        require_hypothesis_audit=False,
        allow_missing_negative_controls=True,
    )
    assert res["promotion_decision"] == "promoted"
    assert res["promotion_track"] == "standard"
    assert res["gate_promo_tob_coverage"] == "pass"

    # 2. Low coverage -> fallback_only promotion
    row_low = row.copy()
    row_low["tob_coverage"] = 0.5
    res = evaluate_row(
        row=row_low,
        hypothesis_index={},
        negative_control_summary={},
        max_q_value=0.1,
        min_events=100,
        min_stability_score=0.0,
        min_sign_consistency=0.0,
        min_cost_survival_ratio=0.0,
        max_negative_control_pass_rate=1.0,
        min_tob_coverage=0.8,
        require_hypothesis_audit=False,
        allow_missing_negative_controls=True,
    )
    assert res["promotion_decision"] == "promoted"
    assert res["promotion_track"] == "fallback_only"
    assert res["gate_promo_tob_coverage"] == "fail"


if __name__ == "__main__":
    pytest.main([__file__])
