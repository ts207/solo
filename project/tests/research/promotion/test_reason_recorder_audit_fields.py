# tests/research/promotion/test_reason_recorder_audit_fields.py

from __future__ import annotations

import json

from project.research.promotion.core import evaluate_row


def _kwargs():
    return {
        "hypothesis_index": {},
        "negative_control_summary": {},
        "max_q_value": 0.10,
        "min_events": 20,
        "min_stability_score": 0.5,
        "min_sign_consistency": 0.8,
        "min_cost_survival_ratio": 0.9,
        "max_negative_control_pass_rate": 0.2,
        "min_tob_coverage": 0.5,
        "require_hypothesis_audit": False,
        "allow_missing_negative_controls": True,
        "min_net_expectancy_bps": 1.0,
        "max_fee_plus_slippage_bps": 1.0,
        "max_daily_turnover_multiple": 1.0,
        "require_retail_viability": True,
        "require_low_capital_viability": False,
        "require_multiplicity_diagnostics": False,
        "min_dsr": 0.0,
        "promotion_confirmatory_gates": {},
    }


def _row():
    return {
        "event_type": "LIQUIDATION_CASCADE",
        "run_mode": "research",
        "n_events": 50,
        "q_value": 0.01,
        "event_is_descriptive": False,
        "event_is_trade_trigger": True,
        "gate_tob_coverage": True,
        "gate_net_expectancy": False,
        "gate_cost_budget": False,
        "gate_turnover": False,
        "tob_coverage": 0.8,
        "bridge_validation_after_cost_bps": -1.0,
        "bridge_effective_cost_bps_per_trade": 2.0,
        "turnover_proxy_mean": 2.0,
        "baseline_expectancy_bps": 5.0,
        "pass_shift_placebo": False,
        "pass_random_entry_placebo": True,
        "pass_direction_reversal_placebo": True,
        "gate_stability": False,
        "gate_delay_robustness": False,
        "effect_shrunk_state": 0.1,
        "std_return": 1.0,
        "val_t_stat": 0.0,
        "oos1_t_stat": 0.0,
        "test_t_stat": 0.0,
        "gate_bridge_microstructure": False,
        "gate_after_cost_stressed_positive": False,
        "gate_delayed_entry_stress": False,
        "q_value_family": 0.05,
        "q_value_cluster": 0.05,
        "q_value_by": 0.05,
        "q_value_program": 0.05,
        "gate_bridge_low_capital_viability": True,
        "low_capital_viability_score": 0.9,
        "low_capital_reject_reason_codes": "",
    }


def test_reason_category_audit_fields_are_emitted():
    result = evaluate_row(row=_row(), **_kwargs())
    reject_payload = json.loads(result["reject_reason_categories_json"])
    promo_payload = json.loads(result["promotion_fail_reason_categories_json"])

    assert "retail_viability" in reject_payload
    assert "falsification" in reject_payload
    assert "stability" in reject_payload
    assert "microstructure" in reject_payload
    assert "stress_tests" in reject_payload

    assert "retail_viability" in promo_payload
    assert "falsification" in promo_payload
    assert "stability" in promo_payload
    assert "microstructure" in promo_payload
    assert "stress_tests" in promo_payload


def test_primary_promo_fail_still_comes_from_first_fail_reason():
    result = evaluate_row(row=_row(), **_kwargs())
    assert result["promotion_fail_gate_primary"] != ""
    assert isinstance(result["promotion_fail_gate_primary"], str)
