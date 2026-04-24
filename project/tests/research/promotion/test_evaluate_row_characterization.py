# tests/research/promotion/test_evaluate_row_characterization.py

from __future__ import annotations

import math
from copy import deepcopy

from project.research.promotion.core import evaluate_row


def _base_kwargs():
    return {
        "hypothesis_index": {},
        "negative_control_summary": {},
        "max_q_value": 0.10,
        "min_events": 20,
        "min_stability_score": 0.1,
        "min_sign_consistency": 0.5,
        "min_cost_survival_ratio": 0.5,
        "max_negative_control_pass_rate": 0.2,
        "min_tob_coverage": 0.5,
        "require_hypothesis_audit": False,
        "allow_missing_negative_controls": True,
        "min_net_expectancy_bps": 0.0,
        "max_fee_plus_slippage_bps": None,
        "max_daily_turnover_multiple": None,
        "require_retail_viability": False,
        "require_low_capital_viability": False,
        "require_multiplicity_diagnostics": False,
        "min_dsr": 0.0,
        "promotion_confirmatory_gates": {},
    }


def _passing_row():
    return {
        "event_type": "VOL_SHOCK",
        "n_events": 50,
        "q_value": 0.01,
        "event_is_descriptive": False,
        "event_is_trade_trigger": True,
        "gate_tob_coverage": True,
        "gate_net_expectancy": True,
        "gate_cost_budget": True,
        "gate_turnover": True,
        "tob_coverage": 0.8,
        "bridge_validation_after_cost_bps": 12.0,
        "sharpe_ratio": 2.0,
        "bridge_effective_cost_bps_per_trade": 2.0,
        "turnover_proxy_mean": 0.5,
        "gate_after_cost_positive": True,
        "gate_after_cost_stressed_positive": True,
        "baseline_expectancy_bps": 5.0,
        "pass_shift_placebo": True,
        "pass_random_entry_placebo": True,
        "pass_direction_reversal_placebo": True,
        "gate_stability": True,
        "gate_delay_robustness": True,
        "effect_shrunk_state": 1.0,
        "std_return": 1.0,
        "val_t_stat": 1.0,
        "oos1_t_stat": 1.0,
        "test_t_stat": 1.0,
        "gate_bridge_microstructure": True,
        "gate_delayed_entry_stress": True,
        "control_pass_rate": 0.05,
        "q_value_by": 0.05,
        "q_value_cluster": 0.05,
        "q_value_family": 0.05,
        "q_value_program": 0.05,
        "run_mode": "research",
        "gate_bridge_low_capital_viability": True,
        "low_capital_viability_score": 0.9,
        "low_capital_reject_reason_codes": "",
    }


def test_evaluate_row_characterization_pass_case():
    row = _passing_row()
    result = evaluate_row(row=deepcopy(row), **_base_kwargs())
    assert result["promotion_decision"] == "promoted", f"Result: {result}"
    assert result["promotion_track"] in {"standard", "fallback_only"}
    assert "promotion_score" in result
    assert "reject_reason" in result
    assert "gate_promo_statistical" in result
    assert "gate_promo_timeframe_consensus" in result


def test_evaluate_row_characterization_dsr_failure():
    row = _passing_row()
    row["returns_oos_combined"] = [0.0] * 5  # Too few samples for DSR
    kwargs = _base_kwargs()
    kwargs["min_dsr"] = 0.95
    result = evaluate_row(row=row, **kwargs)
    assert result["gate_promo_dsr"] is False, f"Result: {result}"
    assert (
        "dsr" in result["promotion_fail_gate_primary"] or result["promotion_decision"] == "rejected"
    )


def test_evaluate_row_characterization_low_capital_failure():
    row = _passing_row()
    row["gate_bridge_low_capital_viability"] = False
    row["low_capital_reject_reason_codes"] = "INSUFFICIENT_LIQUIDITY"
    kwargs = _base_kwargs()
    kwargs["require_low_capital_viability"] = True
    result = evaluate_row(row=row, **kwargs)
    assert result["gate_promo_low_capital_viability"] is False
    assert "low_capital_viability" in result["reject_reason"]


def test_evaluate_row_characterization_nan_oos_sample_counts_fail_closed():
    row = _passing_row()
    row["validation_samples"] = math.nan
    row["test_samples"] = math.nan
    row["mean_validation_return"] = 0.01
    row["mean_test_return"] = 0.01
    kwargs = _base_kwargs()
    kwargs["promotion_confirmatory_gates"] = {"shadow": {"min_oos_event_count": 1}}

    result = evaluate_row(row=row, **kwargs)

    assert result["promotion_decision"] == "rejected"
    assert "gate_promo_oos_validation" in str(result["promotion_fail_gate_primary"])


def test_evaluate_row_characterization_uses_bridge_validation_trades_when_sample_columns_missing():
    row = _passing_row()
    row["validation_samples"] = math.nan
    row["test_samples"] = math.nan
    row["bridge_validation_trades"] = 25
    kwargs = _base_kwargs()
    kwargs["promotion_confirmatory_gates"] = {"shadow": {"min_oos_event_count": 20}}

    result = evaluate_row(row=row, **kwargs)

    assert result["gate_promo_oos_validation"] == "pass"
    assert result["validation_samples_raw"] is None
    assert result["validation_samples"] == 25
    assert result["oos_sample_source"] == "row.bridge_validation_trades"
    assert "gate_promo_oos_validation" not in str(result["promotion_fail_gate_primary"])


def test_evaluate_row_characterization_does_not_fail_closed_on_missing_baseline():
    row = _passing_row()
    row.pop("baseline_expectancy_bps", None)

    result = evaluate_row(row=row, **_base_kwargs())

    assert result["promotion_decision"] == "promoted"
    assert result["gate_promo_baseline_beats_complexity"] is True
    assert "failed_baseline_comparison" not in result["reject_reason"]


def test_evaluate_row_characterization_enforces_program_level_q_value():
    row = _passing_row()
    row["q_value"] = 0.01
    row["q_value_program"] = 0.90

    result = evaluate_row(row=row, **_base_kwargs())

    assert result["promotion_decision"] == "rejected"
    assert result["gate_promo_statistical"] == "fail"
    assert "statistical_program_q_value" in result["reject_reason"]


def test_evaluate_row_characterization_flags_continuation_quality_fragility():
    row = _passing_row()
    row["template_verb"] = "continuation"
    row["gate_bridge_tradable"] = True
    row["gate_bridge_microstructure"] = False
    row["validation_samples"] = 0
    row["test_samples"] = 0
    kwargs = _base_kwargs()
    kwargs["promotion_confirmatory_gates"] = {"shadow": {"min_oos_event_count": 1}}
    kwargs["min_dsr"] = 0.95

    result = evaluate_row(row=row, **kwargs)

    assert result["promotion_decision"] == "rejected"
    assert result["is_continuation_template_family"] is True
    assert result["gate_bridge_tradable"] == "pass"
    assert result["gate_promo_continuation_quality"] == "fail"
    assert "continuation_quality_microstructure" in result["reject_reason"]
    assert (
        "gate_promo_continuation_quality" in str(result["promotion_fail_gate_primary"])
        or "continuation_quality" in result["reject_reason"]
    )


def test_evaluate_row_characterization_honors_scope_multiplicity_policy_flags():
    row = _passing_row()
    row["q_value_scope"] = math.nan
    row["multiplicity_scope_mode"] = "campaign_lineage"
    kwargs = _base_kwargs()
    kwargs["require_scope_level_multiplicity"] = True

    result = evaluate_row(row=row, **kwargs)

    assert result["promotion_decision"] == "rejected"
    assert "gate_promo_multiplicity_scope" in str(result["promotion_fail_gate_primary"])


def test_evaluate_row_characterization_can_disable_effective_q_check_when_requested():
    row = _passing_row()
    row["q_value"] = 0.01
    row["q_value_scope"] = 0.90
    row["multiplicity_scope_mode"] = "campaign_lineage"
    kwargs = _base_kwargs()
    kwargs["require_scope_level_multiplicity"] = False
    kwargs["use_effective_q_value"] = False

    result = evaluate_row(row=row, **kwargs)

    assert result["promotion_decision"] == "promoted"
    assert result["gate_promo_statistical"] == "pass"


def test_evaluate_row_characterization_uses_upstream_effective_q_value():
    row = _passing_row()
    row["q_value"] = 0.01
    row["q_value_program"] = 0.05
    row["effective_q_value"] = 0.90

    result = evaluate_row(row=row, **_base_kwargs())

    assert result["promotion_decision"] == "rejected"
    assert result["gate_promo_statistical"] == "fail"


def test_evaluate_row_characterization_respects_upstream_oos_gate_when_present():
    row = _passing_row()
    row["validation_samples"] = 25
    row["test_samples"] = 25
    row["mean_validation_return"] = 0.01
    row["mean_test_return"] = 0.01
    row["gate_oos_validation"] = False
    kwargs = _base_kwargs()
    kwargs["promotion_confirmatory_gates"] = {"shadow": {"min_oos_event_count": 20}}

    result = evaluate_row(row=row, **kwargs)

    assert result["promotion_decision"] == "rejected"
    assert result["gate_promo_oos_validation"] == "fail"
