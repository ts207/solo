# tests/research/promotion/test_deploy_mode_policy.py

from __future__ import annotations

import numpy as np
from project.research.promotion.core import evaluate_row


def _kwargs():
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
        "promotion_confirmatory_gates": {
            "deployable": {"min_regimes_supported": 2},
            "shadow": {"min_oos_event_count": 20},
        },
    }


def _base_row():
    return {
        "event_type": "VOL_SHOCK",
        "n_events": 60,
        "q_value": 0.01,
        "event_is_descriptive": False,
        "event_is_trade_trigger": True,
        "gate_tob_coverage": True,
        "gate_net_expectancy": True,
        "gate_cost_budget": True,
        "gate_turnover": True,
        "tob_coverage": 0.8,
        "net_expectancy_bps": 10.0,
        "effective_cost_bps": 2.0,
        "turnover_proxy_mean": 0.4,
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
        "gate_after_cost_stressed_positive": True,
        "gate_delayed_entry_stress": True,
        "q_value_family": 0.05,
        "q_value_cluster": 0.25,  # fails strict deploy cluster gate
        "q_value_by": 0.05,
        "q_value_program": 0.05,
        "shrinkage_loso_stable": True,
        "shrinkage_borrowing_dominant": False,
        "structural_robustness_score": 0.8,
        "repeated_fold_consistency": 0.8,
        "robustness_panel_complete": True,
        "regime_counts": {"r1": 20, "r2": 20},
        "gate_regime_stability": True,
        "gate_structural_break": True,
        "bridge_certified": True,
        "gate_bridge_low_capital_viability": True,
        "low_capital_viability_score": 0.9,
        "low_capital_reject_reason_codes": "",
    }


def test_research_mode_does_not_activate_deploy_only_cluster_gate():
    row = _base_row()
    row["run_mode"] = "research"
    result = evaluate_row(row=row, **_kwargs())
    assert result["is_deploy_mode"] is False
    assert "multiplicity_cluster_q" not in result["deploy_only_reject_reason"]


def test_deploy_mode_activates_deploy_only_cluster_gate():
    row = _base_row()
    row["run_mode"] = "production"
    result = evaluate_row(row=row, **_kwargs())
    assert result["is_deploy_mode"] is True
    assert "multiplicity_cluster_q" in result["deploy_only_reject_reason"]


def test_run_mode_is_normalized():
    row = _base_row()
    row["run_mode"] = " Production "
    result = evaluate_row(row=row, **_kwargs())
    assert result["run_mode_normalized"] == "production"
    assert result["is_deploy_mode"] is True
