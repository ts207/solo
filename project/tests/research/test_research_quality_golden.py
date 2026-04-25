from __future__ import annotations

from copy import deepcopy

import pandas as pd

from project.research.promotion import evaluate_row
from project.research.services import candidate_discovery_service as discovery_svc
from project.research.services import promotion_service as promotion_svc
from project.research.services.run_comparison_service import compare_phase2_run_diagnostics


def _promotion_kwargs() -> dict:
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


def _passing_promotion_row() -> dict:
    return {
        "candidate_id": "cand_pass",
        "event_type": "LIQUIDATION_CASCADE",
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


def test_research_golden_robust_candidate_survives_promotion():
    row = _passing_promotion_row()

    result = evaluate_row(row=deepcopy(row), **_promotion_kwargs())

    assert result["promotion_decision"] == "promoted"
    assert result["promotion_fail_gate_primary"] == ""
    assert str(result["reject_reason"] or "") == ""


def test_research_golden_negative_control_failure_blocks_promotion():
    row = _passing_promotion_row()
    row["candidate_id"] = "cand_placebo_fail"
    row["pass_shift_placebo"] = False
    row["control_pass_rate"] = 0.35

    result = evaluate_row(row=deepcopy(row), **_promotion_kwargs())
    annotated = promotion_svc._annotate_promotion_audit_decisions(pd.DataFrame([result]))
    audit_row = annotated.iloc[0]

    assert result["promotion_decision"] == "rejected"
    assert result["promotion_fail_gate_primary"] == "gate_promo_placebo_controls"
    assert audit_row["primary_reject_reason"] == "failed_gate_promo_placebo_controls"
    assert audit_row["weakest_fail_stage"] == "gate_promo_placebo_controls"


def test_research_golden_sample_quality_degradation_surfaces_in_comparison():
    baseline_candidates = pd.DataFrame(
        [
            {
                "candidate_id": "cand_a",
                "symbol": "BTCUSDT",
                "family_id": "fam_1",
                "validation_n_obs": 18,
                "test_n_obs": 14,
                "n_obs": 32,
                "q_value": 0.02,
                "q_value_by": 0.03,
                "estimate_bps": 9.0,
                "resolved_cost_bps": 4.0,
                "is_discovery": True,
            }
        ]
    )
    degraded_candidates = pd.DataFrame(
        [
            {
                "candidate_id": "cand_a",
                "symbol": "BTCUSDT",
                "family_id": "fam_1",
                "validation_n_obs": 0,
                "test_n_obs": 0,
                "n_obs": 0,
                "q_value": 0.60,
                "q_value_by": 0.70,
                "estimate_bps": 1.0,
                "resolved_cost_bps": 9.0,
                "is_discovery": False,
            }
        ]
    )

    baseline_diag = {
        "false_discovery_diagnostics": discovery_svc._build_false_discovery_diagnostics(
            baseline_candidates
        )
    }
    degraded_diag = {
        "false_discovery_diagnostics": discovery_svc._build_false_discovery_diagnostics(
            degraded_candidates
        )
    }
    comparison = compare_phase2_run_diagnostics(baseline_diag, degraded_diag)

    assert comparison["baseline"]["survivor_count"] == 1
    assert comparison["candidate"]["survivor_count"] == 0
    assert comparison["delta"]["survivor_count"] == -1
    assert comparison["delta"]["zero_eval_rows"] == 1
    assert comparison["candidate"]["median_survivor_q_value"] == 1.0
