import pandas as pd
import numpy as np
import pytest
from typing import Any, Dict

from project.research.promotion.core import evaluate_row


class MockContract:
    def __init__(self):
        self.min_net_expectancy_bps = 5.0
        self.max_fee_plus_slippage_bps = 50.0
        self.max_daily_turnover_multiple = 10.0
        self.require_retail_viability = True
        self.require_low_capital_contract = False


def test_hierarchical_defense_success():
    row = {
        "event_type": "VOL_SPIKE",
        "n_events": 100,
        "q_value": 0.01,
        "expectancy": 0.0020,  # 20 bps
        "std_return": 0.01,
        "gate_stability": True,
        "gate_delay_robustness": True,
        "gate_after_cost_positive": True,
        "gate_after_cost_stressed_positive": True,
        "gate_bridge_microstructure": True,
        "bridge_validation_after_cost_bps": 20.0,
        "baseline_expectancy_bps": 5.0,  # Candidate is 20 bps, beats 5*1.1
        "pass_shift_placebo": True,
        "pass_random_entry_placebo": True,
        "pass_direction_reversal_placebo": True,
        "event_is_descriptive": False,
        "event_is_trade_trigger": True,
        "gate_delayed_entry_stress": True,
        "val_t_stat": 2.5,
        "oos1_t_stat": 2.1,
        "test_t_stat": 2.3,
        "tob_coverage": 0.95,
        "turnover_proxy_mean": 1.0,
        "expectancy_bps_1m": 20.0,
        "expectancy_bps_15m": 20.0,
        "net_expectancy_bps": 20.0,
    }

    result = evaluate_row(
        row=row,
        hypothesis_index={},
        negative_control_summary={},
        max_q_value=0.05,
        min_events=20,
        min_stability_score=0.1,
        min_sign_consistency=0.5,
        min_cost_survival_ratio=0.5,
        max_negative_control_pass_rate=0.1,
        min_tob_coverage=0.8,
        require_hypothesis_audit=False,
        allow_missing_negative_controls=True,
        min_dsr=0.0,
    )
    print(f"DEBUG SUCCESS: result = {result}")
    assert result["promotion_decision"] == "promoted"
    assert result["gate_promo_baseline_beats_complexity"] is True
    assert result["gate_promo_placebo_controls"] is True


def test_hierarchical_defense_baseline_fail():
    row = {
        "event_type": "VOL_SPIKE",
        "n_events": 100,
        "q_value": 0.01,
        "expectancy": 0.0006,  # 6 bps
        "bridge_validation_after_cost_bps": 6.0,
        "baseline_expectancy_bps": 10.0,  # Candidate is 6 bps, fails to beat 10*1.1
        "pass_shift_placebo": True,
        "pass_random_entry_placebo": True,
        "pass_direction_reversal_placebo": True,
        "gate_after_cost_positive": True,
        "gate_after_cost_stressed_positive": True,
        "gate_bridge_microstructure": True,
        "gate_delayed_entry_stress": True,
        "tob_coverage": 0.95,
    }

    result = evaluate_row(
        row=row,
        hypothesis_index={},
        negative_control_summary={},
        max_q_value=0.05,
        min_events=20,
        min_stability_score=0.1,
        min_sign_consistency=0.5,
        min_cost_survival_ratio=0.5,
        max_negative_control_pass_rate=0.1,
        min_tob_coverage=0.8,
        require_hypothesis_audit=False,
        allow_missing_negative_controls=True,
    )

    print(f"DEBUG FAIL: result = {result}")
    assert result["promotion_decision"] == "rejected"
    assert "failed_baseline_comparison" in result["reject_reason"]


if __name__ == "__main__":
    test_hierarchical_defense_success()
    test_hierarchical_defense_baseline_fail()
    print("Hierarchical defense tests passed.")
