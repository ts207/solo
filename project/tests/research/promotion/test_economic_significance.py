"""
E5-T3: Economic significance filters.

Candidates that are statistically significant but have trivial economic impact
should be flagged or filtered during promotion.
"""

from __future__ import annotations

import pytest

from project.research.promotion.core import evaluate_row


@pytest.fixture
def base_row():
    return {
        "candidate_id": "c1",
        "event_type": "E1",
        "q_value": 0.01,
        "n_events": 100,
        "effect_raw": 0.0010,  # 10 bps
        "bridge_validation_after_cost_bps": 5.0,  # 5 bps net
        "avg_dynamic_cost_bps": 2.0,
        "turnover_proxy_mean": 1.0,
        "tob_coverage": 0.9,
        "gate_stability": True,
        "gate_delay_robustness": True,
        "gate_bridge_microstructure": True,
        "gate_after_cost_stressed_positive": True,
        "gate_delayed_entry_stress": True,
        "pass_shift_placebo": True,
        "pass_random_entry_placebo": True,
        "pass_direction_reversal_placebo": True,
        "baseline_expectancy_bps": 0.1,  # non-zero
        "net_expectancy_bps": 5.0,
        "sharpe_ratio": 1.0,
        "sign_consistency": 1.0,
        "effect_shrunk_state": 0.001,
        "val_t_stat": 2.5,
        "oos1_t_stat": 2.5,
        "std_return": 0.0001,  # low vol -> high stability score
        "pass_consensus_5m_1m": True,
        "pass_consensus_5m_15m": True,
        "delay_expectancy_map": {"1": 0.0005},
        "event_is_descriptive": False,
        "event_is_trade_trigger": True,
    }


@pytest.fixture
def mock_contract():
    class MockContract:
        min_net_expectancy_bps = 2.0
        max_fee_plus_slippage_bps = 10.0
        max_daily_turnover_multiple = 5.0
        require_retail_viability = True
        require_low_capital_contract = False

    return MockContract()


def test_trivial_expectancy_rejected(base_row, mock_contract):
    """Candidate with expectancy below min_net_expectancy_bps must be rejected."""
    row = base_row.copy()
    row["bridge_validation_after_cost_bps"] = 0.5  # 0.5 bps < 2.0 bps threshold

    result = evaluate_row(
        row=row,
        hypothesis_index={},
        negative_control_summary={},
        max_q_value=0.05,
        min_events=10,
        min_stability_score=0.1,
        min_sign_consistency=0.5,
        min_cost_survival_ratio=0.5,
        max_negative_control_pass_rate=0.5,
        min_tob_coverage=0.5,
        require_hypothesis_audit=False,
        allow_missing_negative_controls=True,
        min_net_expectancy_bps=mock_contract.min_net_expectancy_bps,
        require_retail_viability=True,
    )

    assert result["promotion_decision"] == "rejected"
    assert "retail_net_expectancy" in result["reject_reason"]


def test_excessive_turnover_rejected(base_row, mock_contract):
    """Candidate with turnover above max_daily_turnover_multiple must be rejected."""
    row = base_row.copy()
    row["turnover_proxy_mean"] = 10.0  # 10.0 > 5.0 threshold

    result = evaluate_row(
        row=row,
        hypothesis_index={},
        negative_control_summary={},
        max_q_value=0.05,
        min_events=10,
        min_stability_score=0.1,
        min_sign_consistency=0.5,
        min_cost_survival_ratio=0.5,
        max_negative_control_pass_rate=0.5,
        min_tob_coverage=0.5,
        require_hypothesis_audit=False,
        allow_missing_negative_controls=True,
        max_daily_turnover_multiple=mock_contract.max_daily_turnover_multiple,
        require_retail_viability=True,
    )

    assert result["promotion_decision"] == "rejected"
    assert "retail_turnover" in result["reject_reason"]


def test_flag_trivial_but_not_rejected_if_policy_allows(base_row, mock_contract):
    """
    If require_retail_viability is False, the candidate should be promoted
    but the trivial metrics should still be visible in the result.
    """
    row = base_row.copy()
    row["bridge_validation_after_cost_bps"] = 0.5

    result = evaluate_row(
        row=row,
        hypothesis_index={},
        negative_control_summary={},
        max_q_value=0.05,
        min_events=10,
        min_stability_score=0.1,
        min_sign_consistency=0.5,
        min_cost_survival_ratio=0.5,
        max_negative_control_pass_rate=0.5,
        min_tob_coverage=0.5,
        require_hypothesis_audit=False,
        allow_missing_negative_controls=True,
        min_net_expectancy_bps=mock_contract.min_net_expectancy_bps,
        require_retail_viability=False,  # Don't reject
    )

    assert result["promotion_decision"] == "promoted"
    assert result["gate_promo_retail_net_expectancy"] == False
    assert result["net_expectancy_bps"] == 0.5
