from __future__ import annotations

import pandas as pd

from project.research.bridge_evaluate_phase2 import (
    _build_policy_variant_specs,
    _evaluate_policy_variants_for_candidate,
    _policy_variant_flip_summary,
)


def test_build_policy_variant_specs_includes_delay_and_cooldown_variants():
    variants = _build_policy_variant_specs(
        low_capital_contract={
            "entry_delay_bars_default": 1,
            "entry_delay_bars_stress": 2,
        },
        cooldown_bars=[12],
        include_one_trade_per_episode=True,
    )
    ids = {str(v.get("variant_id", "")) for v in variants}
    assert "delay_1" in ids
    assert "delay_2" in ids
    assert "delay_1__one_trade_per_episode" in ids
    assert "delay_1__cooldown_12" in ids
    assert "delay_1__one_trade_per_episode__cooldown_12" in ids


def test_policy_variants_report_pass_to_fail_flip_under_cooldown():
    row = pd.Series(
        {
            "candidate_id": "cand_1",
            "symbol": "BTCUSDT",
            "horizon_bars": 3,
            "horizon": "15m",
            "turnover_proxy_mean": 1.0,
            "effective_lag_bars": 1,
            "tob_coverage": 1.0,
        }
    )
    bridge_result = {
        "candidate_id": "cand_1",
        "symbol": "BTCUSDT",
        "bridge_validation_trades": 40,
        "bridge_validation_after_cost_bps": 8.0,
        "bridge_effective_cost_bps_per_trade": 2.0,
        "bridge_effective_lag_bars_used": 1,
    }
    variants = _build_policy_variant_specs(
        low_capital_contract={
            "entry_delay_bars_default": 1,
            "entry_delay_bars_stress": 2,
        },
        cooldown_bars=[12],
        include_one_trade_per_episode=True,
    )
    rows = _evaluate_policy_variants_for_candidate(
        row=row,
        bridge_result=bridge_result,
        policy_variants=variants,
        min_validation_trades=20,
        stressed_cost_multiplier=1.5,
        edge_cost_k=2.0,
        require_retail_viability=False,
        enforce_low_capital_viability=False,
        low_capital_contract={},
        min_net_expectancy_bps=0.0,
        max_fee_plus_slippage_bps=None,
        max_daily_turnover_multiple=None,
    )
    df = pd.DataFrame(rows)
    summary = _policy_variant_flip_summary(df)
    assert summary["baseline_pass_policy_count"] == 1
    assert summary["pass_to_fail_policy_count"] == 1
