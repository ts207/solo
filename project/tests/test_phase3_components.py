import pandas as pd
import pytest

from project.engine.execution_model import get_comprehensive_execution_estimate
from project.portfolio.sizing import (
    calculate_execution_aware_target_notional,
    calculate_target_notional,
)
from project.strategy.runtime.exits import check_exit_conditions


def test_execution_estimate():
    market_data = {"spread_bps": 2.0, "liquidity_available": 1000000.0, "vol_regime_bps": 10.0}
    est = get_comprehensive_execution_estimate(
        order_size=1000.0,
        base_price=100.0,
        is_buy=True,
        market_data=market_data,
        urgency="aggressive",
        profile="base",
    )
    assert est["fill_probability"] > 0.9
    assert est["expected_fill_price"] > 100.0
    assert est["expected_slippage_bps"] > 1.0


def test_execution_estimate_uses_absolute_order_size_for_cost_math():
    market_data = {"spread_bps": 2.0, "liquidity_available": 1000000.0, "vol_regime_bps": 10.0}
    long_est = get_comprehensive_execution_estimate(
        order_size=1000.0,
        base_price=100.0,
        is_buy=True,
        market_data=market_data,
        urgency="aggressive",
        profile="base",
    )
    short_est = get_comprehensive_execution_estimate(
        order_size=-1000.0,
        base_price=100.0,
        is_buy=False,
        market_data=market_data,
        urgency="aggressive",
        profile="base",
    )

    assert short_est["fill_probability"] == pytest.approx(long_est["fill_probability"])
    assert short_est["expected_slippage_bps"] == pytest.approx(long_est["expected_slippage_bps"])
    assert short_est["residual_unfilled_quantity"] >= 0.0


def test_sizing():
    portfolio_state = {
        "portfolio_value": 1000000.0,
        "gross_exposure": 0.0,
        "max_gross_leverage": 1.0,
        "target_vol": 0.1,
        "current_vol": 0.1,
        "bucket_exposures": {},
    }
    size = calculate_target_notional(
        event_score=2.0,
        expected_return_bps=50.0,
        expected_adverse_bps=10.0,
        vol_regime=0.1,
        liquidity_usd=1000000.0,
        portfolio_state=portfolio_state,
        symbol="BTCUSDT",
    )
    assert size["target_notional"] > 0
    assert size["confidence_multiplier"] > 0


def test_sizing_is_unit_invariant():
    portfolio_state = {
        "portfolio_value": 1000000.0,
        "gross_exposure": 0.0,
        "max_gross_leverage": 1.0,
        "target_vol": 0.1,
        "current_vol": 0.1,
        "bucket_exposures": {},
    }
    size_bps = calculate_target_notional(
        event_score=2.0,
        expected_return_bps=50.0,
        expected_adverse_bps=10.0,
        vol_regime=0.1,
        liquidity_usd=1000000.0,
        portfolio_state=portfolio_state,
        symbol="BTCUSDT",
    )
    size_decimals = calculate_target_notional(
        event_score=2.0,
        expected_return_bps=0.005,
        expected_adverse_bps=0.001,
        vol_regime=0.1,
        liquidity_usd=1000000.0,
        portfolio_state=portfolio_state,
        symbol="BTCUSDT",
    )
    assert size_bps["confidence_multiplier"] == pytest.approx(
        size_decimals["confidence_multiplier"]
    )
    assert size_bps["target_notional"] == pytest.approx(size_decimals["target_notional"])


def test_sizing_scales_down_in_high_volatility_regime():
    portfolio_state = {
        "portfolio_value": 1000000.0,
        "gross_exposure": 0.0,
        "max_gross_leverage": 1.0,
        "target_vol": 0.1,
        "current_vol": 0.1,
        "bucket_exposures": {},
    }
    calm = calculate_target_notional(
        event_score=2.0,
        expected_return_bps=50.0,
        expected_adverse_bps=10.0,
        vol_regime=0.1,
        liquidity_usd=1000000.0,
        portfolio_state=portfolio_state,
        symbol="BTCUSDT",
    )
    stressed = calculate_target_notional(
        event_score=2.0,
        expected_return_bps=50.0,
        expected_adverse_bps=10.0,
        vol_regime=0.25,
        liquidity_usd=1000000.0,
        portfolio_state=portfolio_state,
        symbol="BTCUSDT",
    )

    assert calm["volatility_adjustment"] == pytest.approx(1.0)
    assert stressed["volatility_adjustment"] == pytest.approx(0.4)
    assert stressed["target_notional"] == pytest.approx(calm["target_notional"] * 0.4)


def test_sizing_scales_down_when_execution_cost_eats_edge():
    portfolio_state = {
        "portfolio_value": 1000000.0,
        "gross_exposure": 0.0,
        "max_gross_leverage": 1.0,
        "target_vol": 0.1,
        "current_vol": 0.1,
        "bucket_exposures": {},
    }
    gross_edge = calculate_target_notional(
        event_score=0.008,
        expected_return_bps=20.0,
        expected_adverse_bps=20.0,
        expected_cost_bps=0.0,
        vol_regime=0.1,
        liquidity_usd=10000000.0,
        portfolio_state=portfolio_state,
        symbol="BTCUSDT",
    )
    cost_drag = calculate_target_notional(
        event_score=0.008,
        expected_return_bps=20.0,
        expected_adverse_bps=20.0,
        expected_cost_bps=10.0,
        vol_regime=0.1,
        liquidity_usd=10000000.0,
        portfolio_state=portfolio_state,
        symbol="BTCUSDT",
    )
    no_edge = calculate_target_notional(
        event_score=2.0,
        expected_return_bps=20.0,
        expected_adverse_bps=10.0,
        expected_cost_bps=25.0,
        vol_regime=0.1,
        liquidity_usd=1000000.0,
        portfolio_state=portfolio_state,
        symbol="BTCUSDT",
    )

    assert gross_edge["net_expected_return"] == pytest.approx(0.002)
    assert cost_drag["net_expected_return"] == pytest.approx(0.001)
    assert cost_drag["target_notional"] < gross_edge["target_notional"]
    assert cost_drag["target_notional"] == pytest.approx(gross_edge["target_notional"] * 0.5)
    assert no_edge["net_expected_return"] == pytest.approx(-0.0005)
    assert no_edge["confidence_multiplier"] == pytest.approx(0.0)
    assert no_edge["target_notional"] == pytest.approx(0.0)


def test_execution_aware_sizing_uses_dynamic_cost_model():
    portfolio_state = {
        "portfolio_value": 1000000.0,
        "gross_exposure": 0.0,
        "max_gross_leverage": 1.0,
        "target_vol": 0.1,
        "current_vol": 0.1,
        "bucket_exposures": {},
    }
    market_data = {
        "spread_bps": 4.0,
        "close": 100.0,
        "high": 100.2,
        "low": 99.8,
        "quote_volume": 250000.0,
        "depth_usd": 50000.0,
        "tob_coverage": 1.0,
    }
    gross_only = calculate_target_notional(
        event_score=0.008,
        expected_return_bps=20.0,
        expected_adverse_bps=20.0,
        vol_regime=0.1,
        liquidity_usd=5000000.0,
        portfolio_state=portfolio_state,
        symbol="BTCUSDT",
    )
    execution_aware = calculate_execution_aware_target_notional(
        event_score=0.008,
        expected_return_bps=20.0,
        expected_adverse_bps=20.0,
        vol_regime=0.1,
        liquidity_usd=5000000.0,
        portfolio_state=portfolio_state,
        symbol="BTCUSDT",
        market_data=market_data,
        execution_cost_config={
            "cost_model": "dynamic",
            "base_fee_bps": 2.0,
            "base_slippage_bps": 1.0,
            "spread_weight": 0.5,
            "volatility_weight": 0.0,
            "liquidity_weight": 0.0,
            "impact_weight": 1.0,
            "min_tob_coverage": 0.8,
        },
    )

    assert execution_aware["estimated_execution_cost_bps"] > 0.0
    assert execution_aware["provisional_target_notional"] == pytest.approx(
        gross_only["target_notional"]
    )
    assert execution_aware["target_notional"] < gross_only["target_notional"]
    assert execution_aware["net_expected_return"] < gross_only["net_expected_return"]


def test_adaptive_exits():
    bar = pd.Series({"close": 105.0, "atr": 2.0})
    blueprint_exit = {
        "time_stop_bars": 96,
        "target_value": 0.04,
        "target_type": "percent",
        "stop_value": 0.03,
        "stop_type": "percent",
    }
    # Long position at 100, target hit at 105 (4% = 104)
    exit_triggered, reason = check_exit_conditions(
        bar=bar,
        position_entry_price=100.0,
        is_long=True,
        blueprint_exit=blueprint_exit,
        bars_held=10,
    )
    assert exit_triggered
    assert reason == "target_hit"


if __name__ == "__main__":
    test_execution_estimate()
    test_sizing()
    test_adaptive_exits()
    print("All Phase 3 component tests passed.")
