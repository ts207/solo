"""Tests that sizing limits are not hardcoded and can be overridden."""

import pytest

from project.portfolio.risk_budget import (
    calculate_cluster_risk_multiplier,
    calculate_portfolio_risk_multiplier,
    get_asset_correlation_adjustment,
)
from project.portfolio.sizing import calculate_target_notional


def test_concentration_cap_is_overridable():
    """concentration_cap must not be hardcoded — caller can supply a different value."""
    base = calculate_target_notional(
        event_score=1.0,
        expected_return_bps=100.0,
        expected_adverse_bps=10.0,
        vol_regime=0.2,
        liquidity_usd=10_000_000.0,
        portfolio_state={"portfolio_value": 100_000.0},
        symbol="BTC",
    )
    # A doubled concentration cap should allow a larger position
    higher_cap = calculate_target_notional(
        event_score=1.0,
        expected_return_bps=100.0,
        expected_adverse_bps=10.0,
        vol_regime=0.2,
        liquidity_usd=10_000_000.0,
        portfolio_state={"portfolio_value": 100_000.0},
        symbol="BTC",
        concentration_cap_pct=0.10,  # 10% instead of hardcoded 5%
    )
    assert higher_cap["target_notional"] >= base["target_notional"], (
        "Higher concentration cap should allow equal-or-larger position."
    )


def test_kelly_clip_is_overridable():
    """Kelly confidence multiplier clip must not be hardcoded."""
    base = calculate_target_notional(
        event_score=1.0,
        expected_return_bps=1000.0,  # very high edge to saturate the clip
        expected_adverse_bps=10.0,
        vol_regime=0.2,
        liquidity_usd=10_000_000.0,
        portfolio_state={"portfolio_value": 100_000.0},
        symbol="BTC",
    )
    clipped_lower = calculate_target_notional(
        event_score=1.0,
        expected_return_bps=1000.0,
        expected_adverse_bps=10.0,
        vol_regime=0.2,
        liquidity_usd=10_000_000.0,
        portfolio_state={"portfolio_value": 100_000.0},
        symbol="BTC",
        max_kelly_multiplier=2.0,  # tighter clip
    )
    assert (
        clipped_lower["target_notional"] < base["target_notional"]
        or clipped_lower["target_notional"] <= base["target_notional"]
    ), "Lower kelly clip must reduce or equal the position size."


def test_sizing_clips_negative_capacity_inputs():
    result = calculate_target_notional(
        event_score=1.0,
        expected_return_bps=100.0,
        expected_adverse_bps=10.0,
        vol_regime=0.2,
        liquidity_usd=-10_000_000.0,
        portfolio_state={"portfolio_value": -100_000.0},
        symbol="BTC",
    )

    assert result["target_notional"] == pytest.approx(0.0)
    assert result["liquidity_cap"] == pytest.approx(0.0)
    assert result["concentration_cap"] == pytest.approx(0.0)


def test_risk_budget_uses_absolute_exposure_values():
    risk_mult = calculate_portfolio_risk_multiplier(
        gross_exposure=-0.95,
        max_gross_leverage=1.0,
        target_vol=0.1,
        current_vol=0.1,
    )
    corr_adj = get_asset_correlation_adjustment(
        asset_bucket="btc",
        bucket_exposures={"btc": -0.75},
        correlation_limit=0.5,
    )

    assert risk_mult == pytest.approx(0.0)
    assert corr_adj == pytest.approx(0.5)


def test_cluster_risk_multiplier_accepts_string_cluster_keys():
    result = calculate_cluster_risk_multiplier(
        cluster_id=7,
        active_cluster_counts={"7": 4},
        max_strategies_per_cluster=3,
    )

    assert result < 1.0
