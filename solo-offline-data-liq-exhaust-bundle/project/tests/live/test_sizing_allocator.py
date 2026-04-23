from __future__ import annotations

from project.live.sizing_allocator import allocate_trade_size, compute_marginal_overlap
from project.live.trade_valuator import TradeValuation


def _valuation(*, downside: float = 10.0) -> TradeValuation:
    return TradeValuation(
        expected_gross_edge_bps=20.0,
        expected_cost_bps=2.0,
        expected_net_edge_bps=18.0,
        expected_downside_bps=downside,
        fill_probability=0.9,
        win_probability=0.65,
        edge_confidence=0.9,
        utility_score=12.0,
        should_trade=True,
    )


def test_sizing_allocator_shrinks_under_slippage_overlap_and_downside() -> None:
    base = allocate_trade_size(
        valuation=_valuation(),
        market_state={"depth_usd": 1_000_000.0, "expected_cost_bps": 1.0},
        portfolio_state={"available_balance": 10_000.0},
        base_size_fraction=1.0,
        max_notional_fraction=0.10,
    )
    stressed = allocate_trade_size(
        valuation=_valuation(downside=60.0),
        market_state={"depth_usd": 1_000_000.0, "expected_cost_bps": 15.0},
        portfolio_state={"available_balance": 10_000.0, "marginal_overlap": 0.8},
        base_size_fraction=1.0,
        max_notional_fraction=0.10,
    )

    assert base.accepted is True
    assert stressed.accepted is True
    assert stressed.notional < base.notional
    assert stressed.size_fraction < base.size_fraction


def test_compute_marginal_overlap_same_symbol_and_family() -> None:
    portfolio_state = {
        "gross_exposure": 5_000.0,
        "symbol_exposures": {"BTCUSDT": 5_000.0},
        "family_exposures": {"LIQUIDATION_CASCADE": 5_000.0},
    }
    overlap = compute_marginal_overlap(
        symbol="BTCUSDT",
        event_family="LIQUIDATION_CASCADE",
        portfolio_state=portfolio_state,
    )
    assert overlap == 1.0


def test_compute_marginal_overlap_same_symbol_different_family() -> None:
    portfolio_state = {
        "gross_exposure": 5_000.0,
        "symbol_exposures": {"BTCUSDT": 5_000.0},
        "family_exposures": {"LIQUIDATION_CASCADE": 5_000.0},
    }
    overlap = compute_marginal_overlap(
        symbol="BTCUSDT",
        event_family="VOL_SPIKE",
        portfolio_state=portfolio_state,
    )
    # same_symbol_only = 5000, family_only = 0 → 0.60 * 5000 / 5000
    assert abs(overlap - 0.60) < 1e-9


def test_compute_marginal_overlap_no_existing_positions() -> None:
    overlap = compute_marginal_overlap(
        symbol="BTCUSDT",
        event_family="VOL_SPIKE",
        portfolio_state={"gross_exposure": 0.0, "symbol_exposures": {}, "family_exposures": {}},
    )
    assert overlap == 0.0


def test_allocate_trade_size_auto_computes_overlap_from_symbol() -> None:
    portfolio_state_flat = {
        "available_balance": 10_000.0,
        "gross_exposure": 0.0,
        "symbol_exposures": {},
        "family_exposures": {},
    }
    portfolio_state_loaded = {
        "available_balance": 10_000.0,
        "gross_exposure": 5_000.0,
        "symbol_exposures": {"BTCUSDT": 5_000.0},
        "family_exposures": {"LIQUIDATION_CASCADE": 5_000.0},
    }
    flat = allocate_trade_size(
        valuation=_valuation(),
        market_state={"depth_usd": 1_000_000.0},
        portfolio_state=portfolio_state_flat,
        base_size_fraction=1.0,
        max_notional_fraction=0.10,
        symbol="BTCUSDT",
        event_family="LIQUIDATION_CASCADE",
    )
    loaded = allocate_trade_size(
        valuation=_valuation(),
        market_state={"depth_usd": 1_000_000.0},
        portfolio_state=portfolio_state_loaded,
        base_size_fraction=1.0,
        max_notional_fraction=0.10,
        symbol="BTCUSDT",
        event_family="LIQUIDATION_CASCADE",
    )
    # Fully loaded portfolio (overlap=1.0) should produce minimum size
    assert flat.accepted is True
    assert loaded.accepted is True
    assert loaded.size_fraction < flat.size_fraction
    # overlap=1.0 → overlap_scale=0.20, so loaded should be roughly 20% of flat
    assert loaded.size_fraction <= flat.size_fraction * 0.25
