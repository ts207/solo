from __future__ import annotations

from project.live.market_state_builder import (
    MarketStateBuilderConfig,
    build_measured_market_state,
)


def test_incomplete_liquidity_state_blocks_trading_without_config_fallbacks() -> None:
    state = build_measured_market_state(
        symbol="BTCUSDT",
        timeframe="5m",
        close=100.0,
        timestamp="2026-04-01T00:00:01+00:00",
        move_bps=50.0,
        ticker={
            "best_bid_price": 99.99,
            "best_ask_price": 100.01,
            "timestamp": "2026-04-01T00:00:00+00:00",
        },
        runtime_features={},
        supported_event_ids=["VOL_SHOCK"],
        config=MarketStateBuilderConfig(min_depth_usd=25_000.0),
    )

    assert state["market_state_complete"] is False
    assert state["is_execution_tradable"] is False
    assert "missing_book_quantities" in state["non_tradable_reasons"]
    assert state["depth_usd"] is None
    assert state["tob_coverage"] is None
    assert state["expected_cost_bps"] is None


def test_missing_required_open_interest_blocks_oi_strategy() -> None:
    state = build_measured_market_state(
        symbol="BTCUSDT",
        timeframe="5m",
        close=100.0,
        timestamp="2026-04-01T00:00:01+00:00",
        move_bps=-50.0,
        ticker={
            "best_bid_price": 99.99,
            "best_bid_qty": 1000.0,
            "best_ask_price": 100.01,
            "best_ask_qty": 1000.0,
            "timestamp": "2026-04-01T00:00:00+00:00",
        },
        runtime_features={},
        supported_event_ids=["OI_SPIKE_NEGATIVE"],
        config=MarketStateBuilderConfig(),
    )

    assert state["market_state_complete"] is False
    assert state["is_execution_tradable"] is False
    assert "missing_open_interest_state" in state["non_tradable_reasons"]
