from __future__ import annotations

import pytest

from project.live.market_state_builder import (
    MarketStateBuilderConfig,
    build_measured_market_state,
)


def test_market_state_builder_extracts_measured_spread_depth_and_cost() -> None:
    state = build_measured_market_state(
        symbol="BTCUSDT",
        timeframe="5m",
        close=100.0,
        timestamp="2026-04-01T00:00:01+00:00",
        move_bps=100.0,
        ticker={
            "best_bid_price": 99.99,
            "best_bid_qty": 1000.0,
            "best_ask_price": 100.01,
            "best_ask_qty": 1200.0,
            "timestamp": "2026-04-01T00:00:00+00:00",
        },
        runtime_features={},
        supported_event_ids=["VOL_SHOCK"],
        config=MarketStateBuilderConfig(min_depth_usd=25_000.0, taker_fee_bps=2.5),
    )

    assert state["market_state_complete"] is True
    assert state["is_execution_tradable"] is True
    assert state["mid_price"] == pytest.approx(100.0)
    assert state["spread_bps"] == pytest.approx(2.0)
    assert state["spread_bps_source"] == "book_ticker"
    assert state["depth_usd"] == pytest.approx(99_990.0)
    assert state["depth_usd_source"] == "book_ticker_tob"
    assert state["tob_coverage"] == pytest.approx(1.0)
    assert state["expected_cost_bps"] == pytest.approx(3.5)
    assert state["expected_cost_bps_source"] == "spread_derived"


def test_market_state_builder_requires_fresh_open_interest_for_oi_strategy() -> None:
    state = build_measured_market_state(
        symbol="BTCUSDT",
        timeframe="5m",
        close=100.0,
        timestamp="2026-04-01T00:02:00+00:00",
        move_bps=-100.0,
        ticker={
            "best_bid_price": 99.99,
            "best_bid_qty": 1000.0,
            "best_ask_price": 100.01,
            "best_ask_qty": 1000.0,
            "timestamp": "2026-04-01T00:01:59+00:00",
        },
        runtime_features={
            "open_interest": 1000.0,
            "open_interest_timestamp": "2026-04-01T00:00:00+00:00",
        },
        supported_event_ids=["OI_SPIKE_NEGATIVE"],
        config=MarketStateBuilderConfig(runtime_feature_stale_after_seconds=30.0),
    )

    assert state["market_state_complete"] is False
    assert state["is_execution_tradable"] is False
    assert state["open_interest_fresh"] is False
    assert "stale_open_interest_state" in state["non_tradable_reasons"]


def test_market_state_builder_uses_refresh_time_for_runtime_feature_freshness() -> None:
    state = build_measured_market_state(
        symbol="BTCUSDT",
        timeframe="5m",
        close=100.0,
        timestamp="2026-04-01T00:00:10+00:00",
        move_bps=-100.0,
        ticker={
            "best_bid_price": 99.99,
            "best_bid_qty": 1000.0,
            "best_ask_price": 100.01,
            "best_ask_qty": 1000.0,
            "timestamp": "2026-04-01T00:00:09+00:00",
        },
        runtime_features={
            "open_interest": 1000.0,
            "refreshed_at": "2026-04-01T00:00:00+00:00",
        },
        supported_event_ids=["OI_SPIKE_NEGATIVE"],
        config=MarketStateBuilderConfig(runtime_feature_stale_after_seconds=30.0),
    )

    assert state["market_state_complete"] is True
    assert state["open_interest_fresh"] is True
    assert state["open_interest_timestamp"] == "2026-04-01T00:00:00+00:00"
