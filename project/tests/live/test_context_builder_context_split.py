from __future__ import annotations

from project.live.context_builder import build_live_trade_context
from project.live.event_detector import DetectedEvent


def test_live_trade_context_exposes_signal_and_execution_context() -> None:
    detected = DetectedEvent(
        event_id="VOL_SHOCK",
        event_family="VOL_SHOCK",
        canonical_regime="VOLATILITY",
        event_side="long",
        features={"move_bps": 42.0},
        event_confidence=0.8,
        event_severity=0.7,
    )
    ctx = build_live_trade_context(
        timestamp="2026-01-01T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        detected_event=detected,
        market_features={
            "spread_bps": 2.0,
            "depth_usd": 100000.0,
            "expected_cost_bps": 3.0,
            "is_execution_tradable": True,
            "ticker_fresh": True,
            "ms_vol_state": 2.0,
        },
        portfolio_state={},
        execution_env={},
    )
    assert ctx.signal_context["primary_event_id"] == "VOL_SHOCK"
    assert ctx.signal_context["ms_vol_state"] == 2.0
    assert ctx.execution_context["spread_bps"] == 2.0
    assert ctx.execution_context["is_execution_tradable"] is True
    assert ctx.market_state_quality["ticker_fresh"] is True
    assert "signal_context" in ctx.regime_snapshot
