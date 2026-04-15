from __future__ import annotations

from project.live.context_builder import build_live_trade_context
from project.live.event_detector import DetectedEvent


def test_context_builder_keeps_event_ids_primary_and_families_compat_only() -> None:
    context = build_live_trade_context(
        timestamp="2026-04-02T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        detected_event=DetectedEvent(
            event_id="VOL_SHOCK",
            event_family="VOL_SHOCK",
            canonical_regime="VOLATILITY",
            event_side="long",
            features={"move_bps": 95.0},
        ),
        market_features={
            "active_event_ids": ["VOL_SHOCK", "LIQUIDITY_VACUUM"],
            "active_event_families": ["LEGACY_VOL_GROUP"],
            "contradiction_event_ids": ["MEAN_REVERSION_SIGNAL"],
        },
        portfolio_state={},
        execution_env={},
    )

    assert context.primary_event_id == "VOL_SHOCK"
    assert context.active_event_ids == ["VOL_SHOCK", "LIQUIDITY_VACUUM"]
    assert context.active_event_families == ["LEGACY_VOL_GROUP"]
    assert context.contradiction_event_ids == ["MEAN_REVERSION_SIGNAL"]
    assert context.contradiction_event_families == []
