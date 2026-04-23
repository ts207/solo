from __future__ import annotations

from project.live.contracts.live_trade_context import LiveTradeContext


def test_live_trade_context_primary_event_id_does_not_backfill_event_family() -> None:
    context = LiveTradeContext(
        timestamp="2026-04-02T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_side="long",
    )

    assert context.primary_event_id == "VOL_SHOCK"
    assert context.event_family == ""


def test_live_trade_context_uses_event_family_as_compatibility_fallback_only() -> None:
    context = LiveTradeContext(
        timestamp="2026-04-02T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        event_family="VOL_SHOCK",
        event_side="long",
    )

    assert context.primary_event_id == "VOL_SHOCK"
    assert context.event_family == "VOL_SHOCK"
