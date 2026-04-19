from __future__ import annotations

from project.live.contracts.trade_intent import TradeIntent
from project.live.trade_valuator import value_trade_intent


def test_trade_valuator_net_edge_drops_below_zero_under_worse_costs() -> None:
    intent = TradeIntent(
        action="trade_small",
        symbol="BTCUSDT",
        side="buy",
        confidence_band="high",
        metadata={"expected_return_bps": 8.0, "expected_downside_bps": 6.0},
    )

    cheap = value_trade_intent(
        intent=intent,
        market_state={"expected_cost_bps": 2.0, "spread_bps": 1.0, "depth_usd": 100000.0},
    )
    expensive = value_trade_intent(
        intent=intent,
        market_state={"expected_cost_bps": 12.0, "spread_bps": 1.0, "depth_usd": 100000.0},
    )

    assert cheap.expected_net_edge_bps == 6.0
    assert cheap.should_trade is True
    assert expensive.expected_net_edge_bps == -4.0
    assert expensive.should_trade is False
    assert "expected_net_edge_non_positive" in expensive.reasons
