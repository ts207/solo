from __future__ import annotations

from project.live.contracts.trade_intent import TradeIntent
from project.live.order_planner import build_order_plan


def test_order_plan_does_not_turn_negative_capacity_inputs_into_positive_quantity():
    intent = TradeIntent(
        action="trade_small",
        symbol="BTCUSDT",
        side="buy",
        size_fraction=1.0,
    )

    plan = build_order_plan(
        intent=intent,
        client_order_id="cid_1",
        market_state={"mid_price": 100.0},
        portfolio_state={"available_balance": -100.0},
        max_notional_fraction=-0.10,
    )

    assert plan.accepted is False
    assert plan.blocked_by == "zero_quantity"
