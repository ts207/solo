from __future__ import annotations

from project.live.contracts.trade_intent import TradeIntent
from project.live.order_planner import build_order_plan
from project.live.venue_rules import VenueSymbolRules


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


def test_order_plan_applies_venue_step_size_and_min_notional():
    intent = TradeIntent(
        action="trade_small",
        symbol="BTCUSDT",
        side="buy",
        size_fraction=1.0,
    )

    plan = build_order_plan(
        intent=intent,
        client_order_id="cid_2",
        market_state={"mid_price": 100.0},
        portfolio_state={"available_balance": 123.45},
        max_notional_fraction=0.10,
        venue_rules=VenueSymbolRules(
            symbol="BTCUSDT",
            tick_size=0.1,
            step_size=0.01,
            min_qty=0.01,
            min_notional=10.0,
        ),
    )

    assert plan.accepted is True
    assert plan.order is not None
    assert plan.order.quantity == 0.12
    assert plan.order.metadata["venue_rules"]["step_size"] == 0.01


def test_order_plan_rejects_below_venue_min_notional_after_rounding():
    intent = TradeIntent(
        action="trade_small",
        symbol="BTCUSDT",
        side="buy",
        size_fraction=1.0,
    )

    plan = build_order_plan(
        intent=intent,
        client_order_id="cid_3",
        market_state={"mid_price": 100.0},
        portfolio_state={"available_balance": 50.0},
        max_notional_fraction=0.10,
        venue_rules=VenueSymbolRules(
            symbol="BTCUSDT",
            tick_size=0.1,
            step_size=0.01,
            min_qty=0.01,
            min_notional=10.0,
        ),
    )

    assert plan.accepted is False
    assert plan.blocked_by == "venue_rules"
    assert "below_min_notional" in plan.plan["venue_rule_reasons"]


def test_order_plan_rejects_unsupported_post_only_flag():
    intent = TradeIntent(
        action="trade_small",
        symbol="BTCUSDT",
        side="buy",
        size_fraction=1.0,
        metadata={"post_only": True},
    )

    plan = build_order_plan(
        intent=intent,
        client_order_id="cid_4",
        market_state={"mid_price": 100.0},
        portfolio_state={"available_balance": 1000.0},
        max_notional_fraction=0.10,
        venue_rules=VenueSymbolRules(
            symbol="BTCUSDT",
            tick_size=0.1,
            step_size=0.01,
            min_qty=0.01,
            min_notional=10.0,
            post_only_supported=False,
        ),
    )

    assert plan.accepted is False
    assert plan.blocked_by == "venue_rules"
    assert "post_only_not_supported" in plan.plan["venue_rule_reasons"]
