from __future__ import annotations

import pytest

from project.live.oms import LiveOrder, OrderSide, OrderType
from project.live.order_normalizer import normalize_order_for_venue
from project.live.venue_rules import VenueSymbolRules


def test_order_normalizer_applies_tick_step_and_notional_filters() -> None:
    order = LiveOrder(
        "cid_norm_1",
        "BTCUSDT",
        OrderSide.BUY,
        OrderType.LIMIT,
        quantity=0.12349,
        price=100.19,
    )

    result = normalize_order_for_venue(
        order,
        venue_rules=VenueSymbolRules(
            symbol="BTCUSDT",
            tick_size=0.1,
            step_size=0.01,
            min_qty=0.01,
            min_notional=10.0,
        ),
    )

    assert result.accepted is True
    assert order.quantity == 0.12
    assert order.remaining_quantity == 0.12
    assert order.price == 100.1
    assert result.diagnostics["notional"] == pytest.approx(12.012)


def test_order_normalizer_rejects_missing_limit_price() -> None:
    order = LiveOrder("cid_norm_2", "BTCUSDT", OrderSide.BUY, OrderType.LIMIT, 1.0)

    result = normalize_order_for_venue(
        order,
        venue_rules=VenueSymbolRules(
            symbol="BTCUSDT",
            tick_size=0.1,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
        ),
    )

    assert result.accepted is False
    assert result.blocked_by == "venue_rules"
    assert result.reasons == ("missing_limit_price",)


def test_order_normalizer_rejects_unsupported_route_flags() -> None:
    order = LiveOrder(
        "cid_norm_3",
        "BTCUSDT",
        OrderSide.SELL,
        OrderType.LIMIT,
        quantity=1.0,
        price=100.0,
        metadata={"post_only": True, "reduce_only": True},
    )

    result = normalize_order_for_venue(
        order,
        venue_rules=VenueSymbolRules(
            symbol="BTCUSDT",
            tick_size=0.1,
            step_size=0.001,
            min_qty=0.001,
            min_notional=5.0,
            reduce_only_supported=False,
            post_only_supported=False,
        ),
    )

    assert result.accepted is False
    assert "reduce_only_not_supported" in result.reasons
    assert "post_only_not_supported" in result.reasons
