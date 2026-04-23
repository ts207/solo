from __future__ import annotations

import asyncio

import pytest

from project.live.execution_router import ExecutionRouter, ExecutionRouterError
from project.live.oms import LiveOrder, OrderSide, OrderType
from project.live.venue_rules import VenueSymbolRules


class _RecordingExchange:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def create_market_order(self, **kwargs):
        self.calls.append(("market", kwargs))
        return {"orderId": "market-1"}

    async def create_limit_order(self, **kwargs):
        self.calls.append(("limit", kwargs))
        return {"orderId": "limit-1"}


def test_execution_router_selects_aggressive_market_route() -> None:
    exchange = _RecordingExchange()
    order = LiveOrder("cid_route_1", "BTCUSDT", OrderSide.BUY, OrderType.MARKET, 1.0)

    submission = asyncio.run(ExecutionRouter(exchange).submit(order))

    assert submission.venue_submitted is True
    assert submission.route.route_type == "market"
    assert submission.exchange_order_id == "market-1"
    assert exchange.calls == [
        (
            "market",
            {
                "symbol": "BTCUSDT",
                "side": "BUY",
                "quantity": 1.0,
                "reduce_only": False,
            },
        )
    ]


def test_execution_router_selects_passive_post_only_route_from_book() -> None:
    exchange = _RecordingExchange()
    order = LiveOrder(
        "cid_route_2",
        "BTCUSDT",
        OrderSide.BUY,
        OrderType.MARKET,
        0.12349,
        metadata={"post_only": True},
    )

    submission = asyncio.run(
        ExecutionRouter(exchange).submit(
            order,
            market_state={"bid": 100.12, "ask": 100.22},
            venue_rules=VenueSymbolRules(
                symbol="BTCUSDT",
                tick_size=0.1,
                step_size=0.01,
                min_qty=0.01,
                min_notional=10.0,
                post_only_supported=True,
            ),
        )
    )

    assert submission.route.route_type == "post_only"
    assert submission.route.is_passive is True
    assert order.order_type == OrderType.LIMIT
    assert order.quantity == 0.12
    assert order.price == 100.1
    assert exchange.calls == [
        (
            "limit",
            {
                "symbol": "BTCUSDT",
                "side": "BUY",
                "quantity": 0.12,
                "price": 100.1,
                "time_in_force": "GTX",
                "reduce_only": False,
                "post_only": True,
            },
        )
    ]


def test_execution_router_rejects_passive_route_without_price_or_book() -> None:
    order = LiveOrder(
        "cid_route_3",
        "BTCUSDT",
        OrderSide.SELL,
        OrderType.MARKET,
        1.0,
        metadata={"post_only": True},
    )

    with pytest.raises(ExecutionRouterError, match="requires a limit price or bid/ask"):
        asyncio.run(ExecutionRouter(_RecordingExchange()).submit(order))


def test_execution_router_routes_reduce_only_market_order() -> None:
    exchange = _RecordingExchange()
    order = LiveOrder(
        "cid_route_4",
        "ETHUSDT",
        OrderSide.SELL,
        OrderType.MARKET,
        0.5,
        metadata={"reduce_only": True},
    )

    submission = asyncio.run(ExecutionRouter(exchange).submit(order))

    assert submission.route.route_type == "reduce_only"
    assert submission.route.reduce_only is True
    assert exchange.calls[0][1]["reduce_only"] is True
