from __future__ import annotations

import asyncio

from project.live.runner import LiveEngineRunner
from project.live.venue_rules import (
    parse_binance_venue_rules,
    parse_bybit_venue_rules,
)


class _DummyDataManager:
    def __init__(self) -> None:
        self.kline_queue = asyncio.Queue()
        self.ticker_queue = asyncio.Queue()

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None


def test_parse_binance_exchange_info_to_venue_rules() -> None:
    rules = parse_binance_venue_rules(
        {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "orderTypes": ["LIMIT", "MARKET"],
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                        {"filterType": "LOT_SIZE", "minQty": "0.001", "stepSize": "0.001"},
                        {"filterType": "MIN_NOTIONAL", "notional": "100"},
                    ],
                }
            ]
        },
        ["BTCUSDT"],
    )

    rule = rules["BTCUSDT"]
    assert rule.tick_size == 0.10
    assert rule.step_size == 0.001
    assert rule.min_qty == 0.001
    assert rule.min_notional == 100.0
    assert rule.market_order_supported is True
    assert rule.limit_order_supported is True
    assert rule.is_actionable is True
    assert rule.source == "exchange:binance"


def test_parse_bybit_instruments_info_to_venue_rules() -> None:
    rules = parse_bybit_venue_rules(
        {
            "list": [
                {
                    "symbol": "BTCUSDT",
                    "status": "Trading",
                    "priceFilter": {"tickSize": "0.10"},
                    "lotSizeFilter": {
                        "qtyStep": "0.001",
                        "minOrderQty": "0.001",
                        "minNotionalValue": "5",
                    },
                }
            ]
        },
        ["BTCUSDT"],
    )

    rule = rules["BTCUSDT"]
    assert rule.tick_size == 0.10
    assert rule.step_size == 0.001
    assert rule.min_qty == 0.001
    assert rule.min_notional == 5.0
    assert rule.post_only_supported is True
    assert rule.is_actionable is True
    assert rule.source == "exchange:bybit"


def test_live_runner_hydrates_binance_venue_rules_from_exchange_metadata() -> None:
    class _DummyRestClient:
        async def get_exchange_info(self):
            return {
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "orderTypes": ["LIMIT", "MARKET"],
                        "filters": [
                            {"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                            {"filterType": "LOT_SIZE", "minQty": "0.001", "stepSize": "0.001"},
                            {"filterType": "MIN_NOTIONAL", "notional": "100"},
                        ],
                    }
                ]
            }

    runner = LiveEngineRunner(
        ["btcusdt"],
        data_manager=_DummyDataManager(),
        strategy_runtime={"implemented": True, "auto_submit": True},
    )
    runner.rest_client = _DummyRestClient()

    asyncio.run(runner._hydrate_venue_rules_once())

    rule = runner._venue_rules_by_symbol["BTCUSDT"]
    assert rule.source == "exchange:binance"
    assert rule.tick_size == 0.10
    assert rule.step_size == 0.001
    assert rule.min_qty == 0.001
    assert rule.min_notional == 100.0
    assert runner.session_metadata["venue_rules_hydrated"] is True


def test_live_runner_keeps_configured_venue_rules_when_hydration_fails() -> None:
    class _FailingRestClient:
        async def get_exchange_info(self):
            raise RuntimeError("metadata outage")

    runner = LiveEngineRunner(
        ["btcusdt"],
        data_manager=_DummyDataManager(),
        strategy_runtime={
            "implemented": True,
            "auto_submit": True,
            "venue_rules": {
                "BTCUSDT": {
                    "tick_size": 0.1,
                    "step_size": 0.001,
                    "min_qty": 0.001,
                    "min_notional": 50.0,
                }
            },
        },
    )
    runner.rest_client = _FailingRestClient()

    asyncio.run(runner._hydrate_venue_rules_once())

    rule = runner._venue_rules_by_symbol["BTCUSDT"]
    assert rule.source == "configured"
    assert rule.min_notional == 50.0
    assert runner.session_metadata["venue_rules_hydrated"] is False
