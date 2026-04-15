from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pandas as pd

from project.live.ingest import manager as mgr


class DummyClient:
    def __init__(self, streams, callback, on_reconnect_exhausted=None):
        self.streams = streams
        self.callback = callback
        self.on_reconnect_exhausted = on_reconnect_exhausted
        self.connected = False
        self.disconnected = False

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.disconnected = True


def test_stream_and_health_keys(monkeypatch) -> None:
    monkeypatch.setattr(mgr, "BinanceWebSocketClient", DummyClient)
    ld = mgr.LiveDataManager(["BTCUSDT", "ETHUSDT"])
    assert ld.streams == [
        "btcusdt@kline_1m",
        "btcusdt@kline_5m",
        "btcusdt@bookTicker",
        "btcusdt@forceOrder",   # liquidation stream for LIQUIDATION_CASCADE detection
        "ethusdt@kline_1m",
        "ethusdt@kline_5m",
        "ethusdt@bookTicker",
        "ethusdt@forceOrder",
    ]
    assert ld.health_monitor_keys() == [
        ("BTCUSDT", "kline:1m"),
        ("BTCUSDT", "kline:5m"),
        ("BTCUSDT", "ticker"),
        ("ETHUSDT", "kline:1m"),
        ("ETHUSDT", "kline:5m"),
        ("ETHUSDT", "ticker"),
    ]



def test_backfill_drops_oldest_when_queue_is_full(monkeypatch) -> None:
    monkeypatch.setattr(mgr, "BinanceWebSocketClient", DummyClient)

    class RestClient:
        async def get_klines(self, symbol, timeframe, limit=100):
            return [
                [1, 10, 11, 9, 10.5, 100],
                [2, 20, 21, 19, 20.5, 200],
            ]

    ld = mgr.LiveDataManager(["BTCUSDT"], rest_client=RestClient())
    ld.kline_queue = asyncio.Queue(maxsize=1)
    asyncio.run(ld.backfill())
    assert ld.kline_queue.qsize() == 1
    item = ld.kline_queue.get_nowait()
    assert item["close"] == 20.5


def test_backfill_drop_preserves_queue_task_accounting(monkeypatch) -> None:
    monkeypatch.setattr(mgr, "BinanceWebSocketClient", DummyClient)

    class RestClient:
        async def get_klines(self, symbol, timeframe, limit=100):
            return [
                [1, 10, 11, 9, 10.5, 100],
                [2, 20, 21, 19, 20.5, 200],
            ]

    ld = mgr.LiveDataManager(["BTCUSDT"], rest_client=RestClient())
    ld.kline_queue = asyncio.Queue(maxsize=1)
    asyncio.run(ld.backfill())
    assert ld.kline_queue._unfinished_tasks == 1


def test_on_message_routes_kline_and_ticker_events(monkeypatch) -> None:
    monkeypatch.setattr(mgr, "BinanceWebSocketClient", DummyClient)
    monkeypatch.setattr(
        mgr,
        "parse_kline_event",
        lambda message: {"kind": "kline", "stream": message["stream"]},
    )
    monkeypatch.setattr(
        mgr,
        "parse_book_ticker_event",
        lambda message, arrival_ts: {"kind": "ticker", "arrival_ts": arrival_ts},
    )

    ld = mgr.LiveDataManager(["BTCUSDT"])
    ld._on_message({"stream": "btcusdt@kline_1m", "data": {}})
    ld._on_message({"stream": "btcusdt@bookTicker", "data": {"b": 1}})

    assert ld.kline_queue.qsize() == 1
    assert ld.ticker_queue.qsize() == 1
    assert ld.kline_queue.get_nowait()["kind"] == "kline"
    assert ld.ticker_queue.get_nowait()["kind"] == "ticker"
