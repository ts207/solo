from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC
from typing import Any

import pandas as pd

from project.live.ingest.bybit_ws_client import BybitWebSocketClient
from project.live.ingest.parsers import (
    KlineEvent,
    parse_book_ticker_event,
    parse_bybit_kline_event,
    parse_bybit_liquidation_event,
    parse_bybit_ticker_event,
    parse_force_order_event,
    parse_kline_event,
)
from project.live.ingest.ws_client import BinanceWebSocketClient

_LOG = logging.getLogger(__name__)


class LiveDataManager:
    def __init__(
        self,
        symbols: list[str],
        exchange: str = "binance",
        on_reconnect_exhausted: Callable[[], None] | None = None,
        rest_client: Any | None = None,
    ):
        self.symbols = [s.lower() for s in symbols]
        self.exchange = exchange.lower()
        self.kline_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self.ticker_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self._loop: asyncio.AbstractEventLoop | None = None
        # Rolling liquidation notional per symbol (reset on each bar by consumer)
        self._liquidation_notional_by_symbol: dict[str, float] = {}
        self._latest_ticker_by_symbol: dict[str, dict[str, Any]] = {}
        self.streams = self._build_streams()

        if self.exchange == "binance":
            self.client = BinanceWebSocketClient(
                self.streams,
                self._on_message,
                on_reconnect_exhausted=on_reconnect_exhausted,
            )
        elif self.exchange == "bybit":
            self.client = BybitWebSocketClient(
                self.streams,
                self._on_message,
                on_reconnect_exhausted=on_reconnect_exhausted,
            )
        else:
            raise ValueError(f"Unsupported exchange: {self.exchange}")

        self.rest_client = rest_client

    def _build_streams(self) -> list[str]:
        streams = []
        for symbol in self.symbols:
            if self.exchange == "binance":
                streams.append(f"{symbol}@kline_1m")
                streams.append(f"{symbol}@kline_5m")
                streams.append(f"{symbol}@bookTicker")
                streams.append(f"{symbol}@forceOrder")
            elif self.exchange == "bybit":
                # Bybit V5 topics
                symbol_upper = symbol.upper()
                streams.append(f"kline.1.{symbol_upper}")
                streams.append(f"kline.5.{symbol_upper}")
                streams.append(f"tickers.{symbol_upper}")
        return streams

    def health_monitor_keys(self) -> list[tuple[str, str]]:
        keys: list[tuple[str, str]] = []
        for stream in self.streams:
            if self.exchange == "binance":
                if "@" not in stream:
                    continue
                symbol, channel = stream.split("@", 1)
                if channel == "bookTicker":
                    keys.append((symbol.upper(), "ticker"))
                elif channel.startswith("kline_"):
                    timeframe = channel.split("_", 1)[1]
                    keys.append((symbol.upper(), f"kline:{timeframe}"))
            elif self.exchange == "bybit":
                parts = stream.split(".")
                if parts[0] == "kline":
                    symbol = parts[-1]
                    timeframe = f"{parts[1]}m"
                    keys.append((symbol, f"kline:{timeframe}"))
                elif parts[0] == "tickers":
                    symbol = parts[-1]
                    keys.append((symbol, "ticker"))
        return keys

    async def start(self):
        _LOG.info(f"Starting Live Data Manager for {self.exchange}...")
        self._loop = asyncio.get_running_loop()

        if self.rest_client:
            await self.backfill()

        await self.client.connect()

    async def backfill(self):
        """Perform a conservative REST backfill for all symbols/timeframes."""
        if not self.rest_client:
            return

        _LOG.info(f"Performing initial REST backfill for {self.exchange}...")
        for symbol in self.symbols:
            try:
                for timeframe, limit in [("1m", 100), ("5m", 60)]:
                    await self._backfill_timeframe(symbol, timeframe, limit)
            except Exception as e:
                _LOG.error(f"Failed to backfill {symbol} on {self.exchange}: {e}")

    async def _backfill_timeframe(self, symbol: str, timeframe: str, limit: int) -> None:
        # Binance: [open_ts, o, h, l, c, v, ...]
        # Bybit:   [open_ts, o, h, l, c, v, turnover] (ts as string)
        klines = await self.rest_client.get_klines(symbol, timeframe, limit=limit)
        for k in klines:
            ts = pd.to_datetime(int(k[0]), unit="ms", utc=True)
            event = KlineEvent(
                symbol=symbol.upper(),
                timeframe=timeframe,
                open=float(k[1]),
                high=float(k[2]),
                low=float(k[3]),
                close=float(k[4]),
                volume=float(k[5]),
                quote_volume=0.0,
                taker_base_volume=0.0,
                is_final=True,
                timestamp=ts,
            )
            self._enqueue_threadsafe(self.kline_queue, event, "Kline")

    async def stop(self):
        _LOG.info(f"Stopping Live Data Manager for {self.exchange}...")
        await self.client.disconnect()

    @staticmethod
    def _drop_oldest_event(queue: asyncio.Queue) -> None:
        queue.get_nowait()
        queue.task_done()

    def _enqueue_threadsafe(self, queue: asyncio.Queue, event: Any, label: str) -> None:
        loop = self._loop
        if loop is None:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                try:
                    self._drop_oldest_event(queue)
                    queue.put_nowait(event)
                except (asyncio.QueueEmpty, asyncio.QueueFull):
                    pass
            return

        def _push() -> None:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                _LOG.warning("%s queue full, dropping oldest event", label)
                try:
                    self._drop_oldest_event(queue)
                    queue.put_nowait(event)
                except (asyncio.QueueEmpty, asyncio.QueueFull):
                    pass

        if loop.is_running():
            loop.call_soon_threadsafe(_push)
        else:
            _push()

    def _record_latest_ticker(self, event: Any) -> None:
        symbol = str(getattr(event, "symbol", "")).upper()
        if not symbol:
            return
        self._latest_ticker_by_symbol[symbol] = {
            "best_bid_price": float(event.best_bid_price),
            "best_bid_qty": float(event.best_bid_qty),
            "best_ask_price": float(event.best_ask_price),
            "best_ask_qty": float(event.best_ask_qty),
            "timestamp": event.timestamp.isoformat()
            if hasattr(event.timestamp, "isoformat")
            else str(event.timestamp),
        }

    def _on_message(self, message: dict[str, Any]):
        arrival_ts = pd.Timestamp.now(UTC)

        if self.exchange == "binance":
            stream_name = message.get("stream", "")
            if "kline" in stream_name:
                event = parse_kline_event(message)
                if event:
                    self._enqueue_threadsafe(self.kline_queue, event, "Kline")
            elif "forceOrder" in stream_name:
                event = parse_force_order_event(message)
                if event:
                    sym = str(event.symbol).upper()
                    self._liquidation_notional_by_symbol[sym] = (
                        self._liquidation_notional_by_symbol.get(sym, 0.0) + event.notional_usd
                    )
                    _LOG.debug("Liquidation event %s: +%.0f USD notional", sym, event.notional_usd)
            elif "bookTicker" in stream_name or "b" in message.get("data", message):
                event = parse_book_ticker_event(message, arrival_ts)
                if event:
                    self._record_latest_ticker(event)
                    self._enqueue_threadsafe(self.ticker_queue, event, "Ticker")
        elif self.exchange == "bybit":
            topic = message.get("topic", "")
            if topic.startswith("kline"):
                event = parse_bybit_kline_event(message)
                if event:
                    self._enqueue_threadsafe(self.kline_queue, event, "Kline")
            elif topic.startswith("tickers"):
                event = parse_bybit_ticker_event(message, arrival_ts)
                if event:
                    self._record_latest_ticker(event)
                    self._enqueue_threadsafe(self.ticker_queue, event, "Ticker")
            elif topic.startswith("liquidation"):
                event = parse_bybit_liquidation_event(message)
                if event:
                    sym = str(event.symbol).upper()
                    self._liquidation_notional_by_symbol[sym] = (
                        self._liquidation_notional_by_symbol.get(sym, 0.0) + event.notional_usd
                    )
                    _LOG.debug("Bybit liquidation %s: +%.0f USD notional", sym, event.notional_usd)

    def get_liquidation_notional(self, symbol: str) -> float:
        """Return cumulative liquidation notional (USD) since last reset for this symbol."""
        return self._liquidation_notional_by_symbol.get(str(symbol).upper(), 0.0)

    def reset_liquidation_notional(self, symbol: str) -> None:
        """Reset the liquidation notional accumulator for a symbol (call once per bar)."""
        self._liquidation_notional_by_symbol.pop(str(symbol).upper(), None)

    def latest_ticker(self, symbol: str) -> dict[str, Any]:
        return dict(self._latest_ticker_by_symbol.get(str(symbol).upper(), {}))
