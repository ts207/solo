from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import pandas as pd


@dataclass
class KlineEvent:
    symbol: str
    timeframe: str
    timestamp: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    taker_base_volume: float
    is_final: bool


@dataclass
class BookTickerEvent:
    symbol: str
    timestamp: pd.Timestamp
    best_bid_price: float
    best_bid_qty: float
    best_ask_price: float
    best_ask_qty: float


@dataclass
class ForceOrderEvent:
    symbol: str
    timestamp: pd.Timestamp
    side: str          # BUY or SELL
    order_type: str    # LIMIT
    filled_qty: float
    price: float
    notional_usd: float  # filled_qty * price


def parse_bybit_liquidation_event(payload: Dict[str, Any]) -> Optional[ForceOrderEvent]:
    """Parse Bybit V5 liquidation stream message.

    Topic: liquidation.{SYMBOL}
    Payload: {"topic":"liquidation.BTCUSDT","ts":1672304486868,"type":"snapshot",
              "data":{"updatedTime":1672304486865,"symbol":"BTCUSDT",
                      "side":"Sell","size":"0.1","price":"16578.50"}}
    """
    topic = payload.get("topic", "")
    if not topic.startswith("liquidation"):
        return None
    data = payload.get("data", {})
    price = float(data.get("price", 0.0))
    filled_qty = float(data.get("size", 0.0))
    ts_ms = payload.get("ts", data.get("updatedTime", 0))
    return ForceOrderEvent(
        symbol=str(data.get("symbol", "")),
        timestamp=pd.to_datetime(int(ts_ms), unit="ms", utc=True),
        side=str(data.get("side", "")),
        order_type="LIQUIDATION",
        filled_qty=filled_qty,
        price=price,
        notional_usd=filled_qty * price,
    )


def parse_force_order_event(payload: Dict[str, Any]) -> Optional[ForceOrderEvent]:
    """Parse Binance forceOrder (liquidation) stream message."""
    data = payload.get("data", payload)
    if data.get("e") != "forceOrder":
        return None
    order = data.get("o", {})
    price = float(order.get("ap", order.get("p", 0.0)))  # average price or order price
    filled_qty = float(order.get("z", 0.0))              # cumulative filled qty
    return ForceOrderEvent(
        symbol=str(order.get("s", "")),
        timestamp=pd.to_datetime(data.get("E", data.get("T", 0)), unit="ms", utc=True),
        side=str(order.get("S", "")),
        order_type=str(order.get("o", "")),
        filled_qty=filled_qty,
        price=price,
        notional_usd=filled_qty * price,
    )


def parse_kline_event(payload: Dict[str, Any]) -> Optional[KlineEvent]:
    """Parse Binance kline stream message."""
    # Handle combined stream payload
    data = payload.get("data", payload)

    event_type = data.get("e")
    if event_type != "kline":
        return None

    kline = data.get("k", {})

    return KlineEvent(
        symbol=data.get("s", ""),
        timeframe=kline.get("i", ""),
        timestamp=pd.to_datetime(kline.get("t", 0), unit="ms", utc=True),
        open=float(kline.get("o", 0.0)),
        high=float(kline.get("h", 0.0)),
        low=float(kline.get("l", 0.0)),
        close=float(kline.get("c", 0.0)),
        volume=float(kline.get("v", 0.0)),
        quote_volume=float(kline.get("q", 0.0)),
        taker_base_volume=float(kline.get("V", 0.0)),
        is_final=bool(kline.get("x", False)),
    )


def parse_book_ticker_event(
    payload: Dict[str, Any], arrival_ts: Optional[pd.Timestamp] = None
) -> Optional[BookTickerEvent]:
    """Parse Binance bookTicker stream message."""
    data = payload.get("data", payload)

    # Single stream: {"u":..., "s":"BNBUSDT","b":"25.3519","B":"31.21","a":"25.3652","A":"40.66"}
    # Combined stream: {"stream":"...","data":{...}}
    if not ("s" in data and "b" in data and "a" in data):
        return None

    # E = Event time, T = Transaction time (if available in stream data)
    # If missing, use provided arrival_ts (socket arrival time)
    ts = data.get("E", data.get("T"))
    if ts:
        timestamp = pd.to_datetime(ts, unit="ms", utc=True)
    elif arrival_ts is not None:
        timestamp = arrival_ts
    else:
        timestamp = pd.Timestamp.now(tz="UTC")

    return BookTickerEvent(
        symbol=data.get("s", ""),
        timestamp=timestamp,
        best_bid_price=float(data.get("b", 0.0)),
        best_bid_qty=float(data.get("B", 0.0)),
        best_ask_price=float(data.get("a", 0.0)),
        best_ask_qty=float(data.get("A", 0.0)),
    )


def parse_bybit_kline_event(payload: Dict[str, Any]) -> Optional[KlineEvent]:
    """Parse Bybit V5 kline stream message."""
    topic = payload.get("topic", "")
    if not topic.startswith("kline"):
        return None

    data_list = payload.get("data", [])
    if not data_list:
        return None

    k = data_list[0]
    parts = topic.split(".")
    # topic format: kline.{interval}.{symbol}
    symbol = parts[-1] if len(parts) >= 3 else ""
    interval = parts[1] if len(parts) >= 2 else ""

    # Bybit interval '1' -> '1m', '5' -> '5m', etc.
    if interval.isdigit():
        interval = f"{interval}m"

    return KlineEvent(
        symbol=symbol,
        timeframe=interval,
        timestamp=pd.to_datetime(k.get("start", 0), unit="ms", utc=True),
        open=float(k.get("open", 0.0)),
        high=float(k.get("high", 0.0)),
        low=float(k.get("low", 0.0)),
        close=float(k.get("close", 0.0)),
        volume=float(k.get("volume", 0.0)),
        quote_volume=float(k.get("turnover", 0.0)),
        taker_base_volume=0.0,  # Not directly provided in basic kline
        is_final=bool(k.get("confirm", False)),
    )


def parse_bybit_ticker_event(
    payload: Dict[str, Any], arrival_ts: Optional[pd.Timestamp] = None
) -> Optional[BookTickerEvent]:
    """Parse Bybit V5 tickers stream message."""
    topic = payload.get("topic", "")
    if not topic.startswith("tickers"):
        return None

    data = payload.get("data", {})
    if not data:
        return None

    ts = payload.get("ts")
    if ts:
        timestamp = pd.to_datetime(ts, unit="ms", utc=True)
    elif arrival_ts is not None:
        timestamp = arrival_ts
    else:
        timestamp = pd.Timestamp.now(tz="UTC")

    return BookTickerEvent(
        symbol=data.get("symbol", ""),
        timestamp=timestamp,
        best_bid_price=float(data.get("bid1Price", 0.0)),
        best_bid_qty=float(data.get("bid1Size", 0.0)),
        best_ask_price=float(data.get("ask1Price", 0.0)),
        best_ask_qty=float(data.get("ask1Size", 0.0)),
    )
