from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping


def _optional_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out) or out <= 0.0:
        return None
    return out


@dataclass(frozen=True)
class OrderBookView:
    symbol: str
    bid: float | None = None
    ask: float | None = None
    mid: float | None = None
    spread_bps: float | None = None
    top_of_book_depth_usd: float | None = None
    source: str = "market_state"

    @classmethod
    def from_market_state(
        cls,
        market_state: Mapping[str, Any] | None,
        *,
        symbol: str = "",
    ) -> "OrderBookView":
        state = dict(market_state or {})
        bid = _optional_float(
            state.get("bid")
            or state.get("best_bid")
            or state.get("bid_price")
            or state.get("best_bid_price")
        )
        ask = _optional_float(
            state.get("ask")
            or state.get("best_ask")
            or state.get("ask_price")
            or state.get("best_ask_price")
        )
        mid = _optional_float(state.get("mid") or state.get("mid_price") or state.get("last_price"))
        if mid is None and bid is not None and ask is not None:
            mid = (bid + ask) / 2.0
        spread_bps = _optional_float(state.get("spread_bps"))
        if spread_bps is None and bid is not None and ask is not None and mid:
            spread_bps = ((ask - bid) / mid) * 10_000.0
        depth = _optional_float(
            state.get("top_of_book_depth_usd")
            or state.get("depth_usd")
            or state.get("liquidity_available")
        )
        return cls(
            symbol=str(symbol or state.get("symbol", "")).upper(),
            bid=bid,
            ask=ask,
            mid=mid,
            spread_bps=spread_bps,
            top_of_book_depth_usd=depth,
            source=str(state.get("source", "market_state") or "market_state"),
        )

    def passive_limit_price(self, side: str) -> float | None:
        normalized = str(side).strip().lower()
        if normalized == "buy":
            return self.bid
        if normalized == "sell":
            return self.ask
        return None

    def aggressive_limit_price(self, side: str) -> float | None:
        normalized = str(side).strip().lower()
        if normalized == "buy":
            return self.ask
        if normalized == "sell":
            return self.bid
        return None
