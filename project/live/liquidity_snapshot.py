from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from typing import Any, Mapping


def finite_float(raw: Any) -> float | None:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not isfinite(value):
        return None
    return value


def positive_float(raw: Any) -> float | None:
    value = finite_float(raw)
    if value is None or value <= 0.0:
        return None
    return value


def parse_timestamp(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        parsed = raw
    elif hasattr(raw, "to_pydatetime"):
        try:
            parsed = raw.to_pydatetime()
        except Exception:
            return None
    else:
        token = str(raw or "").strip()
        if not token:
            return None
        if token.endswith("Z"):
            token = f"{token[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(token)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def age_seconds(observed: datetime | None, reference: datetime | None) -> float | None:
    if observed is None or reference is None:
        return None
    return max(0.0, (reference - observed).total_seconds())


@dataclass(frozen=True)
class LiquiditySnapshot:
    symbol: str
    best_bid_price: float | None
    best_bid_qty: float | None
    best_ask_price: float | None
    best_ask_qty: float | None
    timestamp: datetime | None
    reference_timestamp: datetime | None
    spread_bps: float | None
    top_of_book_depth_usd: float | None
    tob_coverage: float | None
    age_seconds: float | None
    reasons: tuple[str, ...]

    @property
    def mid_price(self) -> float | None:
        if self.best_bid_price is None or self.best_ask_price is None:
            return None
        return (self.best_bid_price + self.best_ask_price) / 2.0

    @property
    def is_complete(self) -> bool:
        return not self.reasons

    def to_market_fields(self) -> dict[str, Any]:
        return {
            "best_bid_price": self.best_bid_price,
            "best_ask_price": self.best_ask_price,
            "best_bid_qty": self.best_bid_qty,
            "best_ask_qty": self.best_ask_qty,
            "mid_price": self.mid_price,
            "spread_bps": self.spread_bps,
            "spread_bps_source": "book_ticker" if self.spread_bps is not None else "missing",
            "depth_usd": self.top_of_book_depth_usd,
            "depth_usd_source": (
                "book_ticker_tob" if self.top_of_book_depth_usd is not None else "missing"
            ),
            "tob_coverage": self.tob_coverage,
            "tob_coverage_source": (
                "book_ticker_tob" if self.tob_coverage is not None else "missing"
            ),
            "ticker_timestamp": self.timestamp.isoformat() if self.timestamp else "",
            "ticker_age_seconds": self.age_seconds,
            "ticker_fresh": "stale_ticker" not in self.reasons
            and "missing_ticker_timestamp" not in self.reasons,
        }


def build_liquidity_snapshot(
    *,
    symbol: str,
    ticker: Mapping[str, Any],
    reference_timestamp: Any,
    min_depth_usd: float,
    max_ticker_stale_seconds: float,
) -> LiquiditySnapshot:
    reference_ts = parse_timestamp(reference_timestamp)
    ticker_ts = parse_timestamp(ticker.get("timestamp"))
    ticker_age = age_seconds(ticker_ts, reference_ts)
    bid = positive_float(ticker.get("best_bid_price"))
    ask = positive_float(ticker.get("best_ask_price"))
    bid_qty = positive_float(ticker.get("best_bid_qty"))
    ask_qty = positive_float(ticker.get("best_ask_qty"))

    reasons: list[str] = []
    if bid is None or ask is None or bid >= ask:
        reasons.append("missing_or_invalid_book_prices")
    if bid_qty is None or ask_qty is None:
        reasons.append("missing_book_quantities")
    if ticker_ts is None:
        reasons.append("missing_ticker_timestamp")
    elif ticker_age is None or ticker_age > float(max_ticker_stale_seconds):
        reasons.append("stale_ticker")

    spread_bps: float | None = None
    depth_usd: float | None = None
    tob_coverage: float | None = None
    if (
        not reasons
        and bid is not None
        and ask is not None
        and bid_qty is not None
        and ask_qty is not None
    ):
        mid = (bid + ask) / 2.0
        spread_bps = ((ask - bid) / mid) * 10_000.0 if mid > 0.0 else None
        depth_usd = min(bid_qty * bid, ask_qty * ask)
        depth_floor = max(float(min_depth_usd), 1e-9)
        tob_coverage = min(1.0, depth_usd / depth_floor)

    return LiquiditySnapshot(
        symbol=str(symbol).upper(),
        best_bid_price=bid,
        best_bid_qty=bid_qty,
        best_ask_price=ask,
        best_ask_qty=ask_qty,
        timestamp=ticker_ts,
        reference_timestamp=reference_ts,
        spread_bps=spread_bps,
        top_of_book_depth_usd=depth_usd,
        tob_coverage=tob_coverage,
        age_seconds=ticker_age,
        reasons=tuple(reasons),
    )
