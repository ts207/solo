from __future__ import annotations

from datetime import datetime, timezone
from math import isfinite
from typing import Any, Mapping


def _finite_float(raw: Any) -> float | None:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not isfinite(value):
        return None
    return value


def _positive_float(raw: Any) -> float | None:
    value = _finite_float(raw)
    if value is None or value <= 0.0:
        return None
    return value


def _parse_timestamp(raw: Any) -> datetime | None:
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


def _age_seconds(observed: datetime | None, reference: datetime | None) -> float | None:
    if observed is None or reference is None:
        return None
    return max(0.0, (reference - observed).total_seconds())


def resolve_live_market_state(
    *,
    symbol: str,
    close: float,
    timestamp: str,
    ticker: Mapping[str, Any],
    runtime_features: Mapping[str, Any],
    min_depth_usd: float,
    max_ticker_stale_seconds: float,
    taker_fee_bps: float = 2.5,
) -> dict[str, Any]:
    """
    Resolve execution-critical market state from measured feed data only.

    Missing book price, size, timestamp, or stale top-of-book data makes the
    result non-tradable. Configured defaults are deliberately not accepted for
    spread, depth, ToB coverage, or expected cost.
    """
    close_f = _positive_float(close) or 0.0
    reference_ts = _parse_timestamp(timestamp)
    ticker_ts = _parse_timestamp(ticker.get("timestamp"))
    ticker_age = _age_seconds(ticker_ts, reference_ts)

    bid = _positive_float(ticker.get("best_bid_price"))
    ask = _positive_float(ticker.get("best_ask_price"))
    bid_qty = _positive_float(ticker.get("best_bid_qty"))
    ask_qty = _positive_float(ticker.get("best_ask_qty"))
    mark_price = _positive_float(runtime_features.get("mark_price"))

    reasons: list[str] = []
    if bid is None or ask is None or bid >= ask:
        reasons.append("missing_or_invalid_book_prices")
    if bid_qty is None or ask_qty is None:
        reasons.append("missing_book_quantities")
    if ticker_ts is None:
        reasons.append("missing_ticker_timestamp")
    elif ticker_age is None or ticker_age > float(max_ticker_stale_seconds):
        reasons.append("stale_ticker")

    mid_price = close_f
    spread_bps: float | None = None
    depth_usd: float | None = None
    tob_coverage: float | None = None
    expected_cost_bps: float | None = None
    microstructure_regime = "untradable"
    if (
        not reasons
        and bid is not None
        and ask is not None
        and bid_qty is not None
        and ask_qty is not None
    ):
        mid_price = (bid + ask) / 2.0
        spread_bps = ((ask - bid) / mid_price) * 10_000.0 if mid_price > 0.0 else None
        tob_bid_usd = bid_qty * bid
        tob_ask_usd = ask_qty * ask
        depth_usd = min(tob_bid_usd, tob_ask_usd)
        depth_floor = max(float(min_depth_usd), 1e-9)
        tob_coverage = min(1.0, depth_usd / depth_floor)
        expected_cost_bps = (
            (spread_bps / 2.0) + float(taker_fee_bps) if spread_bps is not None else None
        )
        microstructure_regime = (
            "healthy" if spread_bps is not None and spread_bps <= 5.0 else "degraded"
        )
    elif mark_price is not None:
        mid_price = mark_price

    is_complete = not reasons
    return {
        "symbol": str(symbol).upper(),
        "market_state_complete": bool(is_complete),
        "is_execution_tradable": bool(is_complete),
        "non_tradable_reason": ";".join(reasons),
        "non_tradable_reasons": reasons,
        "close": float(close_f),
        "last_price": float(close_f),
        "mid_price": float(mid_price),
        "mark_price": float(mark_price or close_f),
        "mark_price_source": "runtime_market_features"
        if mark_price is not None
        else "current_close_fallback",
        "spread_bps": spread_bps,
        "spread_bps_source": "book_ticker" if spread_bps is not None else "missing",
        "depth_usd": depth_usd,
        "depth_usd_source": "book_ticker_tob" if depth_usd is not None else "missing",
        "tob_coverage": tob_coverage,
        "tob_coverage_source": "book_ticker_tob" if tob_coverage is not None else "missing",
        "expected_cost_bps": expected_cost_bps,
        "expected_cost_bps_source": "spread_derived"
        if expected_cost_bps is not None
        else "missing",
        "ticker_timestamp": ticker_ts.isoformat() if ticker_ts is not None else "",
        "ticker_age_seconds": ticker_age,
        "ticker_fresh": bool(
            ticker_ts is not None
            and ticker_age is not None
            and ticker_age <= float(max_ticker_stale_seconds)
        ),
        "microstructure_regime": microstructure_regime,
    }
