from __future__ import annotations

from typing import Any, Mapping

from project.live.cost_estimator import estimate_expected_cost_bps
from project.live.liquidity_snapshot import (
    build_liquidity_snapshot,
    positive_float,
)


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
    close_f = positive_float(close) or 0.0
    liquidity = build_liquidity_snapshot(
        symbol=symbol,
        ticker=ticker,
        reference_timestamp=timestamp,
        min_depth_usd=min_depth_usd,
        max_ticker_stale_seconds=max_ticker_stale_seconds,
    )
    mark_price = positive_float(runtime_features.get("mark_price"))
    cost = estimate_expected_cost_bps(
        spread_bps=liquidity.spread_bps,
        taker_fee_bps=taker_fee_bps,
    )
    reasons = list(liquidity.reasons) + list(cost.reasons)
    mid_price = liquidity.mid_price if liquidity.mid_price is not None else close_f
    if liquidity.mid_price is None and mark_price is not None:
        mid_price = mark_price
    microstructure_regime = (
        "untradable"
        if liquidity.spread_bps is None
        else "healthy"
        if liquidity.spread_bps <= 5.0
        else "degraded"
    )

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
        "spread_bps": liquidity.spread_bps,
        "spread_bps_source": "book_ticker" if liquidity.spread_bps is not None else "missing",
        "depth_usd": liquidity.top_of_book_depth_usd,
        "depth_usd_source": (
            "book_ticker_tob" if liquidity.top_of_book_depth_usd is not None else "missing"
        ),
        "tob_coverage": liquidity.tob_coverage,
        "tob_coverage_source": "book_ticker_tob"
        if liquidity.tob_coverage is not None
        else "missing",
        "expected_cost_bps": cost.expected_cost_bps,
        "expected_cost_bps_source": cost.source,
        "ticker_timestamp": liquidity.timestamp.isoformat() if liquidity.timestamp else "",
        "ticker_age_seconds": liquidity.age_seconds,
        "ticker_fresh": bool(
            liquidity.timestamp is not None
            and liquidity.age_seconds is not None
            and liquidity.age_seconds <= float(max_ticker_stale_seconds)
        ),
        "microstructure_regime": microstructure_regime,
    }
