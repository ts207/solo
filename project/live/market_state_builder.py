from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from project.core.regime_classifier import classify_regime
from project.live.cost_estimator import estimate_expected_cost_bps
from project.live.liquidity_snapshot import (
    build_liquidity_snapshot,
    parse_timestamp,
    positive_float,
)

_FUNDING_REQUIRED_EVENTS: frozenset[str] = frozenset({"FND_DISLOC"})
_OPEN_INTEREST_REQUIRED_EVENTS: frozenset[str] = frozenset(
    {"LIQUIDATION_CASCADE", "OI_SPIKE_NEGATIVE"}
)


@dataclass(frozen=True)
class MarketStateBuilderConfig:
    min_depth_usd: float = 25_000.0
    max_ticker_stale_seconds: float = 30.0
    taker_fee_bps: float = 2.5
    runtime_feature_stale_after_seconds: float = 60.0


def strategy_requires_funding_freshness(supported_event_ids: Sequence[str]) -> bool:
    supported = {str(item).strip().upper() for item in supported_event_ids if str(item).strip()}
    return bool(supported.intersection(_FUNDING_REQUIRED_EVENTS))


def strategy_requires_open_interest_freshness(supported_event_ids: Sequence[str]) -> bool:
    supported = {str(item).strip().upper() for item in supported_event_ids if str(item).strip()}
    return bool(supported.intersection(_OPEN_INTEREST_REQUIRED_EVENTS))


def build_measured_market_state(
    *,
    symbol: str,
    timeframe: str,
    close: float,
    timestamp: str,
    move_bps: float,
    ticker: Mapping[str, Any],
    runtime_features: Mapping[str, Any],
    supported_event_ids: Sequence[str],
    config: MarketStateBuilderConfig,
    liquidation_notional_usd: float = 0.0,
    liquidation_notional_source: str = "missing",
) -> dict[str, Any]:
    normalized_symbol = str(symbol).upper()
    liquidity = build_liquidity_snapshot(
        symbol=normalized_symbol,
        ticker=ticker,
        reference_timestamp=timestamp,
        min_depth_usd=float(config.min_depth_usd),
        max_ticker_stale_seconds=float(config.max_ticker_stale_seconds),
    )
    cost = estimate_expected_cost_bps(
        spread_bps=liquidity.spread_bps,
        taker_fee_bps=float(config.taker_fee_bps),
    )

    reference_ts = parse_timestamp(timestamp)
    runtime = dict(runtime_features)
    reasons = list(liquidity.reasons)
    reasons.extend(cost.reasons)

    funding_state = _runtime_component_state(
        runtime,
        value_key="funding_rate",
        timestamp_key="funding_timestamp",
        reference_timestamp=reference_ts,
        max_age_seconds=float(config.runtime_feature_stale_after_seconds),
        required=strategy_requires_funding_freshness(supported_event_ids),
        missing_reason="missing_funding_state",
        stale_reason="stale_funding_state",
    )
    open_interest_state = _runtime_component_state(
        runtime,
        value_key="open_interest",
        timestamp_key="open_interest_timestamp",
        reference_timestamp=reference_ts,
        max_age_seconds=float(config.runtime_feature_stale_after_seconds),
        required=strategy_requires_open_interest_freshness(supported_event_ids),
        missing_reason="missing_open_interest_state",
        stale_reason="stale_open_interest_state",
    )
    reasons.extend(funding_state["blocking_reasons"])
    reasons.extend(open_interest_state["blocking_reasons"])

    close_f = positive_float(close) or 0.0
    mark_price = positive_float(runtime.get("mark_price"))
    mid_price = liquidity.mid_price
    if mid_price is None:
        mid_price = mark_price if mark_price is not None else close_f

    regime = classify_regime(
        move_bps=float(move_bps),
        rv_pct=runtime.get("rv_pct"),
        ms_trend_state=runtime.get("ms_trend_state"),
    )
    market_state_complete = not reasons
    fields = liquidity.to_market_fields()
    fields.update(
        {
            "symbol": normalized_symbol,
            "timeframe": str(timeframe),
            "timestamp": str(timestamp),
            "market_state_complete": bool(market_state_complete),
            "is_execution_tradable": bool(market_state_complete),
            "non_tradable_reason": ";".join(reasons),
            "non_tradable_reasons": reasons,
            "close": float(close_f),
            "last_price": float(close_f),
            "mid_price": float(mid_price),
            "mark_price": float(mark_price or close_f),
            "mark_price_source": "runtime_market_features"
            if mark_price is not None
            else "current_close_fallback",
            "expected_cost_bps": cost.expected_cost_bps,
            "expected_cost_bps_source": cost.source,
            "microstructure_regime": _microstructure_regime(liquidity.spread_bps),
            "canonical_regime": regime.regime.value,
            "regime_mode": regime.mode.value,
            "regime_confidence": regime.confidence,
            "regime_metadata": regime.metadata,
            "funding_rate": float(runtime.get("funding_rate", 0.0) or 0.0),
            "funding_rate_source": funding_state["source"],
            "funding_timestamp": funding_state["timestamp"],
            "funding_age_seconds": funding_state["age_seconds"],
            "funding_fresh": funding_state["fresh"],
            "open_interest": float(runtime.get("open_interest", 0.0) or 0.0),
            "open_interest_source": open_interest_state["source"],
            "open_interest_delta_fraction": float(
                runtime.get("open_interest_delta_fraction", 0.0) or 0.0
            ),
            "open_interest_timestamp": open_interest_state["timestamp"],
            "open_interest_age_seconds": open_interest_state["age_seconds"],
            "open_interest_fresh": open_interest_state["fresh"],
            "liquidation_notional_usd": float(liquidation_notional_usd),
            "liquidation_notional_source": str(liquidation_notional_source),
        }
    )
    return fields


def _runtime_component_state(
    runtime_features: Mapping[str, Any],
    *,
    value_key: str,
    timestamp_key: str,
    reference_timestamp: Any,
    max_age_seconds: float,
    required: bool,
    missing_reason: str,
    stale_reason: str,
) -> dict[str, Any]:
    has_value = value_key in runtime_features
    timestamp_raw = runtime_features.get(timestamp_key) or runtime_features.get("refreshed_at")
    observed_ts = parse_timestamp(timestamp_raw)
    age = None
    reference_ts = parse_timestamp(reference_timestamp)
    if observed_ts is not None and reference_ts is not None:
        age = max(0.0, (reference_ts - observed_ts).total_seconds())
    fresh = bool(
        has_value and observed_ts is not None and age is not None and age <= max_age_seconds
    )
    blocking: list[str] = []
    if required and not has_value:
        blocking.append(missing_reason)
    elif required and not fresh:
        blocking.append(stale_reason)
    return {
        "source": "runtime_market_features" if has_value else "missing",
        "timestamp": observed_ts.isoformat() if observed_ts is not None else "",
        "age_seconds": age,
        "fresh": fresh,
        "blocking_reasons": blocking,
    }


def _microstructure_regime(spread_bps: float | None) -> str:
    if spread_bps is None:
        return "untradable"
    return "healthy" if spread_bps <= 5.0 else "degraded"
