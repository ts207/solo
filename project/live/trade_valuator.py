from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping

from project.live.contracts.trade_intent import TradeIntent


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


@dataclass(frozen=True)
class TradeValuation:
    expected_gross_edge_bps: float
    expected_cost_bps: float
    expected_net_edge_bps: float
    expected_downside_bps: float
    fill_probability: float
    edge_confidence: float
    utility_score: float
    should_trade: bool
    reasons: tuple[str, ...] = ()

    @property
    def expected_net_pnl_bps(self) -> float:
        return self.expected_net_edge_bps * self.fill_probability


def _confidence_from_band(band: str) -> float:
    return {
        "high": 0.85,
        "medium": 0.65,
        "low": 0.40,
        "none": 0.0,
    }.get(str(band).strip().lower(), 0.0)


def estimate_fill_probability(
    *,
    market_state: Mapping[str, Any],
    route_preference: str = "",
) -> float:
    spread_bps = _finite(market_state.get("spread_bps"), 999.0)
    depth_usd = _finite(
        market_state.get("top_of_book_depth_usd")
        or market_state.get("depth_usd")
        or market_state.get("liquidity_available"),
        0.0,
    )
    urgency = str(route_preference or market_state.get("route_preference", "")).strip().lower()
    probability = 0.70
    if spread_bps <= 3.0:
        probability += 0.10
    elif spread_bps >= 10.0:
        probability -= 0.20
    if depth_usd >= 100_000.0:
        probability += 0.10
    elif depth_usd < 25_000.0:
        probability -= 0.25
    if urgency == "passive":
        probability -= 0.15
    elif urgency == "aggressive":
        probability += 0.10
    return max(0.05, min(0.98, probability))


def value_trade_intent(
    *,
    intent: TradeIntent,
    market_state: Mapping[str, Any],
) -> TradeValuation:
    metadata = dict(intent.metadata or {})
    gross_edge = _finite(
        metadata.get("expected_gross_edge_bps")
        or metadata.get("expected_return_bps")
        or market_state.get("expected_return_bps"),
        0.0,
    )
    cost = _finite(
        metadata.get("expected_cost_bps")
        or market_state.get("expected_cost_bps")
        or market_state.get("estimated_cost_bps"),
        0.0,
    )
    explicit_net = metadata.get("expected_net_edge_bps", market_state.get("expected_net_edge_bps"))
    net_edge = _finite(explicit_net, gross_edge - cost)
    downside = abs(
        _finite(
            metadata.get("expected_downside_bps")
            or metadata.get("expected_adverse_bps")
            or market_state.get("expected_adverse_bps"),
            max(1.0, gross_edge),
        )
    )
    fill_probability = _finite(metadata.get("fill_probability"), -1.0)
    if fill_probability < 0.0:
        fill_probability = estimate_fill_probability(
            market_state=market_state,
            route_preference=str(metadata.get("route_preference", "")),
        )
    fill_probability = max(0.0, min(1.0, fill_probability))
    edge_confidence = max(
        0.0,
        min(
            1.0,
            _finite(metadata.get("edge_confidence"), _confidence_from_band(intent.confidence_band)),
        ),
    )
    if edge_confidence <= 0.0 and intent.support_score > 0.0:
        edge_confidence = max(0.0, min(1.0, float(intent.support_score)))

    utility = (net_edge * fill_probability * max(0.05, edge_confidence)) - (
        0.25 * downside * (1.0 - edge_confidence)
    )
    reasons: list[str] = []
    if net_edge <= 0.0:
        reasons.append("expected_net_edge_non_positive")
    if fill_probability < 0.20:
        reasons.append("fill_probability_low")
    if edge_confidence < 0.20:
        reasons.append("edge_confidence_low")
    if utility <= 0.0:
        reasons.append("expected_utility_non_positive")
    return TradeValuation(
        expected_gross_edge_bps=float(gross_edge),
        expected_cost_bps=float(cost),
        expected_net_edge_bps=float(net_edge),
        expected_downside_bps=float(downside),
        fill_probability=float(fill_probability),
        edge_confidence=float(edge_confidence),
        utility_score=float(utility),
        should_trade=not reasons,
        reasons=tuple(reasons),
    )
