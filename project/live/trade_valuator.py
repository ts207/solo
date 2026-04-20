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
    win_probability: float
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
    """
    P(order executes at a reasonable price).

    For market/aggressive orders on liquid perp markets this is near-certain;
    for passive limit orders it is spread-dependent. Uses a multiplicative
    degradation model rather than additive adjustments.
    """
    if not market_state.get("is_execution_tradable", True):
        return 0.0

    spread_bps = _finite(market_state.get("spread_bps"), 999.0)
    depth_usd = _finite(
        market_state.get("top_of_book_depth_usd")
        or market_state.get("depth_usd")
        or market_state.get("liquidity_available"),
        0.0,
    )
    urgency = str(route_preference or market_state.get("route_preference", "")).strip().lower()

    if urgency in ("aggressive", "market"):
        base = 0.95
        spread_ref = 10.0
    elif urgency == "passive":
        base = 0.60
        spread_ref = 3.0
    else:
        base = 0.88
        spread_ref = 7.0

    # Spread: log-scaled multiplicative penalty above the per-route reference
    if spread_bps > spread_ref:
        spread_factor = 1.0 / (1.0 + 0.30 * math.log(spread_bps / spread_ref))
    else:
        spread_factor = 1.0

    # Depth: shallow book means partial fills or worse slippage
    if depth_usd <= 0.0:
        depth_factor = 0.75
    elif depth_usd < 25_000.0:
        depth_factor = 0.80 + 0.20 * (depth_usd / 25_000.0)
    else:
        depth_factor = 1.0

    return max(0.05, min(0.98, base * spread_factor * depth_factor))


def _infer_win_probability(
    *,
    probability_positive: float,
    net_edge_bps: float,
    downside_bps: float,
) -> float:
    """
    P(trade is profitable after costs).

    Uses `probability_positive` from the thesis when available; otherwise derives
    a conservative estimate from the Kelly break-even win rate given the
    net-edge / downside ratio.
    """
    if probability_positive > 0.01:
        return max(0.01, min(0.99, probability_positive))

    if net_edge_bps <= 0.0:
        return 0.30  # negative expected edge: pessimistic prior

    loss = max(1.0, downside_bps)
    gain = max(1.0, net_edge_bps)
    # Kelly break-even: the win rate at which EV = 0
    break_even = loss / (gain + loss)
    # Conservative estimate: break_even + half the remaining headroom,
    # scaled by the edge/downside ratio
    edge_ratio = min(1.0, gain / loss)
    headroom = max(0.0, 1.0 - break_even)
    return min(0.80, break_even + 0.5 * headroom * edge_ratio)


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

    # Fill probability: use explicit value from metadata, else estimate from market state
    fill_probability = _finite(metadata.get("fill_probability"), -1.0)
    if fill_probability < 0.0:
        fill_probability = estimate_fill_probability(
            market_state=market_state,
            route_preference=str(metadata.get("route_preference", "")),
        )
    fill_probability = max(0.0, min(1.0, fill_probability))

    # Win probability: use probability_positive_post_cost from intent when set by decision layer
    win_probability = _infer_win_probability(
        probability_positive=_finite(intent.probability_positive_post_cost, 0.0),
        net_edge_bps=net_edge,
        downside_bps=downside,
    )

    edge_confidence = max(
        0.0,
        min(
            1.0,
            _finite(metadata.get("edge_confidence"), _confidence_from_band(intent.confidence_band)),
        ),
    )
    if edge_confidence <= 0.0 and intent.support_score > 0.0:
        edge_confidence = max(0.0, min(1.0, float(intent.support_score)))

    # Expected value: P(fill) × [P(win)×net_edge − P(loss)×downside], discounted by confidence
    expected_value = fill_probability * (
        win_probability * net_edge - (1.0 - win_probability) * downside
    )
    utility = expected_value * max(0.05, edge_confidence)

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
        win_probability=float(win_probability),
        edge_confidence=float(edge_confidence),
        utility_score=float(utility),
        should_trade=not reasons,
        reasons=tuple(reasons),
    )
