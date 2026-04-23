from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from project.live.trade_valuator import TradeValuation


@dataclass(frozen=True)
class ExecutionSchedule:
    route_preference: str
    child_order_count: int
    child_notional: float
    time_in_force_seconds: int
    cancel_after_seconds: int
    post_only: bool
    urgency: str


def build_execution_schedule(
    *,
    valuation: TradeValuation,
    notional: float,
    market_state: Mapping[str, Any],
) -> ExecutionSchedule:
    spread_bps = float(market_state.get("spread_bps", 999.0) or 999.0)
    depth = float(
        market_state.get("top_of_book_depth_usd")
        or market_state.get("depth_usd")
        or market_state.get("liquidity_available")
        or 0.0
    )
    urgency = "normal"
    if valuation.expected_net_edge_bps >= valuation.expected_downside_bps:
        urgency = "high"
    elif valuation.fill_probability < 0.45:
        urgency = "low"

    passive = spread_bps >= 3.0 and urgency != "high"
    route = "passive" if passive else "aggressive"
    participation = notional / depth if depth > 0.0 else 1.0
    if participation <= 0.02:
        child_count = 1
    elif participation <= 0.08:
        child_count = 3
    else:
        child_count = 5
    tif = 90 if passive else 15
    if urgency == "high":
        tif = 10
    return ExecutionSchedule(
        route_preference=route,
        child_order_count=child_count,
        child_notional=float(notional / child_count) if child_count else 0.0,
        time_in_force_seconds=tif,
        cancel_after_seconds=tif,
        post_only=passive,
        urgency=urgency,
    )
