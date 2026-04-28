from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class ExecutionCostBreakdown:
    fee_bps: float
    spread_bps: float
    slippage_bps: float
    funding_bps: float
    total_bps: float
    cost_model_version: str
    degraded: bool = False
    degraded_reason: str | None = None


def estimate_execution_cost_bps(
    *,
    side: Literal["long", "short"],
    entry_price: float,
    exit_price: float | None,
    best_bid: float | None,
    best_ask: float | None,
    funding_rate: float | None,
    horizon_bars: int,
    fee_bps_per_side: float = 2.0,
    fallback_slippage_bps: float = 1.0,
) -> ExecutionCostBreakdown:
    degraded = False
    degraded_reason = None

    fee_bps = fee_bps_per_side * 2.0

    if best_bid and best_ask and best_bid > 0 and best_ask >= best_bid:
        mid = (best_bid + best_ask) / 2.0
        spread_bps = ((best_ask - best_bid) / mid) * 10_000.0
    else:
        spread_bps = 0.0
        degraded = True
        degraded_reason = "missing_bid_ask"

    slippage_bps = fallback_slippage_bps * 2.0

    if funding_rate is not None:
        # funding_rate assumed decimal per 8h; 5m bars -> 96 bars per 8h
        funding_periods = horizon_bars / 96.0
        direction_sign = 1.0 if side == "long" else -1.0
        funding_bps = funding_rate * direction_sign * funding_periods * 10_000.0
    else:
        funding_bps = 0.0
        degraded = True
        degraded_reason = degraded_reason or "missing_funding"

    total = fee_bps + spread_bps + slippage_bps + funding_bps

    return ExecutionCostBreakdown(
        fee_bps=fee_bps,
        spread_bps=spread_bps,
        slippage_bps=slippage_bps,
        funding_bps=funding_bps,
        total_bps=total,
        cost_model_version="paper_cost_v1",
        degraded=degraded,
        degraded_reason=degraded_reason,
    )
