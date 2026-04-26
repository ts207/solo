from __future__ import annotations

import enum
import logging
from typing import Any

from project.core.execution_costs import estimate_fill_probability_v2

_LOG = logging.getLogger(__name__)


class OrderUrgency(enum.Enum):
    BASE = "base"
    PASSIVE = "passive"
    AGGRESSIVE = "aggressive"
    DELAYED_AGGRESSIVE = "delayed_aggressive"


class ExecutionProfile(enum.Enum):
    BASE = "base"
    OPTIMISTIC = "optimistic"
    STRESSED = "stressed"


def calculate_fill_probability(
    order_size: float,
    liquidity_available: float,
    spread_bps: float,
    vol_regime: float,
    urgency: OrderUrgency,
    profile: ExecutionProfile,
) -> float:
    """
    Calculate fill probability based on order size, market conditions, and urgency.
    """
    return estimate_fill_probability_v2(
        order_size=order_size,
        liquidity_available=liquidity_available,
        spread_bps=spread_bps,
        vol_regime_bps=float(vol_regime) * 10_000.0,
        urgency=urgency.value,
        profile=profile.value,
    )


def estimate_fill_details(
    order_size: float,
    price: float,
    spread_bps: float,
    liquidity_available: float,
    vol_regime: float,
    urgency: OrderUrgency = OrderUrgency.BASE,
    profile: ExecutionProfile = ExecutionProfile.BASE,
) -> dict[str, Any]:
    """
    Detailed fill estimation.
    """
    prob = calculate_fill_probability(
        order_size, liquidity_available, spread_bps, vol_regime, urgency, profile
    )

    # Simple Monte Carlo for fill if needed, but here we return expectation
    # In backtest we might want to use the prob to decide if filled or use it for scaling

    return {
        "fill_probability": prob,
        "expected_filled_quantity": order_size * prob,
        "residual_quantity": order_size * (1.0 - prob),
    }
