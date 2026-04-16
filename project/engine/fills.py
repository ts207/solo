from __future__ import annotations

import enum
import logging
from typing import Dict, Any

import numpy as np
import pandas as pd

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
    # Base fill probability by urgency
    if urgency == OrderUrgency.AGGRESSIVE:
        base_prob = 1.0
    elif urgency == OrderUrgency.PASSIVE:
        base_prob = 0.8
    elif urgency == OrderUrgency.DELAYED_AGGRESSIVE:
        base_prob = 0.95
    else:
        base_prob = 0.9

    # Profile adjustments
    if profile == ExecutionProfile.OPTIMISTIC:
        profile_mult = 1.1
    elif profile == ExecutionProfile.STRESSED:
        profile_mult = 0.6
    else:
        profile_mult = 1.0

    # Participation rate impact
    participation_rate = abs(float(order_size)) / max(1.0, float(liquidity_available))
    participation_impact = np.exp(-participation_rate * 5.0)

    # Volatility impact (higher vol -> lower fill prob for passive)
    if urgency == OrderUrgency.PASSIVE:
        vol_impact = np.exp(-vol_regime * 2.0)
    else:
        vol_impact = 1.0

    fill_prob = base_prob * profile_mult * participation_impact * vol_impact
    return float(np.clip(fill_prob, 0.0, 1.0))


def estimate_fill_details(
    order_size: float,
    price: float,
    spread_bps: float,
    liquidity_available: float,
    vol_regime: float,
    urgency: OrderUrgency = OrderUrgency.BASE,
    profile: ExecutionProfile = ExecutionProfile.BASE,
) -> Dict[str, Any]:
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
