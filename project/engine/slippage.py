from __future__ import annotations

import logging
from typing import Dict, Any

import numpy as np
import pandas as pd

from project.engine.fills import OrderUrgency, ExecutionProfile

_LOG = logging.getLogger(__name__)


def calibrate_passive_slippage() -> float:
    """
    Returns the calibrated adverse selection cost for passive fills.
    Fixed at 0.2 bps until the TOB-based calibration pipeline is automated.
    """
    return 0.2


def calculate_slippage_bps(
    order_size: float,
    spread_bps: float,
    liquidity_available: float,
    vol_regime_bps: float,
    urgency: OrderUrgency,
    profile: ExecutionProfile,
) -> float:
    """
    Calculate expected slippage in bps.
    """
    # Base slippage components
    # Aggressive takes half spread + impact
    # Passive might have zero or negative slippage but higher fill risk

    if urgency == OrderUrgency.AGGRESSIVE:
        base_slippage = spread_bps * 0.5
    elif urgency == OrderUrgency.PASSIVE:
        base_slippage = calibrate_passive_slippage()  # Adverse selection calibrated default
    elif urgency == OrderUrgency.DELAYED_AGGRESSIVE:
        base_slippage = spread_bps * 0.7  # Delayed often means worse price
    else:
        base_slippage = spread_bps * 0.5

    # Profile adjustments
    if profile == ExecutionProfile.OPTIMISTIC:
        profile_mult = 0.8
        impact_sqrt_mult = 5.0
    elif profile == ExecutionProfile.STRESSED:
        profile_mult = 1.5
        impact_sqrt_mult = 20.0
    else:
        profile_mult = 1.0
        impact_sqrt_mult = 10.0

    # Impact calculation (Square root model)
    participation_rate = abs(float(order_size)) / max(1.0, float(liquidity_available))
    impact_bps = np.sqrt(participation_rate) * impact_sqrt_mult

    # Volatility impact (higher vol -> wider uncertainty / slippage)
    vol_impact_bps = vol_regime_bps * 0.1

    total_slippage_bps = (base_slippage + impact_bps + vol_impact_bps) * profile_mult

    return float(np.clip(total_slippage_bps, 0.0, 1000.0))


def calculate_fill_price(
    base_price: float,
    is_buy: bool,
    slippage_bps: float,
) -> float:
    """
    Apply slippage to base price.
    """
    mult = 1.0 + (slippage_bps / 10000.0) if is_buy else 1.0 - (slippage_bps / 10000.0)
    return float(base_price * mult)
