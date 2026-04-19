from __future__ import annotations

import logging

from project.core.execution_costs import estimate_slippage_bps_v2
from project.engine.fills import ExecutionProfile, OrderUrgency

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
    return estimate_slippage_bps_v2(
        order_size=order_size,
        spread_bps=spread_bps,
        liquidity_available=liquidity_available,
        vol_regime_bps=vol_regime_bps,
        urgency=urgency.value,
        profile=profile.value,
        passive_adverse_selection_bps=calibrate_passive_slippage(),
    )


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
