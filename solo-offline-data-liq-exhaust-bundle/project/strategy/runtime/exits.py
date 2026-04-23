from __future__ import annotations

import logging
from typing import Dict, Any, Optional, Tuple

import numpy as np
import pandas as pd

_LOG = logging.getLogger(__name__)


def check_exit_conditions(
    bar: pd.Series,
    position_entry_price: float,
    is_long: bool,
    blueprint_exit: Dict[str, Any],
    bars_held: int,
    market_data: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str]:
    """
    Evaluate adaptive exit conditions.
    """

    # 1. Fixed Horizon (Timeout)
    timeout_bars = int(blueprint_exit.get("time_stop_bars", 96))
    if bars_held >= timeout_bars:
        return True, "time_stop"

    # 2. Target Hit
    target_value = float(blueprint_exit.get("target_value", 0.05))
    target_type = str(blueprint_exit.get("target_type", "percent")).lower()

    # Calculate target price
    if target_type == "percent":
        target_price = (
            position_entry_price * (1.0 + target_value)
            if is_long
            else position_entry_price * (1.0 - target_value)
        )
    elif target_type == "atr":
        atr = float(bar.get("atr", 0.0))
        target_price = (
            position_entry_price + (target_value * atr)
            if is_long
            else position_entry_price - (target_value * atr)
        )
    else:
        target_price = (
            position_entry_price * (1.0 + target_value)
            if is_long
            else position_entry_price * (1.0 - target_value)
        )

    # Check against open (gap) and close. Some callers only provide ``close``
    # and ``atr``; in that case treat the bar as gap-free.
    bar_close = float(bar["close"])
    bar_open = float(bar.get("open", bar_close))

    if is_long:
        # For long, hit target if high >= target. But here we only have open/close.
        # Conservative: check max(open, close) >= target
        high_proxy = max(bar_open, bar_close)
        if high_proxy >= target_price:
            return True, "target_hit"
    else:
        # For short, hit target if low <= target.
        # Conservative: check min(open, close) <= target
        low_proxy = min(bar_open, bar_close)
        if low_proxy <= target_price:
            return True, "target_hit"

    # 3. Stop Hit
    stop_value = float(blueprint_exit.get("stop_value", 0.03))
    stop_type = str(blueprint_exit.get("stop_type", "percent")).lower()

    if stop_type == "percent":
        stop_price = (
            position_entry_price * (1.0 - stop_value)
            if is_long
            else position_entry_price * (1.0 + stop_value)
        )
    elif stop_type == "atr":
        atr = float(bar.get("atr", 0.0))
        stop_price = (
            position_entry_price - (stop_value * atr)
            if is_long
            else position_entry_price + (stop_value * atr)
        )
    else:
        stop_price = (
            position_entry_price * (1.0 - stop_value)
            if is_long
            else position_entry_price * (1.0 + stop_value)
        )

    if is_long:
        # For long, stop hit if low <= stop.
        # Check min(open, close) <= stop (capture gap down)
        low_proxy = min(bar_open, bar_close)
        if low_proxy <= stop_price:
            return True, "stop_hit"
    else:
        # For short, stop hit if high >= stop.
        # Check max(open, close) >= stop (capture gap up)
        high_proxy = max(bar_open, bar_close)
        if high_proxy >= stop_price:
            return True, "stop_hit"

    # 4. Invalidation / Opposing Event
    if market_data:
        # Volatility normalization (Exit if vol collapses or explodes)
        vol_current = float(market_data.get("vol_regime_bps", 0.0))
        if vol_current > float(blueprint_exit.get("vol_max_bps", 1000.0)):
            return True, "vol_normalization_high"

        # Event invalidation (Check for opposing signal)
        opposing_event = market_data.get("opposing_event", False)
        if opposing_event:
            return True, "opposing_event"

    return False, ""
