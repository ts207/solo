from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from project.engine.fills import ExecutionProfile, OrderUrgency, calculate_fill_probability
from project.engine.slippage import calculate_fill_price, calculate_slippage_bps

_LOG = logging.getLogger(__name__)


def get_comprehensive_execution_estimate(
    order_size: float,
    base_price: float,
    is_buy: bool,
    market_data: dict[str, Any],
    urgency: str = "aggressive",
    profile: str = "base",
) -> dict[str, Any]:
    """
    Get detailed execution estimate including fill prob and expected price.
    """
    urgency_enum = OrderUrgency(urgency.lower())
    profile_enum = ExecutionProfile(profile.lower())

    spread_bps = float(market_data.get("spread_bps", 1.0))
    liquidity_available = float(market_data.get("liquidity_available", 1e6))
    vol_regime_bps = float(market_data.get("vol_regime_bps", 10.0))

    order_size_abs = abs(float(order_size))

    fill_prob = calculate_fill_probability(
        order_size=order_size_abs,
        liquidity_available=liquidity_available,
        spread_bps=spread_bps,
        vol_regime=vol_regime_bps / 10000.0,  # scaled for fills.py
        urgency=urgency_enum,
        profile=profile_enum,
    )

    slippage_bps = calculate_slippage_bps(
        order_size=order_size_abs,
        spread_bps=spread_bps,
        liquidity_available=liquidity_available,
        vol_regime_bps=vol_regime_bps,
        urgency=urgency_enum,
        profile=profile_enum,
    )

    expected_fill_price = calculate_fill_price(
        base_price=base_price,
        is_buy=is_buy,
        slippage_bps=slippage_bps,
    )

    return {
        "expected_fill_price": expected_fill_price,
        "fill_probability": fill_prob,
        "expected_slippage_bps": slippage_bps,
        "residual_unfilled_quantity": order_size_abs * (1.0 - fill_prob),
    }


def load_calibration_config(
    symbol: str,
    *,
    calibration_dir,
    base_config: dict,
) -> dict:
    """
    Merge per-symbol calibration JSON (if present) over base_config.
    Keys in the calibration file override base_config; absent or None-valued keys
    are preserved from base. Returns a merged dict safe to pass to
    estimate_transaction_cost_bps as the config argument.
    """
    path = Path(calibration_dir) / f"{symbol}.json"
    merged = dict(base_config)
    if path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                _LOG.warning("Calibration file %s is not a JSON object; ignoring.", path)
            else:
                merged.update({k: v for k, v in raw.items() if v is not None})
        except json.JSONDecodeError as exc:
            _LOG.warning("Malformed calibration JSON at %s: %s; using base_config.", path, exc)
    return merged
