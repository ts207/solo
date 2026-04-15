from __future__ import annotations

import logging
from typing import Dict, Any

import numpy as np
import pandas as pd

_LOG = logging.getLogger(__name__)


def basic_zscore_reversion(
    bars: pd.DataFrame,
    window: int = 20,
    z_threshold: float = 2.0,
) -> pd.Series:
    """
    Fade simple standard deviation extensions.
    """
    close = bars["close"]
    rolling_mean = close.rolling(window).mean()
    rolling_std = close.rolling(window).std()
    z_score = (close - rolling_mean) / rolling_std

    positions = pd.Series(0, index=bars.index)
    positions[z_score > z_threshold] = -1  # Fade overextension
    positions[z_score < -z_threshold] = 1  # Fade underextension
    return positions


def basic_breakout_rule(
    bars: pd.DataFrame,
    window: int = 20,
) -> pd.Series:
    """
    Trend following on local high/low breaks.
    """
    high = bars["high"].shift(1).rolling(window).max()
    low = bars["low"].shift(1).rolling(window).min()
    close = bars["close"]

    positions = pd.Series(0, index=bars.index)
    positions[close > high] = 1
    positions[close < low] = -1
    return positions


def simple_funding_extreme_fade(
    bars: pd.DataFrame,
    funding_rate: pd.Series,
    threshold: float = 0.0005,  # 5bps per 8h usually
) -> pd.Series:
    """
    Contrarian entries at extreme funding rates.
    """
    positions = pd.Series(0, index=bars.index)
    aligned_funding = funding_rate.reindex(bars.index).ffill()

    positions[aligned_funding > threshold] = -1  # Short crowded longs
    positions[aligned_funding < -threshold] = 1  # Long crowded shorts
    return positions


def naive_vwap_pullback(
    bars: pd.DataFrame,
    z_threshold: float = 1.0,
) -> pd.Series:
    """
    Mean reversion towards VWAP.
    """
    v = bars["volume"]
    p = (bars["high"] + bars["low"] + bars["close"]) / 3.0
    vwap = (p * v).cumsum() / v.cumsum()

    # Simple distance from VWAP
    dist = (bars["close"] - vwap) / bars["close"]
    std = dist.rolling(100).std()
    z_score = dist / std

    positions = pd.Series(0, index=bars.index)
    positions[z_score > z_threshold] = -1
    positions[z_score < -z_threshold] = 1
    return positions


def evaluate_baseline_performance(
    bars: pd.DataFrame,
    returns: pd.Series,
    baseline_type: str,
    **kwargs,
) -> Dict[str, float]:
    """
    Helper to get baseline metrics for comparison.
    """
    if baseline_type == "zscore":
        pos = basic_zscore_reversion(bars, **kwargs)
    elif baseline_type == "breakout":
        pos = basic_breakout_rule(bars, **kwargs)
    elif baseline_type == "funding":
        pos = simple_funding_extreme_fade(bars, **kwargs)
    elif baseline_type == "vwap":
        pos = naive_vwap_pullback(bars, **kwargs)
    else:
        raise ValueError(f"Unknown baseline type: {baseline_type}")

    pnl = pos.shift(1) * returns
    expectancy = pnl.mean() * 10000.0 if not pnl.empty else 0.0
    sharpe = (
        (pnl.mean() / pnl.std() * np.sqrt(252 * 24)) if not pnl.empty and pnl.std() > 0 else 0.0
    )

    return {
        "expectancy_bps": float(expectancy),
        "sharpe_ratio": float(sharpe),
    }
