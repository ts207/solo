from __future__ import annotations

import logging
from typing import Dict, Any, List

import numpy as np
import pandas as pd

_LOG = logging.getLogger(__name__)


def evaluate_regime_stability(
    returns: pd.Series,
    regimes: pd.Series,
) -> Dict[str, Any]:
    """
    Evaluate performance stability across different regimes.
    Regimes can be vol, trend, or liquidity states.
    """
    if returns.empty or regimes.empty:
        return {}

    # Align data
    common_idx = returns.index.intersection(regimes.index)
    returns = returns.reindex(common_idx)
    regimes = regimes.reindex(common_idx)

    # Performance by regime
    perf_by_regime = returns.groupby(regimes).mean()
    expectancy_bps = perf_by_regime * 10000.0

    # Metrics
    # Sharpe Stability: mean(SR_regime) / std(SR_regime)
    sr_by_regime = returns.groupby(regimes).apply(lambda x: x.mean() / max(1e-6, x.std()))
    sr_stability = sr_by_regime.mean() / max(1e-6, sr_by_regime.std())

    # Worst Slice PnL / Mean PnL
    worst_slice = expectancy_bps.min()
    avg_expectancy = expectancy_bps.mean()
    concentration_ratio = worst_slice / max(1e-6, avg_expectancy)

    return {
        "expectancy_by_regime_bps": expectancy_bps.to_dict(),
        "sr_by_regime": sr_by_regime.to_dict(),
        "sr_stability_ratio": float(sr_stability),
        "regime_concentration_ratio": float(concentration_ratio),
        "is_stable": bool(sr_stability > 0.5 and concentration_ratio > -0.5),
    }


def analyze_parameter_ruggedness(
    parameter_grid: Dict[str, List[float]],
    performance_matrix: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Check if performance is sensitive to small parameter changes.
    ruggedness = mean(gradient)
    """
    if performance_matrix.empty:
        return {}

    # Calculate gradients between adjacent parameter points
    # A rugged performance landscape is bad (overfitting)
    gradients = performance_matrix.diff().dropna().abs()
    avg_ruggedness = gradients.mean().mean()

    return {
        "avg_ruggedness_index": float(avg_ruggedness),
        "is_smooth": bool(avg_ruggedness < 0.1),  # Threshold for ruggedness
    }
