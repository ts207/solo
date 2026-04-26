from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

_LOG = logging.getLogger(__name__)


@dataclass
class WindowResult:
    train_range: tuple[pd.Timestamp, pd.Timestamp]
    test_range: tuple[pd.Timestamp, pd.Timestamp]
    train_metrics: dict[str, float]
    test_metrics: dict[str, float]


def generate_walkforward_windows(
    index: pd.DatetimeIndex,
    train_size_bars: int,
    test_size_bars: int,
    step_size_bars: int,
    embargo_bars: int = 0,
) -> list[tuple[pd.DatetimeIndex, pd.DatetimeIndex]]:
    """
    Generate training and testing window indices.
    """
    windows = []
    total_len = len(index)

    start = 0
    embargo_bars = int(max(0, embargo_bars))
    while start + train_size_bars + embargo_bars + test_size_bars <= total_len:
        train_idx = index[start : start + train_size_bars]
        test_start = start + train_size_bars + embargo_bars
        test_idx = index[test_start : test_start + test_size_bars]
        windows.append((train_idx, test_idx))
        start += step_size_bars

    return windows


def evaluate_walkforward_stability(
    results: list[WindowResult],
) -> dict[str, Any]:
    """
    Calculate stability metrics across walk-forward windows.
    """
    if not results:
        return {}

    test_expectancies = [r.test_metrics.get("expectancy_bps", 0.0) for r in results]
    train_expectancies = [r.train_metrics.get("expectancy_bps", 0.0) for r in results]

    # Expectancy Stability: Standard deviation of expectancy / mean expectancy
    avg_exp = np.mean(test_expectancies)
    std_exp = np.std(test_expectancies)

    # Stability: low variation relative to the absolute mean, zeroed for negative-mean strategies.
    # Using a signal-to-noise ratio formulation:
    #   stability = |mean| / (|mean| + std + eps)
    # This correctly returns 0 for avg_exp <= 0 (no edge) and approaches 1 as noise → 0.
    # A consistent loser (avg=-5, std=1) gets ≈ 5/16 ≈ 0.31, not the artificially-high
    # score that the previous +10 floor produced.
    _eps = 1.0  # 1 bps noise floor; small enough to not inflate losers
    if avg_exp <= 0.0:
        expectancy_stability = 0.0
    else:
        expectancy_stability = abs(avg_exp) / (abs(avg_exp) + std_exp + _eps)

    # Sign Consistency: Percentage of windows with positive expectancy
    sign_consistency = np.mean([1.0 if e > 0 else 0.0 for e in test_expectancies])

    # Degradation: ratio of means is more stable than mean(test/train) near zero.
    avg_train = np.mean(train_expectancies)
    degradation = (avg_exp / avg_train) if abs(avg_train) > 1e-6 else 0.0

    return {
        "avg_test_expectancy_bps": float(avg_exp),
        "avg_train_expectancy_bps": float(avg_train),
        "expectancy_stability": float(np.clip(expectancy_stability, 0.0, 1.0)),
        "sign_consistency": float(sign_consistency),
        "avg_train_test_degradation": float(degradation),
        "n_windows": len(results),
    }
