"""
Ground truth reference values for basic features.
"""

from __future__ import annotations

import pandas as pd
import numpy as np


def get_reference_sma(data: pd.Series, window: int) -> pd.Series:
    """
    Reference implementation of Simple Moving Average.
    """
    return data.rolling(window=window).mean()


def get_reference_volatility(data: pd.Series, window: int) -> pd.Series:
    """
    Reference implementation of rolling volatility (std dev).
    """
    return data.rolling(window=window).std()


def get_synthetic_test_data(n: int = 100) -> pd.Series:
    """
    Generate synthetic test data for verification.
    Uses exponential of a cumulative sum to ensure positive prices.
    """
    np.random.seed(42)
    return pd.Series(np.exp(np.random.randn(n).cumsum() * 0.01) * 100.0)
