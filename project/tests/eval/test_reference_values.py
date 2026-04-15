"""
Tests for reference values.
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from project.eval.reference_values import (
    get_reference_sma,
    get_reference_volatility,
    get_synthetic_test_data,
)


def test_reference_sma():
    data = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    expected = pd.Series([np.nan, 1.5, 2.5, 3.5, 4.5])
    actual = get_reference_sma(data, window=2)
    pd.testing.assert_series_equal(actual, expected)


def test_reference_volatility():
    data = pd.Series([1.0, 2.0, 1.0, 2.0])
    # Mean = 1.5. Diffs = [-0.5, 0.5, -0.5, 0.5]. Sq Diffs = [0.25, 0.25, 0.25, 0.25]. Sum = 1.0. Var = 1.0 / (4-1) = 0.333. Std = sqrt(0.333) = 0.577
    actual = get_reference_volatility(data, window=4).iloc[-1]
    assert np.isclose(actual, 0.5773502691896257)


def test_synthetic_data_determinism():
    d1 = get_synthetic_test_data()
    d2 = get_synthetic_test_data()
    pd.testing.assert_series_equal(d1, d2)
