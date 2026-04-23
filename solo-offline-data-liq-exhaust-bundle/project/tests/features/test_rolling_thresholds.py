from __future__ import annotations

import numpy as np
import pandas as pd

from project.features.rolling_thresholds import lagged_rolling_quantile


def test_lagged_rolling_quantile_matches_expected_shifted_behavior():
    series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])

    result = lagged_rolling_quantile(series, window=3, quantile=0.5, min_periods=2)

    expected = pd.Series([np.nan, np.nan, 1.5, 2.0, 3.0], dtype="float")
    pd.testing.assert_series_equal(result.reset_index(drop=True), expected, check_names=False)


def test_lagged_rolling_quantile_defaults_min_periods_to_window():
    series = pd.Series([1.0, 2.0, 3.0, 4.0])

    result = lagged_rolling_quantile(series, window=3, quantile=0.5)

    expected = pd.Series([np.nan, np.nan, np.nan, 2.0], dtype="float")
    pd.testing.assert_series_equal(result.reset_index(drop=True), expected, check_names=False)
