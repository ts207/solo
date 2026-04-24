"""
Verification tests for technical features.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from project.eval.feature_verification_suite import (
    run_feature_verification,
)
from project.eval.reference_values import get_reference_volatility, get_synthetic_test_data
from project.eval.verification import VerificationHarness
from project.pipelines.features.build_features import _safe_logret_1


def test_feature_verification_suite_run():
    report = run_feature_verification()
    assert len(report) == 5
    assert all(report["pass"])


def test_logret_1_verification():
    harness = VerificationHarness()
    data = pd.Series([100.0, 101.0, 99.0, 102.0])

    # Reference calculation
    expected = np.log(data / data.shift(1))

    # Actual calculation
    actual = _safe_logret_1(data)

    result = harness.compare_series(actual, expected)
    assert result["pass"] is True


def test_rv_96_verification():
    harness = VerificationHarness(tolerance=1e-5)
    n = 200
    data = get_synthetic_test_data(n)

    # Simulate the pipeline calculation for rv_96
    logret = _safe_logret_1(data)
    rv_window = 96
    actual_rv = logret.rolling(window=rv_window, min_periods=8).std()

    # Reference calculation
    expected_rv = get_reference_volatility(logret, window=rv_window)

    # Compare
    # Align both by dropping NaNs from the most restrictive one (expected_rv)
    # and then subsetting actual_rv to match.
    common_index = expected_rv.dropna().index
    result = harness.compare_series(actual_rv.loc[common_index], expected_rv.loc[common_index])
    assert result["pass"] is True
