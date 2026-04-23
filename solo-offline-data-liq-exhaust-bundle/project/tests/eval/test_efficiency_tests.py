import pandas as pd
import pytest

from project.eval.efficiency_tests import (
    build_efficiency_report,
    compute_hurst_exponent,
    compute_return_autocorrelation,
    compute_variance_ratio,
)


def test_compute_return_autocorrelation_detects_negative_serial_dependence():
    returns = pd.Series([1.0, -1.0, 1.0, -1.0, 1.0, -1.0])
    assert compute_return_autocorrelation(returns) == pytest.approx(-1.0)


def test_compute_variance_ratio_matches_manual_rollup():
    returns = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    one_step_var = returns.var(ddof=1)
    two_step = pd.Series([3.0, 5.0, 7.0, 9.0])
    expected = two_step.var(ddof=1) / (2.0 * one_step_var)
    assert compute_variance_ratio(returns, lag=2) == pytest.approx(expected)


def test_build_efficiency_report_exposes_expected_keys():
    report = build_efficiency_report(pd.Series([0.1, -0.2, 0.3, -0.1]), lag=2)
    assert set(report) == {
        "hurst_exponent",
        "observations",
        "return_autocorr",
        "variance_ratio",
    }
    assert report["observations"] == 4.0


def test_compute_hurst_exponent_detects_persistent_series():
    base = pd.Series([0.3] * 40 + [-0.05] * 5 + [0.25] * 40 + [-0.05] * 5)
    hurst = compute_hurst_exponent(base, min_lag=2, max_lag=10)
    assert hurst > 0.6


def test_compute_hurst_exponent_returns_nan_for_short_series():
    hurst = compute_hurst_exponent(pd.Series([0.1, -0.1, 0.2]), min_lag=2, max_lag=5)
    assert pd.isna(hurst)
