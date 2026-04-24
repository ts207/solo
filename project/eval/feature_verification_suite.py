"""
Feature logic verification suite.
Generates reports on the correctness of computed features.
"""

from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
import pandas as pd

from project.core.stats import _kurtosis, _skew
from project.eval.reference_values import (
    get_reference_volatility,
    get_synthetic_test_data,
)
from project.eval.verification import VerificationHarness
from project.pipelines.features.build_features import _safe_logret_1


class FeatureVerificationSuite:
    def __init__(self, tolerance: float = 1e-6):
        self.harness = VerificationHarness(tolerance=tolerance)
        self.results: List[Dict[str, Any]] = []

    def verify_logret(self, data: pd.Series):
        """Verify log returns."""
        actual = _safe_logret_1(data)
        expected = np.log(data / data.shift(1))

        res = self.harness.compare_series(actual, expected)
        res["feature"] = "logret_1"
        self.results.append(res)
        return res

    def verify_volatility(self, data: pd.Series, window: int = 96):
        """Verify rolling volatility."""
        logret = _safe_logret_1(data)
        actual = logret.rolling(window=window, min_periods=window).std()
        expected = get_reference_volatility(logret, window=window)

        # Align
        common_index = expected.dropna().index
        res = self.harness.compare_series(actual.loc[common_index], expected.loc[common_index])
        res["feature"] = f"rv_{window}"
        self.results.append(res)
        return res

    def verify_skew(self, data: pd.Series):
        """Verify skewness."""
        from scipy.stats import skew as scipy_skew

        actual = _skew(data)
        expected = float(scipy_skew(data.dropna()))

        is_pass = bool(abs(actual - expected) <= self.harness.tolerance)
        res = {"pass": is_pass, "max_diff": abs(actual - expected), "feature": "skew"}
        self.results.append(res)
        return res

    def verify_kurtosis(self, data: pd.Series):
        """Verify kurtosis."""
        from scipy.stats import kurtosis as scipy_kurtosis

        actual = _kurtosis(data)
        expected = float(scipy_kurtosis(data.dropna(), fisher=True, bias=False))

        is_pass = bool(abs(actual - expected) <= self.harness.tolerance)
        res = {"pass": is_pass, "max_diff": abs(actual - expected), "feature": "kurtosis"}
        self.results.append(res)
        return res

    def verify_determinism(self, data: pd.Series):
        """Verify that computation is deterministic."""
        # Simple check: run logret twice
        res1 = _safe_logret_1(data)
        res2 = _safe_logret_1(data)

        is_pass = res1.equals(res2)
        self.results.append(
            {
                "pass": is_pass,
                "feature": "determinism",
                "reason": "Repeated runs produced identical output"
                if is_pass
                else "Non-deterministic output detected",
            }
        )
        return is_pass

    def get_report(self) -> pd.DataFrame:
        """Return a summary report of all verifications."""
        return pd.DataFrame(self.results)


def run_feature_verification() -> pd.DataFrame:
    """Run the standard feature verification suite."""
    suite = FeatureVerificationSuite()
    data = get_synthetic_test_data(n=500)

    suite.verify_logret(data)
    suite.verify_volatility(data, window=96)
    suite.verify_skew(data)
    suite.verify_kurtosis(data)
    suite.verify_determinism(data)

    return suite.get_report()
