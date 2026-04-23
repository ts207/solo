"""
Verification tests for determinism.
"""

from __future__ import annotations

import pandas as pd
import pytest
from project.eval.feature_verification_suite import FeatureVerificationSuite
from project.eval.reference_values import get_synthetic_test_data


def test_feature_computation_determinism():
    """Verify that repeated feature computation on the same input produces identical output."""
    data = get_synthetic_test_data(n=200)

    suite1 = FeatureVerificationSuite()
    suite1.verify_logret(data)
    suite1.verify_volatility(data)
    report1 = suite1.get_report()

    suite2 = FeatureVerificationSuite()
    suite2.verify_logret(data)
    suite2.verify_volatility(data)
    report2 = suite2.get_report()

    pd.testing.assert_frame_equal(report1, report2)
