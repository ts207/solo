"""
Tests for the verification harness.
"""

from __future__ import annotations

import pandas as pd
import pytest
from project.eval.verification import VerificationHarness


def test_harness_init():
    harness = VerificationHarness()
    assert harness is not None


def test_compare_series_exact():
    harness = VerificationHarness()
    s1 = pd.Series([1.0, 2.0, 3.0])
    s2 = pd.Series([1.0, 2.0, 3.0])
    result = harness.compare_series(s1, s2)
    assert result["pass"] is True
    assert result["max_diff"] == 0.0


def test_compare_series_within_tolerance():
    harness = VerificationHarness(tolerance=1e-5)
    s1 = pd.Series([1.0, 2.0, 3.0])
    s2 = pd.Series([1.000001, 2.0, 3.0])
    result = harness.compare_series(s1, s2)
    assert result["pass"] is True
    assert result["max_diff"] < 1e-5


def test_compare_series_outside_tolerance():
    harness = VerificationHarness(tolerance=1e-7)
    s1 = pd.Series([1.0, 2.0, 3.0])
    s2 = pd.Series([1.1, 2.0, 3.0])
    result = harness.compare_series(s1, s2)
    assert result["pass"] is False
    assert result["max_diff"] > 1e-7
