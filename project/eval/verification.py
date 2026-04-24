"""
Base verification harness for feature and detection logic.
"""

from __future__ import annotations

from typing import Any, Dict

import numpy as np
import pandas as pd


class VerificationHarness:
    def __init__(self, tolerance: float = 1e-6):
        self.tolerance = tolerance

    def compare_series(self, actual: pd.Series, expected: pd.Series) -> Dict[str, Any]:
        """
        Compare two series and return results.
        """
        if actual.shape != expected.shape:
            return {
                "pass": False,
                "reason": f"Shape mismatch: {actual.shape} vs {expected.shape}",
                "max_diff": np.nan,
            }

        diff = (actual - expected).abs()
        max_diff = float(diff.max())
        is_pass = bool(max_diff <= self.tolerance)

        return {
            "pass": is_pass,
            "max_diff": max_diff,
            "mean_diff": float(diff.mean()),
            "std_diff": float(diff.std()) if len(diff) > 1 else 0.0,
        }
