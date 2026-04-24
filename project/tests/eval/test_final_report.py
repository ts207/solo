"""
Tests for final verification report.
"""

from __future__ import annotations

import pandas as pd

from project.eval.final_verification_report import generate_final_verification_report


def test_final_verification_report_generation():
    report = generate_final_verification_report()
    assert isinstance(report, pd.DataFrame)
    assert len(report) >= 8  # 5 from features, 3 from detection
    assert all(report["pass"])
    assert "type" in report.columns
