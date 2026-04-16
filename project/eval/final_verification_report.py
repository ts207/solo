"""
Final verification report generator.
Combines feature and detection verification results.
"""

from __future__ import annotations

import pandas as pd
from project.eval.feature_verification_suite import run_feature_verification
from project.eval.detection_verification_suite import run_detection_verification


def generate_final_verification_report() -> pd.DataFrame:
    """
    Execute all verification suites and combine results.
    """
    f_report = run_feature_verification()
    f_report["type"] = "Feature"

    d_report = run_detection_verification()
    d_report["type"] = "Detection"

    final_report = pd.concat([f_report, d_report], ignore_index=True)
    return final_report


if __name__ == "__main__":
    report = generate_final_verification_report()
    print("# Final Verification Report")
    print(report.to_markdown(index=False))
