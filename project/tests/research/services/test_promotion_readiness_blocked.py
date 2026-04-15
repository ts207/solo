from __future__ import annotations

import json
from pathlib import Path
from project.research.services.promotion_readiness_service import build_promotion_readiness_report


def test_promotion_readiness_catches_blocked_family():
    benchmark_review = {
        "matrix_id": "test_matrix",
        "slices": [
            {
                "benchmark_id": "stat_disloc_zscore_stretch_live",
                "family": "STATISTICAL_DISLOCATION",
                "benchmark_status": "informative",
                "live_foundation_readiness": "ready",
            }
        ],
    }

    benchmark_certification = {
        "passed": False,
        "issues": [
            {
                "benchmark_id": "stat_disloc_zscore_stretch_live",
                "severity": "fail",
                "type": "sample_collapse",
                "message": "Slice stat_disloc_zscore_stretch_live hard_evaluated_rows dropped by >20%",
            }
        ],
    }

    report = build_promotion_readiness_report(
        benchmark_review=benchmark_review, benchmark_certification=benchmark_certification
    )

    assert report["overall_passed"] is False
    assert report["family_health"]["STATISTICAL_DISLOCATION"]["healthy"] is False
    assert "stat_disloc_zscore_stretch_live" in report["rerun_priority"]
    assert "Benchmark certification FAILED." in report["blockers"]

    print("Promotion readiness successfully caught blocked family.")


if __name__ == "__main__":
    test_promotion_readiness_catches_blocked_family()
