from __future__ import annotations

import json
from pathlib import Path
from project.research.services.benchmark_governance_service import certify_benchmark_review


def test_governance_catches_regressions():
    current_review = {
        "matrix_id": "test_matrix",
        "slices": [
            {
                "benchmark_id": "stat_disloc_zscore_stretch_live",
                "benchmark_status": "foundation_only",  # Regression: informative -> foundation_only
                "live_foundation_readiness": "blocked",  # Regression: warn -> blocked
                "hard_evaluated_rows": 2,  # Regression: 6 -> 2 (collapse)
                "confidence_evaluated_rows": 2,
            }
        ],
    }

    prior_review = {
        "matrix_id": "test_matrix",
        "slices": [
            {
                "benchmark_id": "stat_disloc_zscore_stretch_live",
                "benchmark_status": "informative",
                "live_foundation_readiness": "warn",
                "hard_evaluated_rows": 6,
                "confidence_evaluated_rows": 6,
            }
        ],
    }

    thresholds = {
        "stat_disloc_zscore_stretch_live": {
            "min_evaluated_rows": 5,
            "required_status": "informative",
            "required_foundation": "warn",
        }
    }

    cert = certify_benchmark_review(
        current_review=current_review, prior_review=prior_review, acceptance_thresholds=thresholds
    )

    assert cert["passed"] is False

    types = [i["type"] for i in cert["issues"]]
    assert "low_status" in types
    assert "low_foundation" in types
    assert "low_sample" in types
    assert "status_regression" in types
    assert "sample_collapse" in types

    print("Certification successfully caught all regressions.")


def test_governance_fails_on_execution_failures():
    current_review = {
        "matrix_id": "test_matrix",
        "slices": [
            {
                "benchmark_id": "slice_1",
                "benchmark_status": "informative",
                "live_foundation_readiness": "ready",
                "hard_evaluated_rows": 10,
                "confidence_evaluated_rows": 10,
            }
        ],
    }

    execution_manifest = {
        "matrix_id": "test_matrix",
        "failures": 2,
        "results": [
            {"run_id": "run_a", "status": "failed"},
            {"run_id": "run_b", "status": "failed"},
        ],
    }

    cert = certify_benchmark_review(
        current_review=current_review,
        execution_manifest=execution_manifest,
    )

    assert cert["passed"] is False
    issue = next(i for i in cert["issues"] if i["type"] == "execution_failures")
    assert issue["benchmark_id"] == "__matrix__"
    assert issue["failed_run_ids"] == ["run_a", "run_b"]


if __name__ == "__main__":
    test_governance_catches_regressions()
