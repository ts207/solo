from __future__ import annotations

import pytest
from project.research.services.benchmark_governance_service import certify_benchmark_review


def test_certify_with_multiple_priors():
    current_review = {
        "matrix_id": "test_matrix",
        "slices": [
            {"benchmark_id": "s1", "benchmark_status": "informative", "hard_evaluated_rows": 100}
        ],
    }

    priors = [
        {
            "matrix_id": "test_matrix",
            "slices": [
                {
                    "benchmark_id": "s1",
                    "benchmark_status": "informative",
                    "hard_evaluated_rows": 110,
                }
            ],
        },
        {
            "matrix_id": "test_matrix",
            "slices": [
                {
                    "benchmark_id": "s1",
                    "benchmark_status": "informative",
                    "hard_evaluated_rows": 120,
                }
            ],
        },
    ]

    # This should FAIL because the current implementation expects Optional[Dict], not List[Dict]
    cert = certify_benchmark_review(current_review=current_review, prior_review=priors)

    assert "historical_drift" in cert
    assert len(cert["historical_drift"]) > 0
