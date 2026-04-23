from __future__ import annotations

import json
from pathlib import Path

import pytest

from project.core.exceptions import DataIntegrityError
from project.research.services.benchmark_review_service import (
    build_benchmark_review,
    classify_benchmark_slice,
    write_benchmark_review,
)


def test_classify_benchmark_slice_distinguishes_review_states(tmp_path: Path) -> None:
    foundation = tmp_path / "live.json"
    foundation.write_text(json.dumps({"readiness": "warn"}), encoding="utf-8")
    comparison = tmp_path / "comparison.json"
    comparison.write_text(
        json.dumps(
            {
                "hard_label": {
                    "evaluated_rows": 6,
                    "selected": {"hypothesis_id": "h1", "valid": True},
                },
                "confidence_aware": {
                    "evaluated_rows": 6,
                    "selected": {"hypothesis_id": "h1", "valid": False},
                },
                "selection_changed": False,
                "selection_outcome_changed": True,
            }
        ),
        encoding="utf-8",
    )

    status = classify_benchmark_slice(
        generated_reports={
            "live_foundation": str(foundation),
            "context_mode_comparison": str(comparison),
        }
    )
    assert status == "quality_boundary"


def test_build_and_write_benchmark_review(tmp_path: Path) -> None:
    foundation = tmp_path / "live.json"
    foundation.write_text(json.dumps({"readiness": "warn"}), encoding="utf-8")
    comparison = tmp_path / "comparison.json"
    comparison.write_text(
        json.dumps(
            {
                "hard_label": {
                    "evaluated_rows": 6,
                    "selected": {"hypothesis_id": "h1", "valid": True},
                },
                "confidence_aware": {
                    "evaluated_rows": 6,
                    "selected": {"hypothesis_id": "h2", "valid": True},
                },
                "selection_changed": True,
                "selection_outcome_changed": False,
            }
        ),
        encoding="utf-8",
    )

    summary = {
        "matrix_id": "family_matrix",
        "slices": [
            {
                "benchmark_id": "vol_1",
                "run_id": "r1",
                "family": "VOLATILITY_TRANSITION",
                "event_type": "VOL_SHOCK",
                "template": "continuation",
                "context_label": "vol_high",
                "status": "success",
                "generated_reports": {
                    "live_foundation": str(foundation),
                    "context_mode_comparison": str(comparison),
                },
            }
        ],
    }

    review = build_benchmark_review(summary=summary)
    assert review["status_counts"]["authoritative"] == 1
    assert review["slices"][0]["selected_hypothesis_hard"] == "h1"
    assert review["slices"][0]["selected_hypothesis_confidence_aware"] == "h2"

    paths = write_benchmark_review(out_dir=tmp_path / "out", review=review)
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["schema_version"] == "benchmark_review_v1"
    assert paths["markdown"].exists()


def test_classify_benchmark_slice_raises_on_malformed_report_json(tmp_path: Path) -> None:
    foundation = tmp_path / "live.json"
    foundation.write_text("{", encoding="utf-8")

    with pytest.raises(DataIntegrityError, match="Failed to read benchmark review json artifact"):
        classify_benchmark_slice(
            generated_reports={
                "live_foundation": str(foundation),
            }
        )


def test_build_benchmark_review_preserves_benchmark_metadata() -> None:
    summary = {
        "matrix_id": "family_matrix",
        "slices": [
            {
                "benchmark_id": "m0_strong_event_D",
                "run_id": "m0_strong_event_D",
                "family": "m0_strong_event",
                "template": "D",
                "context_label": "hierarchical_v2_with_folds",
                "status": "success",
                "generated_reports": {},
            }
        ],
    }

    review = build_benchmark_review(summary=summary)
    row = review["slices"][0]
    assert row["benchmark_id"] == "m0_strong_event_D"
    assert row["family"] == "m0_strong_event"
    assert row["template"] == "D"
    assert row["context_label"] == "hierarchical_v2_with_folds"
