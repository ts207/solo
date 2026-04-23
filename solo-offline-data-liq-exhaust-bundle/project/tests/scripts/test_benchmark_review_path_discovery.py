from __future__ import annotations

import json
from pathlib import Path

from project.scripts.show_benchmark_review import find_latest_review, find_historical_reviews


def _write_review(root: Path, root_name: str, *, matrix_id: str, run_name: str) -> Path:
    review_dir = root / "reports" / root_name / "history" / f"{matrix_id}_{run_name}"
    review_dir.mkdir(parents=True, exist_ok=True)
    review_path = review_dir / "benchmark_review.json"
    review_path.write_text(json.dumps({"matrix_id": matrix_id, "slices": []}), encoding="utf-8")
    return review_path


def test_find_latest_review_prefers_canonical_benchmarks_root(tmp_path):
    data_root = tmp_path / "data"
    canonical = _write_review(data_root, "benchmarks", matrix_id="research_family_v1", run_name="20260318_000002")
    legacy = _write_review(
        data_root, "perf_benchmarks", matrix_id="research_family_v1", run_name="20260318_000003"
    )

    assert find_latest_review(data_root) == canonical
    assert legacy.exists()


def test_find_latest_review_falls_back_to_legacy_benchmarks_root(tmp_path):
    data_root = tmp_path / "data"
    legacy = _write_review(
        data_root, "perf_benchmarks", matrix_id="research_family_v1", run_name="20260318_000003"
    )

    assert find_latest_review(data_root) == legacy


def test_find_historical_reviews_reads_canonical_history_before_legacy(tmp_path, monkeypatch):
    data_root = tmp_path / "data"
    canonical_old = _write_review(
        data_root, "benchmarks", matrix_id="research_family_v1", run_name="20260318_000001"
    )
    canonical_new = _write_review(
        data_root, "benchmarks", matrix_id="research_family_v1", run_name="20260318_000002"
    )
    _write_review(data_root, "perf_benchmarks", matrix_id="research_family_v1", run_name="20260318_000003")

    monkeypatch.setattr("project.scripts.show_benchmark_review.get_data_root", lambda: data_root)
    reviews = find_historical_reviews("research_family_v1", limit=5)
    assert reviews[:2] == [canonical_new, canonical_old]
