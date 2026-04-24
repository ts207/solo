from __future__ import annotations

import json
import shutil

from project.core.config import get_data_root


def test_maintenance_cycle_finds_multiple_priors():
    data_root = get_data_root()
    bench_dir = data_root / "reports" / "benchmarks"
    history_dir = bench_dir / "history"

    # Setup mock history
    if history_dir.exists():
        shutil.rmtree(history_dir)
    history_dir.mkdir(parents=True)

    matrix_id = "research_family_v1"
    (history_dir / f"{matrix_id}_20260318_000001").mkdir()
    (history_dir / f"{matrix_id}_20260318_000001" / "benchmark_review.json").write_text(
        json.dumps({"matrix_id": matrix_id, "slices": []})
    )
    (history_dir / f"{matrix_id}_20260318_000001" / "benchmark_certification.json").write_text(
        json.dumps({"passed": True})
    )

    (history_dir / f"{matrix_id}_20260318_000002").mkdir()
    (history_dir / f"{matrix_id}_20260318_000002" / "benchmark_review.json").write_text(
        json.dumps({"matrix_id": matrix_id, "slices": []})
    )
    (history_dir / f"{matrix_id}_20260318_000002" / "benchmark_certification.json").write_text(
        json.dumps({"passed": True})
    )

    # We need to modify run_benchmark_maintenance_cycle.py first to even have something to test against.
    # The plan says "Update the dry-run test to verify that the maintenance cycle correctly identifies and loads up to 5 historical baselines"
    # I'll implement the code and then verify it with a script that imports it.

    from project.research.benchmarks.benchmark_utils import find_historical_reviews

    priors = find_historical_reviews(matrix_id=matrix_id, history_limit=5)
    assert len(priors) == 2
    print("Successfully found multiple historical priors.")


if __name__ == "__main__":
    test_maintenance_cycle_finds_multiple_priors()
