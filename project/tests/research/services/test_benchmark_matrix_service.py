from __future__ import annotations

import json
from pathlib import Path

from project.research.services.benchmark_matrix_service import (
    build_benchmark_summary,
    load_benchmark_matrix,
    write_benchmark_summary,
)


def test_load_benchmark_matrix_accepts_metadata_fields(tmp_path: Path) -> None:
    matrix_path = tmp_path / "matrix.yaml"
    matrix_path.write_text(
        "version: 2\n"
        "matrix_id: family_matrix\n"
        "runs:\n"
        "  - benchmark_id: stat_disloc_1\n"
        "    family: STATISTICAL_DISLOCATION\n"
        "    template: mean_reversion\n"
        "    context_label: vol_high\n"
        "    timeframe: 5m\n"
        "    run_id: r1\n"
        "    symbols: BTCUSDT\n"
        "    start: 2024-01-01\n"
        "    end: 2024-01-31\n",
        encoding="utf-8",
    )

    payload = load_benchmark_matrix(matrix_path)
    assert payload["matrix_id"] == "family_matrix"
    assert payload["runs"][0]["family"] == "STATISTICAL_DISLOCATION"


def test_build_and_write_benchmark_summary(tmp_path: Path) -> None:
    matrix = {
        "matrix_id": "family_matrix",
        "description": "unit test matrix",
        "runs": [
            {
                "benchmark_id": "stat_disloc_1",
                "family": "STATISTICAL_DISLOCATION",
                "template": "mean_reversion",
                "context_label": "vol_high",
                "run_id": "r1",
                "symbols": "BTCUSDT",
                "start": "2024-01-01",
                "end": "2024-01-31",
            },
            {
                "benchmark_id": "trend_1",
                "family": "TREND_STRUCTURE",
                "template": "continuation",
                "context_label": "trend_confirmed",
                "run_id": "r2",
                "symbols": "BTCUSDT",
                "start": "2024-02-01",
                "end": "2024-02-28",
            },
        ],
    }
    manifest = {
        "results": [
            {"run_id": "r1", "status": "success", "returncode": 0, "duration_sec": 1.0},
            {"run_id": "r2", "status": "dry_run", "returncode": None, "duration_sec": None},
        ]
    }

    summary = build_benchmark_summary(matrix=matrix, manifest=manifest)
    assert summary["status_counts"]["success"] == 1
    assert summary["status_counts"]["dry_run"] == 1
    assert summary["families"]["STATISTICAL_DISLOCATION"] == 1
    assert summary["templates"]["continuation"] == 1

    paths = write_benchmark_summary(out_dir=tmp_path / "out", summary=summary)
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["matrix_id"] == "family_matrix"
    assert paths["markdown"].exists()
