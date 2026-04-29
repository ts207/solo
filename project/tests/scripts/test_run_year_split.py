from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.scripts import run_year_split


def test_run_year_split_script_writes_report(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    phase2 = data_root / "reports" / "phase2" / "run"
    phase2.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand",
                "hypothesis_id": "hyp",
                "t_stat_net": 1.2,
                "gate_bridge_tradable": True,
            }
        ]
    ).to_parquet(phase2 / "phase2_candidates.parquet", index=False)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand",
                "hypothesis_id": "hyp",
                "event_timestamp": "2022-01-01T00:00:00Z",
            },
            {
                "candidate_id": "cand",
                "hypothesis_id": "hyp",
                "event_timestamp": "2023-01-01T00:00:00Z",
            },
        ]
    ).to_parquet(phase2 / "phase2_candidate_event_timestamps.parquet", index=False)

    exit_code = run_year_split.main(
        [
            "--run-id",
            "run",
            "--data-root",
            str(data_root),
        ]
    )

    report_path = data_root / "reports" / "regime" / "run" / "cand_year_split.json"
    assert exit_code == 0
    assert report_path.exists()
    assert json.loads(report_path.read_text(encoding="utf-8"))["classification"] == (
        "general_candidate"
    )
