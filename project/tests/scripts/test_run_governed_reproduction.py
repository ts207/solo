from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.scripts import run_governed_reproduction


def _write_run(data_root: Path, run_id: str) -> None:
    phase2 = data_root / "reports" / "phase2" / run_id
    phase2.mkdir(parents=True)
    (phase2 / "phase2_diagnostics.json").write_text(
        json.dumps(
            {
                "valid_metrics_rows": 1,
                "bridge_candidates_rows": 1,
                "hypotheses_generated": 1,
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "candidate_id": "cand",
                "n_events": 7,
                "t_stat_net": 1.5,
                "mean_return_net_bps": 3.2,
                "gate_bridge_tradable": True,
            }
        ]
    ).to_parquet(phase2 / "phase2_candidates.parquet", index=False)


def test_run_governed_reproduction_script_writes_report(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_run(data_root, "run")

    exit_code = run_governed_reproduction.main(
        [
            "--source-run-id",
            "run",
            "--reproduction-run-id",
            "run",
            "--data-root",
            str(data_root),
        ]
    )

    report_path = data_root / "reports" / "reproduction" / "run" / "governed_reproduction.json"
    assert exit_code == 0
    assert report_path.exists()
    assert json.loads(report_path.read_text(encoding="utf-8"))["status"] == "pass"
