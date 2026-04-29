from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.scripts import run_specificity


def test_run_specificity_script_writes_review_report_for_missing_trace_returns(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    phase2 = data_root / "reports" / "phase2" / "run"
    phase2.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand",
                "hypothesis_id": "hyp",
                "event_type": "PRICE_DOWN_OI_DOWN",
                "context_signature": "{'VOL_REGIME': 'HIGH'}",
                "rule_template": "mean_reversion",
                "direction": "long",
                "horizon": "24b",
                "n": 79,
                "t_stat_net": 2.3456,
            }
        ]
    ).to_parquet(phase2 / "phase2_candidates.parquet", index=False)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand",
                "hypothesis_id": "hyp",
                "event_type": "PRICE_DOWN_OI_DOWN",
                "event_timestamp": "2022-01-01T00:00:00Z",
            }
        ]
    ).to_parquet(phase2 / "phase2_candidate_event_timestamps.parquet", index=False)

    exit_code = run_specificity.main(
        [
            "--run-id",
            "run",
            "--candidate-id",
            "cand",
            "--data-root",
            str(data_root),
        ]
    )

    report_path = data_root / "reports" / "specificity" / "run" / "cand_specificity.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert payload["status"] == "review"
    assert payload["classification"] == "insufficient_trace_data"
    assert payload["next_safe_command"] == (
        "Implement candidate trace extraction before promotion or validation."
    )
