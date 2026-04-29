from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.research import specificity


def _write_candidate(data_root: Path, run_id: str) -> None:
    phase2 = data_root / "reports" / "phase2" / run_id
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
                "entry_lag_bars": 1,
                "n": 4,
                "t_stat_net": 2.3,
            }
        ]
    ).to_parquet(phase2 / "phase2_candidates.parquet", index=False)


def test_specificity_emits_review_when_trace_returns_are_missing(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_candidate(data_root, "run")
    pd.DataFrame(
        [
            {
                "candidate_id": "cand",
                "hypothesis_id": "hyp",
                "event_type": "PRICE_DOWN_OI_DOWN",
                "event_timestamp": "2024-01-01T00:00:00Z",
            }
        ]
    ).to_parquet(
        data_root / "reports" / "phase2" / "run" / "phase2_candidate_event_timestamps.parquet",
        index=False,
    )

    report = specificity.build_specificity_report(
        run_id="run",
        candidate_id="cand",
        data_root=data_root,
    )

    assert report["schema_version"] == "specificity_v1"
    assert report["status"] == "review"
    assert report["classification"] == "insufficient_trace_data"
    assert report["decision"] == "review"
    assert report["tests"]["base"]["event_count"] == 4
    assert report["tests"]["base"]["mean_return_net_bps"] is None
    assert report["specificity_lift"]["pass"] is None
    assert "aggregate candidate metrics only" in report["reason"]


def test_specificity_passes_when_base_beats_event_context_and_direction_controls(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    run_id = "run"
    _write_candidate(data_root, run_id)
    trace_path = data_root / "reports" / "phase2" / run_id / "edge_cell_pnl_traces.parquet"
    rows = []
    for test_name, returns in {
        "base": [10.0, 12.0, 8.0],
        "event_only": [2.0, 1.0, 3.0],
        "context_only": [1.0, 0.0, 2.0],
        "opposite_direction": [-10.0, -8.0, -12.0],
        "lag_0": [3.0, 4.0, 2.0],
        "lag_1": [10.0, 12.0, 8.0],
        "lag_2": [1.0, 2.0, 3.0],
        "lag_3": [0.0, 1.0, 2.0],
    }.items():
        for value in returns:
            rows.append({"specificity_test": test_name, "return_net_bps": value})
    pd.DataFrame(rows).to_parquet(trace_path, index=False)

    report = specificity.build_specificity_report(
        run_id=run_id,
        candidate_id="cand",
        data_root=data_root,
    )

    assert report["status"] == "pass"
    assert report["classification"] == "event_specific"
    assert report["decision"] == "advance"
    assert report["specificity_lift"]["base_vs_event_only_bps"] == 8.0
    assert report["specificity_lift"]["base_vs_context_only_bps"] == 9.0
    assert report["specificity_lift"]["pass"] is True
    assert [row["entry_lag_bars"] for row in report["tests"]["entry_lag_sensitivity"]] == [
        0,
        1,
        2,
        3,
    ]


def test_run_specificity_writes_candidate_report(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_candidate(data_root, "run")

    report = specificity.run_specificity(run_id="run", candidate_id="cand", data_root=data_root)

    path = specificity.report_path(data_root, "run", "cand")
    assert path.exists()
    assert json.loads(path.read_text(encoding="utf-8"))["classification"] == report["classification"]
