from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.research import year_split


def _write_candidate(data_root: Path, run_id: str) -> None:
    phase2 = data_root / "reports" / "phase2" / run_id
    phase2.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand",
                "hypothesis_id": "hyp",
                "t_stat_net": 2.1,
                "gate_bridge_tradable": True,
            }
        ]
    ).to_parquet(phase2 / "phase2_candidates.parquet", index=False)


def _write_events(data_root: Path, run_id: str, dates: list[str]) -> None:
    phase2 = data_root / "reports" / "phase2" / run_id
    phase2.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand",
                "hypothesis_id": "hyp",
                "event_timestamp": date,
                "return_net_bps": 1.0,
            }
            for date in dates
        ]
    ).to_parquet(phase2 / "phase2_candidate_event_timestamps.parquet", index=False)


def test_year_split_passes_when_no_year_exceeds_half_support(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_candidate(data_root, "run")
    _write_events(
        data_root,
        "run",
        [
            "2022-01-01T00:00:00Z",
            "2022-02-01T00:00:00Z",
            "2023-01-01T00:00:00Z",
            "2023-02-01T00:00:00Z",
            "2024-01-01T00:00:00Z",
        ],
    )

    report = year_split.build_year_split_report(run_id="run", data_root=data_root)

    assert report["schema_version"] == "year_split_v1"
    assert report["status"] == "pass"
    assert report["classification"] == "general_candidate"
    assert report["concentration"]["max_event_share"] == 0.4
    assert report["totals"]["event_count"] == 5


def test_year_split_marks_year_conditional_when_event_support_concentrates(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    _write_candidate(data_root, "run")
    _write_events(
        data_root,
        "run",
        [
            "2024-01-01T00:00:00Z",
            "2024-02-01T00:00:00Z",
            "2024-03-01T00:00:00Z",
            "2023-01-01T00:00:00Z",
        ],
    )

    report = year_split.build_year_split_report(run_id="run", data_root=data_root)

    assert report["status"] == "fail"
    assert report["classification"] == "year_conditional"
    assert report["decision"] == "park"
    assert report["reason"] == "year_pnl_concentration"
    assert report["concentration"]["max_event_share"] == 0.75


def test_year_split_blocks_missing_event_timestamps(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_candidate(data_root, "run")

    report = year_split.build_year_split_report(run_id="run", data_root=data_root)

    assert report["status"] == "blocked"
    assert report["classification"] == "unsupported"


def test_run_year_split_writes_candidate_report(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_candidate(data_root, "run")
    _write_events(data_root, "run", ["2022-01-01T00:00:00Z", "2023-01-01T00:00:00Z"])

    report = year_split.run_year_split(run_id="run", data_root=data_root)

    path = year_split.report_path(data_root, "run", report["candidate_id"])
    assert path.exists()
    assert json.loads(path.read_text(encoding="utf-8"))["status"] == "pass"
