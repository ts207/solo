from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from project.research import governed_reproduction


def _write_run(
    data_root: Path,
    run_id: str,
    *,
    estimated: int = 1,
    valid_metrics: int = 1,
    bridge_rows: int = 1,
    event_count: int = 79,
    t_stat: float = 2.3,
    net_bps: float = 42.0,
    robustness: float = 0.83,
) -> None:
    phase2 = data_root / "reports" / "phase2" / run_id
    phase2.mkdir(parents=True)
    (phase2 / "phase2_diagnostics.json").write_text(
        json.dumps(
            {
                "valid_metrics_rows": valid_metrics,
                "bridge_candidates_rows": bridge_rows,
                "hypotheses_generated": estimated,
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "candidate_id": "cand",
                "event_type": "PRICE_DOWN_OI_DOWN",
                "symbol": "BTCUSDT",
                "n_events": event_count,
                "n": event_count,
                "t_stat_net": t_stat,
                "mean_return_net_bps": net_bps,
                "robustness_score": robustness,
                "gate_bridge_tradable": bridge_rows > 0,
            }
        ]
    ).to_parquet(phase2 / "phase2_candidates.parquet", index=False)
    plan = data_root / "artifacts" / "experiments" / "program" / run_id
    plan.mkdir(parents=True)
    (plan / "validated_plan.json").write_text(
        json.dumps({"program_id": "program", "estimated_hypothesis_count": estimated}),
        encoding="utf-8",
    )


def test_governed_reproduction_passes_current_governed_evidence(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_run(data_root, "source")
    _write_run(data_root, "repro")

    report = governed_reproduction.build_governed_reproduction_report(
        source_run_id="source",
        reproduction_run_id="repro",
        data_root=data_root,
    )

    assert report["schema_version"] == "governed_reproduction_v1"
    assert report["status"] == "pass"
    assert report["decision"] == "advance"
    assert report["source"]["event_count"] == 79
    assert report["reproduction"]["estimated_hypothesis_count"] == 1
    assert report["deltas"]["event_count_delta_pct"] == 0.0


def test_governed_reproduction_blocks_zero_metrics(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_run(data_root, "source")
    _write_run(data_root, "repro", valid_metrics=0)

    report = governed_reproduction.build_governed_reproduction_report(
        source_run_id="source",
        reproduction_run_id="repro",
        data_root=data_root,
    )

    assert report["status"] == "blocked"
    assert report["decision"] == "review"
    assert any(
        check["id"] == "valid_metrics_rows_positive" and check["status"] == "fail"
        for check in report["blocking_checks"]
    )


def test_governed_reproduction_fails_event_count_collapse(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_run(data_root, "source", event_count=100)
    _write_run(data_root, "repro", event_count=20)

    report = governed_reproduction.build_governed_reproduction_report(
        source_run_id="source",
        reproduction_run_id="repro",
        data_root=data_root,
    )

    assert report["status"] == "fail"
    assert report["decision"] == "kill"
    assert report["deltas"]["event_count_delta_pct"] == -80.0


def test_governed_reproduction_uses_results_context_for_review_reason(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_run(data_root, "run")
    results_dir = data_root / "reports" / "results"
    results_dir.mkdir(parents=True)
    (results_dir / "results_index.json").write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "run_id": "run",
                        "candidate_id": "cand",
                        "decision_reason": "year_split_pending",
                        "nearby_attempt_count": 28,
                        "manual_decision": True,
                    },
                    {
                        "run_id": "run",
                        "candidate_id": "cand",
                        "decision_reason": "year_split_pending",
                        "nearby_attempt_count": 13,
                        "manual_decision": True,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    report = governed_reproduction.build_governed_reproduction_report(
        source_run_id="run",
        reproduction_run_id="run",
        data_root=data_root,
    )

    assert report["status"] == "pass"
    assert report["decision"] == "review"
    assert "nearby_attempt_count=13" in report["reason"]
    assert "year_split_pending" in report["reason"]


def test_run_governed_reproduction_writes_report(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    _write_run(data_root, "run")

    report = governed_reproduction.run_governed_reproduction(
        source_run_id="run",
        reproduction_run_id="run",
        data_root=data_root,
    )

    path = governed_reproduction.report_path(data_root, "run")
    assert path.exists()
    assert json.loads(path.read_text(encoding="utf-8"))["status"] == report["status"]
