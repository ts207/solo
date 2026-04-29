from __future__ import annotations

import subprocess
import sys

from project.scripts import discover_doctor
from project.scripts.discover_doctor import build_discover_doctor_report


def test_discover_doctor_missing_run_blocks(tmp_path):
    report = build_discover_doctor_report(
        run_id="missing_run",
        data_root=tmp_path,
    )

    assert report["kind"] == "discover_doctor"
    assert report["run_id"] == "missing_run"
    assert report["status"] == "blocked"
    assert report["evidence_class"] == "blocked"
    assert report["classification"] in discover_doctor.BLOCKED_CLASSIFICATIONS
    assert report["next_safe_command"].startswith("make explain-empty RUN_ID=missing_run")
    assert report["requires"] == [
        "governed_reproduction",
        "candidate_autopsy",
        "year_split",
        "forward_confirmation",
    ]
    assert "edge validate run" in report["forbidden_actions"]
    assert "change_horizon" in report["forbidden_rescue_actions"]


def test_discover_doctor_cli_missing_run_exits_nonzero(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            "project/scripts/discover_doctor.py",
            "--run_id",
            "missing_run",
            "--data_root",
            str(tmp_path),
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert '"kind": "discover_doctor"' in result.stdout
    assert '"status": "blocked"' in result.stdout
    assert '"evidence_class": "blocked"' in result.stdout


def test_discover_doctor_zero_feasible_blocks_validation(monkeypatch, tmp_path):
    def fake_summary(**_kwargs):
        return {
            "run_id": "zero_feasible",
            "data_root": str(tmp_path),
            "counts": {"candidates_total": 1, "candidates_final": 0},
            "diagnostics": {
                "hypotheses_generated": 1,
                "feasible_hypotheses": 0,
                "valid_metrics_rows": 1,
            },
            "top_candidates": {"rows": [{"candidate_id": "cand"}]},
        }

    monkeypatch.setattr(discover_doctor, "build_discover_summary", fake_summary)

    report = build_discover_doctor_report(run_id="zero_feasible", data_root=tmp_path)

    assert report["status"] == "blocked"
    assert report["evidence_class"] == "blocked"
    assert report["classification"] == "zero_feasible_hypotheses"
    assert "edge validate run" in report["forbidden_actions"]


def test_discover_doctor_zero_metrics_blocks_validation(monkeypatch, tmp_path):
    def fake_summary(**_kwargs):
        return {
            "run_id": "zero_metrics",
            "data_root": str(tmp_path),
            "counts": {"candidates_total": 1, "candidates_final": 0},
            "diagnostics": {
                "hypotheses_generated": 1,
                "feasible_hypotheses": 1,
                "valid_metrics_rows": 0,
            },
            "top_candidates": {"rows": [{"candidate_id": "cand"}]},
        }

    monkeypatch.setattr(discover_doctor, "build_discover_summary", fake_summary)

    report = build_discover_doctor_report(run_id="zero_metrics", data_root=tmp_path)

    assert report["status"] == "blocked"
    assert report["classification"] == "zero_valid_metrics_rows"
    assert report["next_safe_command"].startswith("make explain-empty RUN_ID=zero_metrics")


def test_discover_doctor_bridge_candidates_are_validate_ready_not_edge(monkeypatch, tmp_path):
    def fake_summary(**_kwargs):
        return {
            "run_id": "bridge_run",
            "data_root": str(tmp_path),
            "counts": {"candidates_total": 3, "candidates_final": 1},
            "diagnostics": {
                "hypotheses_generated": 3,
                "feasible_hypotheses": 3,
                "valid_metrics_rows": 3,
                "bridge_candidates_rows": 1,
            },
            "top_candidates": {"rows": [{"candidate_id": "cand"}]},
        }

    monkeypatch.setattr(discover_doctor, "build_discover_summary", fake_summary)

    report = build_discover_doctor_report(run_id="bridge_run", data_root=tmp_path)

    assert report["status"] == "validate_ready"
    assert report["evidence_class"] == "validate_ready"
    assert report["classification"] == "bridge_candidates_present"
    assert report["next_safe_command"] == f"make validate RUN_ID=bridge_run DATA_ROOT={tmp_path}"
    assert "edge deploy live-run" in report["forbidden_actions"]
    assert "forward_confirmation" in report["requires"]


def test_discover_doctor_candidates_without_bridge_are_candidate_signal(monkeypatch, tmp_path):
    def fake_summary(**_kwargs):
        return {
            "run_id": "review_run",
            "data_root": str(tmp_path),
            "counts": {"candidates_total": 3, "candidates_final": 0},
            "diagnostics": {
                "hypotheses_generated": 3,
                "feasible_hypotheses": 3,
                "valid_metrics_rows": 3,
                "bridge_candidates_rows": 0,
            },
            "top_candidates": {"rows": [{"candidate_id": "cand"}]},
        }

    monkeypatch.setattr(discover_doctor, "build_discover_summary", fake_summary)

    report = build_discover_doctor_report(run_id="review_run", data_root=tmp_path)

    assert report["status"] == "review_candidate"
    assert report["evidence_class"] == "candidate_signal"
    assert report["classification"] == "candidates_present_but_no_final_bridge_survivors"
    assert report["next_safe_command"] == (
        "Review top_candidates and write a candidate autopsy before any validation decision."
    )
    assert "edge promote run" in report["forbidden_actions"]
