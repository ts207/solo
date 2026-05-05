from __future__ import annotations

from pathlib import Path

from project.research.run_diagnostics import build_rejection_explanation, build_run_status_report


def test_run_status_reports_not_started_for_missing_artifacts(tmp_path: Path) -> None:
    report = build_run_status_report(run_id="missing", data_root=tmp_path)

    assert report["stage"] == "not_started"
    assert report["next_safe_command"] == "Run data preflight before discovery."


def test_rejection_explanation_handles_missing_artifacts(tmp_path: Path) -> None:
    report = build_rejection_explanation(run_id="missing", data_root=tmp_path)

    assert report["primary_rejection"] == "no_artifacts_found"
    assert report["failure_class"] == "mechanical"
