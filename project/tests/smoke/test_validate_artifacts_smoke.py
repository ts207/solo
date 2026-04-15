from __future__ import annotations

from pathlib import Path

from project.reliability.cli_smoke import run_smoke_cli


def test_validate_artifacts_smoke_after_full_run(tmp_path: Path):
    full_summary = run_smoke_cli("full", root=tmp_path, storage_mode="auto")
    assert "engine" in full_summary and "promotion" in full_summary and "research" in full_summary
    validate_summary = run_smoke_cli("validate-artifacts", root=tmp_path)
    assert validate_summary["structural"]["stage_registry_issues"] == 0
    assert validate_summary["promotion"]["bundle_rows"] >= 1
    assert validate_summary["engine"]["trace_count"] >= 1
