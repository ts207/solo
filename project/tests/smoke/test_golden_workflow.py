from __future__ import annotations

import json
from pathlib import Path

from project.scripts.run_golden_workflow import run_golden_workflow


def test_golden_workflow_runs_end_to_end(tmp_path: Path) -> None:
    payload = run_golden_workflow(
        root=tmp_path,
        config_path=Path("project/configs/golden_workflow.yaml"),
    )
    summary_path = tmp_path / "reliability" / "golden_workflow_summary.json"
    smoke_summary_path = tmp_path / "reliability" / "smoke_summary.json"

    assert payload["workflow_id"] == "golden_workflow_v1"
    assert summary_path.exists()
    assert smoke_summary_path.exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["summary"]["mode"] == "full"
    assert "research" in summary["summary"]
    assert "promotion" in summary["summary"]
    assert "engine" in summary["summary"]
