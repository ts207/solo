from __future__ import annotations

import json
from pathlib import Path

from project.pipelines import run_all
from project.pipelines.pipeline_provenance import write_run_manifest


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_run_all_preserves_terminal_status_when_writing_execution_reports(tmp_path, monkeypatch):
    data_root = tmp_path / "data"
    run_id = "run_success"
    manifest_path = data_root / "runs" / run_id / "run_manifest.json"
    _write(manifest_path, {"run_id": run_id, "status": "success", "terminal_status": "completed"})

    stale_manifest = {"run_id": run_id, "status": "running"}
    execution_report_paths = {"contract_conformance_json": "x"}

    monkeypatch.setattr(run_all, "DATA_ROOT", data_root)

    latest_manifest = run_all.read_run_manifest(run_id, data_root=data_root) or dict(stale_manifest)
    latest_manifest["execution_report_paths"] = execution_report_paths
    latest_manifest["contract_conformance_status"] = "pass"
    latest_manifest["contract_conformance_stage_mismatch_count"] = 0
    latest_manifest["contract_conformance_artifact_mismatch_count"] = 0
    write_run_manifest(run_id, latest_manifest, data_root=data_root)

    saved = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert saved["status"] == "success"
    assert saved["terminal_status"] == "completed"
    assert saved["contract_conformance_status"] == "pass"
