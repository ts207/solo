from __future__ import annotations

import json
from pathlib import Path

from project.pipelines import run_all_finalize


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_finalize_successful_run_refuses_success_when_required_outputs_are_missing(tmp_path):
    data_root = tmp_path / "data"
    run_id = "reconcile_missing_outputs"
    stage_instance_id = "build_features"
    missing_output = tmp_path / "missing_feature.parquet"

    _write_json(
        data_root / "runs" / run_id / "run_manifest.json",
        {
            "run_id": run_id,
            "status": "running",
            "planned_stage_instances": [stage_instance_id],
            "stage_timings_sec": {},
            "stage_instance_timings_sec": {},
        },
    )
    _write_json(
        data_root / "runs" / run_id / f"{stage_instance_id}.json",
        {
            "run_id": run_id,
            "stage": stage_instance_id,
            "stage_instance_id": stage_instance_id,
            "started_at": "2026-03-29T00:00:00+00:00",
            "finished_at": "2026-03-29T00:00:10+00:00",
            "status": "success",
            "parameters": {},
            "inputs": [],
            "outputs": [{"path": str(missing_output)}],
            "spec_hashes": {},
            "ontology_spec_hash": "sha256:abc",
        },
    )

    captured: dict[str, object] = {}

    def fake_finalize_run_manifest(run_manifest, status, **kwargs):
        captured["status"] = status
        captured["failed_stage"] = kwargs.get("failed_stage")
        captured["failed_stage_instance"] = kwargs.get("failed_stage_instance")
        run_manifest["status"] = status
        run_manifest["failed_stage"] = kwargs.get("failed_stage")
        run_manifest["failed_stage_instance"] = kwargs.get("failed_stage_instance")

    rc = run_all_finalize.finalize_successful_run(
        run_manifest={"run_id": run_id, "status": "running"},
        run_id=run_id,
        preflight={
            "emit_run_hash_requested": False,
            "research_compare_baseline_run_id": "",
        },
        stage_execution={
            "checklist_decision": None,
            "auto_continue_applied": False,
            "auto_continue_reason": "",
            "non_production_overrides": [],
        },
        stage_timings=[],
        stage_instance_timings=[],
        finalize_run_manifest=fake_finalize_run_manifest,
        apply_run_terminal_audit=lambda *_args, **_kwargs: None,
        maybe_emit_run_hash=lambda *_args, **_kwargs: None,
        write_run_manifest=lambda *_args, **_kwargs: None,
        write_run_kpi_scorecard=lambda *_args, **_kwargs: None,
        print_artifact_summary=lambda *_args, **_kwargs: None,
        data_root=data_root,
    )

    assert rc == 1
    assert captured["status"] == "failed"
    assert captured["failed_stage"] == "run_reconciliation"
    assert captured["failed_stage_instance"] == "run_reconciliation"
