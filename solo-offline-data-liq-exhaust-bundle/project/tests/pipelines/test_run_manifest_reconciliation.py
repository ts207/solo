from __future__ import annotations

import json
from pathlib import Path

import project.pipelines.pipeline_provenance as provenance


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_reconcile_run_manifest_from_stage_manifests_marks_completed_manual_replay_success(
    tmp_path, monkeypatch
):
    data_root = tmp_path / "data"
    monkeypatch.setattr(provenance, "_get_data_root", lambda: data_root)

    run_id = "manual_replay_run"
    run_manifest = {
        "run_id": run_id,
        "status": "failed",
        "failed_stage": "promote_candidates",
        "failed_stage_instance": "promote_candidates",
        "planned_stage_instances": [
            "export_edge_candidates",
            "promote_candidates",
            "update_edge_registry",
        ],
        "stage_timings_sec": {},
        "stage_instance_timings_sec": {},
    }
    _write_json(data_root / "runs" / run_id / "run_manifest.json", run_manifest)

    _write_json(
        data_root / "runs" / run_id / "export_edge_candidates.json",
        {
            "stage": "export_edge_candidates",
            "status": "success",
            "started_at": "2026-03-11T03:00:00+00:00",
            "finished_at": "2026-03-11T03:00:10+00:00",
            "outputs": [],
        },
    )
    _write_json(
        data_root / "runs" / run_id / "promote_candidates.json",
        {
            "stage": "promote_candidates",
            "status": "success",
            "started_at": "2026-03-11T03:00:10+00:00",
            "finished_at": "2026-03-11T03:00:20+00:00",
            "outputs": [],
        },
    )
    _write_json(
        data_root / "runs" / run_id / "update_edge_registry.json",
        {
            "stage": "update_edge_registry",
            "status": "success",
            "started_at": "2026-03-11T03:00:20+00:00",
            "finished_at": "2026-03-11T03:00:30+00:00",
            "outputs": [],
        },
    )

    reconciled = provenance.reconcile_run_manifest_from_stage_manifests(run_id)

    assert reconciled["status"] == "success"
    assert reconciled["failed_stage"] is None
    assert reconciled["failed_stage_instance"] is None
    assert reconciled["finished_at"] == "2026-03-11T03:00:30+00:00"
    assert "update_edge_registry" in reconciled["stage_instance_timings_sec"]


def test_reconcile_run_manifest_from_stage_manifests_keeps_failed_when_planned_stage_missing(
    tmp_path, monkeypatch
):
    data_root = tmp_path / "data"
    monkeypatch.setattr(provenance, "_get_data_root", lambda: data_root)

    run_id = "incomplete_manual_replay_run"
    run_manifest = {
        "run_id": run_id,
        "status": "failed",
        "failed_stage": "promote_candidates",
        "failed_stage_instance": "promote_candidates",
        "planned_stage_instances": [
            "export_edge_candidates",
            "promote_candidates",
            "update_edge_registry",
        ],
        "stage_timings_sec": {},
        "stage_instance_timings_sec": {},
    }
    _write_json(data_root / "runs" / run_id / "run_manifest.json", run_manifest)
    _write_json(
        data_root / "runs" / run_id / "export_edge_candidates.json",
        {
            "stage": "export_edge_candidates",
            "status": "success",
            "started_at": "2026-03-11T03:00:00+00:00",
            "finished_at": "2026-03-11T03:00:10+00:00",
        },
    )
    _write_json(
        data_root / "runs" / run_id / "promote_candidates.json",
        {
            "stage": "promote_candidates",
            "status": "success",
            "started_at": "2026-03-11T03:00:10+00:00",
            "finished_at": "2026-03-11T03:00:20+00:00",
        },
    )

    reconciled = provenance.reconcile_run_manifest_from_stage_manifests(run_id)

    assert reconciled["status"] == "failed"
    assert reconciled["failed_stage"] == "promote_candidates"


def test_reconcile_run_manifest_from_stage_manifests_treats_warning_as_terminal(
    tmp_path, monkeypatch
):
    data_root = tmp_path / "data"
    monkeypatch.setattr(provenance, "_get_data_root", lambda: data_root)

    run_id = "warning_terminal_run"
    run_manifest = {
        "run_id": run_id,
        "status": "failed",
        "failed_stage": "promote_candidates",
        "failed_stage_instance": "promote_candidates",
        "planned_stage_instances": ["validate_feature_integrity_5m", "promote_candidates"],
        "stage_timings_sec": {},
        "stage_instance_timings_sec": {},
    }
    _write_json(data_root / "runs" / run_id / "run_manifest.json", run_manifest)
    _write_json(
        data_root / "runs" / run_id / "validate_feature_integrity_5m.json",
        {
            "stage": "validate_feature_integrity_5m",
            "status": "warning",
            "started_at": "2026-03-11T03:00:00+00:00",
            "finished_at": "2026-03-11T03:00:05+00:00",
        },
    )
    _write_json(
        data_root / "runs" / run_id / "promote_candidates.json",
        {
            "stage": "promote_candidates",
            "status": "success",
            "started_at": "2026-03-11T03:00:05+00:00",
            "finished_at": "2026-03-11T03:00:10+00:00",
            "outputs": [],
        },
    )

    reconciled = provenance.reconcile_run_manifest_from_stage_manifests(run_id)

    assert reconciled["status"] == "success"
    assert reconciled["failed_stage"] is None


def test_reconcile_run_manifest_from_stage_manifests_loads_checklist_decision(
    tmp_path, monkeypatch
):
    data_root = tmp_path / "data"
    monkeypatch.setattr(provenance, "_get_data_root", lambda: data_root)

    run_id = "warning_checklist_run"
    _write_json(
        data_root / "runs" / run_id / "run_manifest.json",
        {
            "run_id": run_id,
            "status": "failed",
            "failed_stage": "generate_recommendations_checklist",
            "failed_stage_instance": "generate_recommendations_checklist",
            "planned_stage_instances": ["generate_recommendations_checklist"],
            "stage_timings_sec": {},
            "stage_instance_timings_sec": {},
            "checklist_decision": None,
        },
    )
    _write_json(
        data_root / "runs" / run_id / "generate_recommendations_checklist.json",
        {
            "stage": "generate_recommendations_checklist",
            "status": "warning",
            "started_at": "2026-03-11T03:00:00+00:00",
            "finished_at": "2026-03-11T03:00:05+00:00",
        },
    )
    _write_json(
        data_root / "runs" / run_id / "research_checklist" / "checklist.json",
        {"decision": "KEEP_RESEARCH"},
    )

    reconciled = provenance.reconcile_run_manifest_from_stage_manifests(run_id)

    assert reconciled["status"] == "success"
    assert reconciled["checklist_decision"] == "KEEP_RESEARCH"


def test_reconcile_run_manifest_respects_explicit_data_root(tmp_path, monkeypatch):
    monkeypatch.setattr(provenance, "_get_data_root", lambda: tmp_path / "wrong_data")
    data_root = tmp_path / "data"
    run_id = "explicit_root_run"

    _write_json(
        data_root / "runs" / run_id / "run_manifest.json",
        {
            "run_id": run_id,
            "status": "failed",
            "failed_stage": "promote_candidates",
            "failed_stage_instance": "promote_candidates",
            "planned_stage_instances": ["promote_candidates"],
            "stage_timings_sec": {},
            "stage_instance_timings_sec": {},
        },
    )
    _write_json(
        data_root / "runs" / run_id / "promote_candidates.json",
        {
            "stage": "promote_candidates",
            "status": "success",
            "started_at": "2026-03-11T03:00:10+00:00",
            "finished_at": "2026-03-11T03:00:20+00:00",
            "outputs": [],
        },
    )

    reconciled = provenance.reconcile_run_manifest_from_stage_manifests(run_id, data_root=data_root)

    assert reconciled["status"] == "success"
    persisted = json.loads(
        (data_root / "runs" / run_id / "run_manifest.json").read_text(encoding="utf-8")
    )
    assert persisted["status"] == "success"
    assert not (tmp_path / "wrong_data" / "runs" / run_id / "run_manifest.json").exists()
