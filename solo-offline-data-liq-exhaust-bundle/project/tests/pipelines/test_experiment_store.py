from __future__ import annotations

import json
import sqlite3

from project.io.experiment_store import upsert_run_manifest, upsert_stage_manifest


def test_experiment_store_upserts_run_and_stage_manifest(tmp_path):
    data_root = tmp_path / "data"
    run_id = "r1"
    stage_instance_id = "build_features"
    run_payload = {
        "run_id": run_id,
        "status": "running",
        "started_at": "2026-01-01T00:00:00Z",
        "finished_at": "",
        "objective_name": "retail_profitability",
        "retail_profile_name": "capital_constrained",
        "run_mode": "research",
    }
    upsert_run_manifest(data_root, run_id, run_payload)

    stage_manifest_path = data_root / "runs" / run_id / f"{stage_instance_id}.json"
    stage_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    stage_payload = {
        "run_id": run_id,
        "stage": "build_features",
        "status": "success",
        "started_at": "2026-01-01T00:00:01Z",
        "finished_at": "2026-01-01T00:00:02Z",
    }
    stage_manifest_path.write_text(json.dumps(stage_payload), encoding="utf-8")
    upsert_stage_manifest(
        data_root=data_root,
        run_id=run_id,
        stage_instance_id=stage_instance_id,
        manifest_path=stage_manifest_path,
        payload=stage_payload,
    )

    db_path = data_root / "runs" / "experiment_store.sqlite"
    assert db_path.exists()
    conn = sqlite3.connect(str(db_path))
    try:
        run_row = conn.execute(
            "SELECT run_id, status, objective_name FROM runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        assert run_row == (run_id, "running", "retail_profitability")
        stage_row = conn.execute(
            "SELECT run_id, stage_instance_id, status FROM stage_manifests WHERE run_id=?",
            (run_id,),
        ).fetchone()
        assert stage_row == (run_id, stage_instance_id, "success")
    finally:
        conn.close()
