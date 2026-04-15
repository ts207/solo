from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict


def _db_path(data_root: Path) -> Path:
    return Path(data_root) / "runs" / "experiment_store.sqlite"


def _connect(data_root: Path) -> sqlite3.Connection:
    db_path = _db_path(data_root)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def ensure_schema(data_root: Path) -> None:
    conn = _connect(data_root)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                status TEXT,
                started_at TEXT,
                finished_at TEXT,
                objective_name TEXT,
                retail_profile_name TEXT,
                run_mode TEXT,
                payload_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stage_manifests (
                run_id TEXT NOT NULL,
                stage_instance_id TEXT NOT NULL,
                stage_name TEXT,
                status TEXT,
                started_at TEXT,
                finished_at TEXT,
                manifest_path TEXT,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (run_id, stage_instance_id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stage_status ON stage_manifests(status)")
        conn.commit()
    finally:
        conn.close()


def upsert_run_manifest(data_root: Path, run_id: str, payload: Dict[str, Any]) -> None:
    ensure_schema(data_root)
    conn = _connect(data_root)
    try:
        run_id_token = str(run_id).strip()
        status = str(payload.get("status", "")).strip()
        started_at = str(payload.get("started_at", "")).strip()
        finished_at = str(payload.get("finished_at", "")).strip()
        objective_name = str(payload.get("objective_name", "")).strip()
        retail_profile_name = str(payload.get("retail_profile_name", "")).strip()
        run_mode = str(payload.get("run_mode", "")).strip()
        payload_json = json.dumps(payload, sort_keys=True)
        conn.execute(
            """
            INSERT INTO runs (
                run_id, status, started_at, finished_at, objective_name,
                retail_profile_name, run_mode, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                status=excluded.status,
                started_at=excluded.started_at,
                finished_at=excluded.finished_at,
                objective_name=excluded.objective_name,
                retail_profile_name=excluded.retail_profile_name,
                run_mode=excluded.run_mode,
                payload_json=excluded.payload_json
            """,
            (
                run_id_token,
                status,
                started_at,
                finished_at,
                objective_name,
                retail_profile_name,
                run_mode,
                payload_json,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_stage_manifest(
    data_root: Path,
    run_id: str,
    stage_instance_id: str,
    manifest_path: Path,
    payload: Dict[str, Any],
) -> None:
    ensure_schema(data_root)
    conn = _connect(data_root)
    try:
        stage_name = str(payload.get("stage", payload.get("stage_name", ""))).strip()
        status = str(payload.get("status", "")).strip()
        started_at = str(payload.get("started_at", "")).strip()
        finished_at = str(payload.get("finished_at", payload.get("ended_at", ""))).strip()
        payload_json = json.dumps(payload, sort_keys=True)
        conn.execute(
            """
            INSERT INTO stage_manifests (
                run_id, stage_instance_id, stage_name, status,
                started_at, finished_at, manifest_path, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, stage_instance_id) DO UPDATE SET
                stage_name=excluded.stage_name,
                status=excluded.status,
                started_at=excluded.started_at,
                finished_at=excluded.finished_at,
                manifest_path=excluded.manifest_path,
                payload_json=excluded.payload_json
            """,
            (
                str(run_id).strip(),
                str(stage_instance_id).strip(),
                stage_name,
                status,
                started_at,
                finished_at,
                str(manifest_path),
                payload_json,
            ),
        )
        conn.commit()
    finally:
        conn.close()
