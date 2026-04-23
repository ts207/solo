from __future__ import annotations

import json
import sqlite3
import sys

from project.scripts.build_run_registry import main, query_top_promoted


def test_build_run_registry_uses_catalog_helpers(tmp_path, monkeypatch):
    data_root = tmp_path / "data"
    run_dir = data_root / "runs" / "run_x"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "run_x",
                "status": "success",
                "stage": "promote_blueprints",
                "started_at": "2026-01-01T00:00:00",
                "stats": {"survivors_count": 7, "tested_count": 11},
            }
        ),
        encoding="utf-8",
    )
    db_path = data_root / "meta" / "runs.sqlite"
    monkeypatch.setattr(sys, "argv", ["build_run_registry", "--data_root", str(data_root)])
    rc = main()
    assert rc == 0
    conn = sqlite3.connect(db_path)
    try:
        rows = query_top_promoted(conn, limit=5)
    finally:
        conn.close()
    assert rows[0]["run_id"] == "run_x"
    assert rows[0]["survivors_count"] == 7
