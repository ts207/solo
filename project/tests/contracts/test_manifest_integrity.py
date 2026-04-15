from __future__ import annotations

import json
from pathlib import Path

from project.reliability.manifest_checks import validate_manifest_core


def test_validate_manifest_core_for_engine_manifest(tmp_path: Path):
    payload = {
        "manifest_type": "engine_run_manifest",
        "manifest_version": "engine_run_manifest_v1",
        "run_id": "r1",
        "artifacts": [],
        "schemas": {},
        "metrics": {},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    out = validate_manifest_core(path)
    assert out["run_id"] == "r1"
