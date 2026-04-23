from __future__ import annotations

import importlib.util
import json
import sys

from project.tests.conftest import PROJECT_ROOT


def _load_script_module():
    script_path = PROJECT_ROOT / "scripts" / "run_regime_shakeout_matrix.py"
    spec = importlib.util.spec_from_file_location("run_regime_shakeout_matrix", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_run_regime_shakeout_matrix_writes_manifest(tmp_path, monkeypatch):
    module = _load_script_module()
    out_dir = tmp_path / "out"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_regime_shakeout_matrix.py",
            "--matrix",
            str((PROJECT_ROOT.parent / "spec" / "benchmarks" / "regime_shakeout_matrix.yaml").resolve()),
            "--registry_root",
            str((PROJECT_ROOT / "configs" / "registries").resolve()),
            "--data_root",
            str((tmp_path / "data").resolve()),
            "--out_dir",
            str(out_dir),
            "--execute",
            "0",
            "--plan_only",
            "1",
        ],
    )

    rc = module.main()

    assert rc == 0
    manifest = out_dir / "regime_shakeout_manifest.json"
    assert manifest.exists()
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert payload["planned_runs"] == 88
    assert payload["failures"] == 0
    assert len(payload["results"]) == 88
    assert any(row["slice_type"] == "regime_first" for row in payload["results"])
    assert any(row["slice_type"] == "raw_control" for row in payload["results"])
    assert payload["results"][0]["validated_plan"]["estimated_hypothesis_count"] >= 0
