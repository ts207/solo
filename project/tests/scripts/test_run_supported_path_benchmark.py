from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

from project.tests.conftest import PROJECT_ROOT


def _load_runner_module():
    script_path = PROJECT_ROOT / "scripts" / "run_supported_path_benchmark.py"
    spec = importlib.util.spec_from_file_location("run_supported_path_benchmark", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_contract_failures_include_current_conformance_schema_violations(tmp_path: Path) -> None:
    module = _load_runner_module()
    run_id = "unit_run"
    run_dir = tmp_path / "runs" / run_id
    execution_dir = run_dir / "execution"
    execution_dir.mkdir(parents=True)
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "status": "success",
                "contract_conformance_status": "fail",
            }
        ),
        encoding="utf-8",
    )
    (execution_dir / "contract_conformance.json").write_text(
        json.dumps(
            {
                "artifact_results": [
                    {
                        "contract_id": "discovery_phase2_candidates",
                        "status": "schema_violation",
                    },
                    {
                        "contract_id": "run_manifest",
                        "status": "conformant",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    assert module._contract_failures(tmp_path, run_id) == [
        "contract_conformance_status:fail",
        "discovery_phase2_candidates:schema_violation",
    ]
