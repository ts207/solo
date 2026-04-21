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


def test_benchmark_slice_skips_export_when_no_promotions(tmp_path: Path, monkeypatch) -> None:
    module = _load_runner_module()
    commands_seen: list[list[str]] = []

    def fake_run_command(command):
        commands_seen.append(command)
        return {"command": command, "returncode": 0, "stdout_tail": "", "stderr_tail": ""}

    def fake_promotion_metrics(_data_root, _run_id):
        return {"promotion_count": 0, "diagnostics_present": True}

    def fake_collect_metrics(_data_root, _run_id, _slice_spec, *, execute, runtime_max_rows):
        return {
            "candidate_counts": {"count": 0, "diagnostics_present": True},
            "validation": {
                "validated_count": 0,
                "promotion_ready_count": 0,
                "pass_rate": 0.0,
                "bundle_present": True,
                "report_present": True,
            },
            "promotion": {"promotion_count": 0, "diagnostics_present": True},
            "thesis_export": {
                "thesis_export_count": 0,
                "active_thesis_count": 0,
                "pending_thesis_count": 0,
                "present": False,
            },
            "runtime_events": {"count": 0, "status": "ok"},
            "portfolio_allocations": {"allocated_count": 0},
            "artifact_contract_failures": [],
        }

    monkeypatch.setattr(module, "_run_command", fake_run_command)
    monkeypatch.setattr(module, "_promotion_metrics", fake_promotion_metrics)
    monkeypatch.setattr(module, "_collect_metrics", fake_collect_metrics)

    result = module._benchmark_slice(
        slice_spec=module.SUITE[0],
        execute=True,
        run_prefix="unit",
        data_root=tmp_path,
        runtime_max_rows=10,
    )

    assert result["status"] == "completed"
    assert len(commands_seen) == 3
    assert result["commands"][3]["status"] == "skipped_no_promotions"
