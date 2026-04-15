from __future__ import annotations

import json
import subprocess
from pathlib import Path

from project.scripts.run_certification_workflow import run_certification_workflow


def test_certification_workflow_runs_end_to_end(tmp_path: Path) -> None:
    payload = run_certification_workflow(
        root=tmp_path,
        config_path=Path("project/configs/golden_certification.yaml"),
    )
    certification_summary_path = tmp_path / "reliability" / "golden_certification_summary.json"
    certification_manifest_path = tmp_path / "reliability" / "runtime_certification_manifest.json"
    live_state_snapshot_path = tmp_path / "reliability" / "live_state.json"
    workflow_summary_path = tmp_path / "reliability" / "golden_workflow_summary.json"

    assert payload["workflow_id"] == "golden_certification_v1"
    assert certification_summary_path.exists()
    assert certification_manifest_path.exists()
    assert live_state_snapshot_path.exists()
    assert workflow_summary_path.exists()

    certification_summary = json.loads(certification_summary_path.read_text(encoding="utf-8"))
    certification_manifest = json.loads(certification_manifest_path.read_text(encoding="utf-8"))
    assert certification_summary["runtime_run_id"] == "smoke_run"
    assert certification_manifest["manifest_type"] == "runtime_certification_manifest"
    assert certification_manifest["status"] == "pass"
    assert certification_manifest["certification_checks"]["postflight_passed"]
    assert certification_manifest["certification_checks"]["feeds_healthy"]
    assert certification_manifest["certification_checks"]["live_state_snapshot_present"]
    assert certification_manifest["certification_checks"]["replay_digest_present"]
    assert certification_manifest["certification_checks"]["promotion_export_consistent"]
    assert certification_manifest["certification_checks"]["deployment_gate_passed"]
    assert certification_manifest["control_plane"]["promoted_rows"] == 0
    assert certification_manifest["control_plane"]["exported_thesis_count"] == 0
    assert certification_manifest["control_plane"]["store_thesis_count"] == 0
    assert certification_summary["live_state_snapshot_path"] == str(live_state_snapshot_path)
    assert certification_manifest["live_state"]["snapshot_path"] == str(live_state_snapshot_path)


def test_certification_workflow_persists_benchmark_status(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "golden_certification_benchmark.yaml"
    config_path.write_text(
        "\n".join(
            [
                "workflow_id: golden_certification_v1",
                "golden_workflow_config: project/configs/golden_workflow.yaml",
                "runtime_run_id: smoke_run",
                "stale_threshold_sec: 60.0",
                "freshness_streams:",
                "  - symbol: BTCUSDT",
                "    stream: kline_5m",
                "  - symbol: ETHUSDT",
                "    stream: kline_5m",
                "oms_lineage:",
                "  order_source: smoke_oms",
                "  session_id: golden-certification-session",
                "live_state_snapshot_path: reliability/live_state.json",
                "required_outputs:",
                "  - reliability/runtime_certification_manifest.json",
                "benchmark_matrix_path: spec/benchmarks/regime_shakeout_matrix.yaml",
                "enforce_benchmark_certification: true",
                "",
            ]
        ),
        encoding="utf-8",
    )

    real_run = subprocess.run

    def patched_run(cmd, *args, **kwargs):
        if (
            isinstance(cmd, (list, tuple))
            and len(cmd) >= 3
            and cmd[1] == "-m"
            and cmd[2] == "project.scripts.run_benchmark_matrix"
        ):
            class Result:
                returncode = 0

            return Result()
        return real_run(cmd, *args, **kwargs)

    monkeypatch.setattr(subprocess, "run", patched_run)

    payload = run_certification_workflow(root=tmp_path, config_path=config_path)
    certification_manifest_path = tmp_path / "reliability" / "runtime_certification_manifest.json"
    certification_manifest = json.loads(certification_manifest_path.read_text(encoding="utf-8"))

    assert payload["runtime_certification"]["benchmark_certification_passed"] is True
    assert certification_manifest["benchmark_certification_passed"] is True
