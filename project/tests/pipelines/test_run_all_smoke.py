from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

from project.pipelines import run_all
from project.tests.conftest import REPO_ROOT

_REPO_ROOT = str(REPO_ROOT)


def _env_with_pythonpath(data_root: Path) -> dict:
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{_REPO_ROOT}:{existing}" if existing else _REPO_ROOT
    env["BACKTEST_DATA_ROOT"] = str(data_root)
    return env


def test_run_all_plan_only(tmp_path: Path):
    """Verify that run_all.py --plan_only 1 works without execution."""
    data_root = tmp_path / "data"
    cmd = [
        sys.executable,
        "-m",
        "project.pipelines.run_all",
        "--run_id",
        "smoke_test_plan",
        "--symbols",
        "BTCUSDT",
        "--start",
        "2024-01-01",
        "--end",
        "2024-01-02",
        "--plan_only",
        "1",
    ]
    result = subprocess.run(
        cmd, capture_output=True, text=True, cwd=_REPO_ROOT, env=_env_with_pythonpath(data_root)
    )
    assert result.returncode == 0, result.stderr
    assert "Plan for run smoke_test_plan" in result.stdout
    assert "Effective behavior:" in result.stdout
    assert "phase2_event_type=VOL_SHOCK" in result.stdout
    assert "expectancy_tail=analysis:True robustness:True checklist:True" in result.stdout


def test_run_all_plan_only_shows_template_only_event_widening(tmp_path: Path):
    data_root = tmp_path / "data"
    cmd = [
        sys.executable,
        "-m",
        "project.pipelines.run_all",
        "--run_id",
        "smoke_test_plan_templates",
        "--symbols",
        "BTCUSDT",
        "--start",
        "2024-01-01",
        "--end",
        "2024-01-02",
        "--templates",
        "continuation",
        "--plan_only",
        "1",
    ]
    result = subprocess.run(
        cmd, capture_output=True, text=True, cwd=_REPO_ROOT, env=_env_with_pythonpath(data_root)
    )

    assert result.returncode == 0, result.stderr
    assert "phase2_event_type=all (template_only_auto_widen)" in result.stdout


def test_run_all_dry_run(tmp_path: Path):
    """Verify that run_all.py --dry_run 1 initializes manifest but does not execute."""
    data_root = tmp_path / "data"
    run_id = f"smoke_test_dry_{uuid.uuid4().hex[:8]}"
    cmd = [
        sys.executable,
        "-m",
        "project.pipelines.run_all",
        "--run_id",
        run_id,
        "--symbols",
        "BTCUSDT",
        "--start",
        "2024-01-01",
        "--end",
        "2024-01-02",
        "--dry_run",
        "1",
    ]
    result = subprocess.run(
        cmd, capture_output=True, text=True, cwd=_REPO_ROOT, env=_env_with_pythonpath(data_root)
    )
    assert result.returncode == 0, result.stderr
    assert f"Dry run for {run_id} completed" in result.stdout
    manifest_path = data_root / "runs" / run_id / "run_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["status"] == "success"
    assert payload["dry_run"] is True
    assert payload["normalized_symbols"] == ["BTCUSDT"]
    assert payload["normalized_timeframes"] == ["5m"]
    assert payload["effective_behavior"]["phase2_event_type"] == "VOL_SHOCK"
    assert payload["effective_behavior"]["run_expectancy_analysis"] is True


def test_export_runtime_mode_env_sets_certification_flags(monkeypatch):
    monkeypatch.delenv("BACKTEST_STRICT_RUN_SCOPED_READS", raising=False)
    monkeypatch.delenv("BACKTEST_REQUIRE_STAGE_MANIFEST", raising=False)

    run_all._export_runtime_mode_env(
        {
            "strict_run_scoped_reads": True,
            "require_stage_manifests": True,
        }
    )
    assert os.environ["BACKTEST_STRICT_RUN_SCOPED_READS"] == "1"
    assert os.environ["BACKTEST_REQUIRE_STAGE_MANIFEST"] == "1"

    run_all._export_runtime_mode_env(
        {
            "strict_run_scoped_reads": False,
            "require_stage_manifests": False,
        }
    )
    assert os.environ["BACKTEST_STRICT_RUN_SCOPED_READS"] == "0"
    assert os.environ["BACKTEST_REQUIRE_STAGE_MANIFEST"] == "0"
