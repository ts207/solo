from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

from project.core.golden_regression import (
    GoldenToleranceConfig,
    collect_core_artifact_snapshot,
    compare_golden_snapshots,
)
from project.tests.conftest import PROJECT_ROOT


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _seed_run_artifacts(data_root: Path, run_id: str) -> None:
    _write_json(
        data_root / "runs" / run_id / "run_manifest.json",
        {
            "status": "success",
            "run_mode": "research",
            "objective_name": "retail_profitability",
            "retail_profile_name": "capital_constrained",
            "objective_spec_hash": "sha_obj",
            "retail_profile_spec_hash": "sha_profile",
        },
    )
    _write_json(
        data_root / "reports" / "promotions" / run_id / "promotion_summary.json",
        {
            "candidates_promoted_final": 3,
            "rejected_total": 2,
            "promotion_tier_counts": {"deployable": 1, "shadow": 2, "research": 2},
        },
    )
    _write_json(
        data_root / "reports" / "strategy_blueprints" / run_id / "blueprint_summary.json",
        {
            "blueprint_count": 4,
            "fallback_event_count": 1,
            "candidates_compiled": 4,
        },
    )
    _write_json(
        data_root / "runs" / run_id / "research_checklist" / "checklist.json",
        {"decision": "PROMOTE"},
    )
    _write_json(
        data_root / "runs" / run_id / "research_checklist" / "release_signoff.json",
        {"decision": "APPROVE_RELEASE", "override_audit": {"non_production_override_count": 0}},
    )
    _write_json(
        data_root / "runs" / run_id / "kpi_scorecard.json",
        {
            "metrics": {
                "net_expectancy_bps": {"value": 5.0},
                "oos_sign_consistency": {"value": 0.71},
                "max_drawdown_pct": {"value": -0.13},
                "trade_count": {"value": 240},
                "turnover_proxy_mean": {"value": 2.1},
            }
        },
    )


def _load_script_module():
    script_path = PROJECT_ROOT / "scripts" / "run_golden_regression.py"
    spec = importlib.util.spec_from_file_location("run_golden_regression", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_collect_core_artifact_snapshot_reads_expected_fields(tmp_path):
    run_id = "golden_run_1"
    data_root = tmp_path / "data"
    _seed_run_artifacts(data_root, run_id)

    snapshot = collect_core_artifact_snapshot(data_root=data_root, run_id=run_id)

    assert snapshot["run_manifest"]["status"] == "success"
    assert snapshot["promotion_summary"]["candidates_promoted_final"] == 3
    assert snapshot["promotion_summary"]["tier_counts"]["deployable"] == 1
    assert snapshot["blueprint_summary"]["blueprint_count"] == 4
    assert snapshot["checklist"]["decision"] == "PROMOTE"
    assert snapshot["release_signoff"]["decision"] == "APPROVE_RELEASE"
    assert snapshot["kpi_scorecard"]["net_expectancy_bps"] == 5.0


def test_compare_golden_snapshots_honors_tolerance():
    baseline = {"kpi_scorecard": {"net_expectancy_bps": 5.0, "trade_count": 100}}
    candidate = {"kpi_scorecard": {"net_expectancy_bps": 5.2, "trade_count": 102}}
    tolerance = GoldenToleranceConfig(
        default_numeric_abs_tolerance=0.0,
        per_metric_abs_tolerance={
            "kpi_scorecard.net_expectancy_bps": 0.25,
            "kpi_scorecard.trade_count": 5.0,
        },
    )
    report = compare_golden_snapshots(
        baseline=baseline,
        candidate=candidate,
        tolerance=tolerance,
    )
    assert report["passed"] is True
    assert report["diff_count"] == 0

    strict = GoldenToleranceConfig(
        default_numeric_abs_tolerance=0.0,
        per_metric_abs_tolerance={},
    )
    strict_report = compare_golden_snapshots(
        baseline=baseline,
        candidate=candidate,
        tolerance=strict,
    )
    assert strict_report["passed"] is False
    assert strict_report["diff_count"] >= 1


def test_run_golden_regression_script_compare_mode_fails_on_drift(tmp_path, monkeypatch):
    module = _load_script_module()
    run_id = "golden_run_2"
    data_root = tmp_path / "data"
    _seed_run_artifacts(data_root, run_id)

    baseline_snapshot = tmp_path / "baseline_snapshot.json"
    baseline_snapshot.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "kpi_scorecard": {"net_expectancy_bps": 10.0},
                "run_manifest": {"status": "success"},
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    tolerance_path = tmp_path / "tolerances.yaml"
    tolerance_path.write_text(
        "version: 1\ndefaults:\n  numeric_abs_tolerance: 0.0\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "reports" / "golden_regression" / run_id

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_golden_regression.py",
            "--run_id",
            run_id,
            "--data_root",
            str(data_root),
            "--baseline_snapshot",
            str(baseline_snapshot),
            "--tolerance_spec",
            str(tolerance_path),
            "--out_dir",
            str(out_dir),
        ],
    )
    rc = module.main()
    assert rc == 1

    report_path = out_dir / "regression_report.json"
    assert report_path.exists()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["mode"] == "compare"
    assert report["passed"] is False
    assert report["diff_count"] >= 1
