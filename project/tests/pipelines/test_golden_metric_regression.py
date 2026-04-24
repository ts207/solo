"""Tests for metric-level golden regression checks (E2-T3)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from project.core.golden_regression import (
    GoldenToleranceConfig,
    collect_core_artifact_snapshot,
    compare_golden_snapshots,
    load_tolerance_config,
)

_TOLERANCE_YAML = Path(__file__).parent.parent / "fixtures" / "golden_tolerance_config.yaml"

_RUN_ID = "test-run-001"


def _write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _seed_artifacts(
    root: Path,
    run_id: str = _RUN_ID,
    net_expectancy_bps: float = 5.0,
) -> None:
    """Write all JSON files at the exact paths the catalog functions expect."""
    # run_manifest_path -> {root}/runs/{run_id}/run_manifest.json
    _write_json(
        root / "runs" / run_id / "run_manifest.json",
        {
            "status": "completed",
            "run_mode": "research",
            "objective_name": "test_objective",
            "retail_profile_name": "test_profile",
            "objective_spec_hash": "abc123",
            "retail_profile_spec_hash": "def456",
        },
    )

    # checklist_path -> {root}/runs/{run_id}/research_checklist/checklist.json
    _write_json(
        root / "runs" / run_id / "research_checklist" / "checklist.json",
        {"decision": "pass"},
    )

    # release_signoff_path -> {root}/runs/{run_id}/research_checklist/release_signoff.json
    _write_json(
        root / "runs" / run_id / "research_checklist" / "release_signoff.json",
        {
            "decision": "approved",
            "override_audit": {"non_production_override_count": 0},
        },
    )

    # kpi_scorecard_path -> {root}/runs/{run_id}/kpi_scorecard.json
    _write_json(
        root / "runs" / run_id / "kpi_scorecard.json",
        {
            "metrics": {
                "net_expectancy_bps": {"value": net_expectancy_bps},
                "oos_sign_consistency": {"value": 0.72},
                "max_drawdown_pct": {"value": 0.15},
                "trade_count": {"value": 1200},
                "turnover_proxy_mean": {"value": 0.03},
            }
        },
    )

    # promotion_summary_path -> {root}/reports/promotions/{run_id}/promotion_summary.json
    _write_json(
        root / "reports" / "promotions" / run_id / "promotion_summary.json",
        {
            "candidates_promoted_final": 10,
            "rejected_total": 5,
            "promotion_tier_counts": {
                "deployable": 3,
                "shadow": 4,
                "research": 3,
            },
        },
    )

    # blueprint_summary_path -> {root}/reports/strategy_blueprints/{run_id}/blueprint_summary.json
    _write_json(
        root / "reports" / "strategy_blueprints" / run_id / "blueprint_summary.json",
        {
            "blueprint_count": 10,
            "fallback_event_count": 0,
            "candidates_compiled": 10,
        },
    )


class TestGoldenMetricRegression:
    def test_identical_run_passes_regression(self, tmp_path: Path) -> None:
        """Same artifacts as golden baseline must pass all metric tolerance checks."""
        tolerance = load_tolerance_config(_TOLERANCE_YAML)

        _seed_artifacts(tmp_path, run_id=_RUN_ID)
        baseline = collect_core_artifact_snapshot(data_root=tmp_path, run_id=_RUN_ID)
        candidate = collect_core_artifact_snapshot(data_root=tmp_path, run_id=_RUN_ID)

        result = compare_golden_snapshots(
            baseline=baseline,
            candidate=candidate,
            tolerance=tolerance,
        )

        assert result["passed"] is True, f"Expected pass but got diffs: {result['diffs']}"

    def test_metric_drift_within_tolerance_passes(self, tmp_path: Path) -> None:
        """Metric drift within configured tolerance must pass."""
        tolerance = load_tolerance_config(_TOLERANCE_YAML)

        baseline_root = tmp_path / "baseline"
        candidate_root = tmp_path / "candidate"

        # baseline: net_expectancy_bps=5.0; candidate: 5.5 (delta=0.5 < 1.0 tolerance)
        _seed_artifacts(baseline_root, run_id=_RUN_ID, net_expectancy_bps=5.0)
        _seed_artifacts(candidate_root, run_id=_RUN_ID, net_expectancy_bps=5.5)

        baseline = collect_core_artifact_snapshot(data_root=baseline_root, run_id=_RUN_ID)
        candidate = collect_core_artifact_snapshot(data_root=candidate_root, run_id=_RUN_ID)

        result = compare_golden_snapshots(
            baseline=baseline,
            candidate=candidate,
            tolerance=tolerance,
        )

        assert result["passed"] is True, (
            f"Expected pass for 0.5 bps drift (tolerance=1.0) but got diffs: {result['diffs']}"
        )

    def test_metric_drift_outside_tolerance_fails(self, tmp_path: Path) -> None:
        """Metric drift beyond configured tolerance must fail the regression."""
        tolerance = load_tolerance_config(_TOLERANCE_YAML)

        baseline_root = tmp_path / "baseline"
        candidate_root = tmp_path / "candidate"

        # baseline: net_expectancy_bps=5.0; candidate: 8.0 (delta=3.0 > 1.0 tolerance)
        _seed_artifacts(baseline_root, run_id=_RUN_ID, net_expectancy_bps=5.0)
        _seed_artifacts(candidate_root, run_id=_RUN_ID, net_expectancy_bps=8.0)

        baseline = collect_core_artifact_snapshot(data_root=baseline_root, run_id=_RUN_ID)
        candidate = collect_core_artifact_snapshot(data_root=candidate_root, run_id=_RUN_ID)

        result = compare_golden_snapshots(
            baseline=baseline,
            candidate=candidate,
            tolerance=tolerance,
        )

        assert result["passed"] is False, (
            "Expected failure for 3.0 bps drift (tolerance=1.0) but test passed"
        )

        diff_metrics = [d["metric"] for d in result["diffs"]]
        assert any("net_expectancy_bps" in m for m in diff_metrics), (
            f"Expected 'net_expectancy_bps' in diffs but got: {diff_metrics}"
        )

    def test_tolerance_config_loads_correctly(self) -> None:
        """load_tolerance_config must parse the YAML and return a GoldenToleranceConfig."""
        assert _TOLERANCE_YAML.exists(), f"Tolerance YAML not found: {_TOLERANCE_YAML}"

        tolerance = load_tolerance_config(_TOLERANCE_YAML)

        assert isinstance(tolerance, GoldenToleranceConfig)
        assert "kpi_scorecard.net_expectancy_bps" in tolerance.per_metric_abs_tolerance
        assert (
            abs(tolerance.per_metric_abs_tolerance["kpi_scorecard.net_expectancy_bps"] - 1.0) < 1e-9
        ), (
            f"Expected 1.0 but got "
            f"{tolerance.per_metric_abs_tolerance['kpi_scorecard.net_expectancy_bps']}"
        )
