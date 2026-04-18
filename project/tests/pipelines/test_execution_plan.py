from __future__ import annotations

import pytest
from project.pipelines.execution_plan import (
    PlannedStage,
    ExecutionPlan,
    ExecutionVerificationReport,
    StageVerificationResult,
    build_execution_plan,
    verify_execution,
)


def test_planned_stage_is_active_only_when_selected():
    active = PlannedStage(stage_name="build_features_5m", script_path="p.py", reason_code="selected")
    skipped = PlannedStage(stage_name="ingest_ohlcv_1h", script_path="p.py", reason_code="skipped")
    assert active.is_active
    assert not skipped.is_active


def test_execution_plan_active_and_skipped_partition():
    plan = build_execution_plan(
        run_id="r1",
        planned_at="2026-04-18T00:00:00Z",
        stage_specs=[
            ("stage_a", "a.py", [], "selected"),
            ("stage_b", "b.py", [], "skipped"),
            ("stage_c", "c.py", [], "selected"),
        ],
        run_mode="research",
        symbols=["BTCUSDT"],
    )
    assert plan.run_id == "r1"
    assert len(plan.active_stages) == 2
    assert len(plan.skipped_stages) == 1
    assert plan.skipped_stages[0].stage_name == "stage_b"


def test_execution_plan_explain_returns_string():
    plan = build_execution_plan(
        run_id="r1",
        planned_at="2026-04-18T00:00:00Z",
        stage_specs=[("stage_a", "a.py", ["--run_id", "r1"], "selected")],
    )
    explanation = plan.explain()
    assert "r1" in explanation
    assert "stage_a" in explanation


def test_verify_execution_success():
    plan = build_execution_plan(
        run_id="r1",
        planned_at="2026-04-18T00:00:00Z",
        stage_specs=[
            ("stage_a", "a.py", [], "selected"),
            ("stage_b", "b.py", [], "skipped"),
        ],
    )
    manifest = {
        "status": "success",
        "stage_timings_sec": {"stage_a": 1.5},
        "failed_stage": None,
    }
    report = verify_execution(plan, manifest, verified_at="2026-04-18T01:00:00Z")
    assert report.final_status == "success"
    assert report.passed
    assert len(report.mismatches) == 0


def test_verify_execution_detects_failed_stage():
    plan = build_execution_plan(
        run_id="r1",
        planned_at="2026-04-18T00:00:00Z",
        stage_specs=[
            ("stage_a", "a.py", [], "selected"),
            ("stage_b", "b.py", [], "selected"),
        ],
    )
    manifest = {
        "status": "failed",
        "stage_timings_sec": {"stage_a": 1.0, "stage_b": 0.5},
        "failed_stage": "stage_b",
    }
    report = verify_execution(plan, manifest, verified_at="2026-04-18T01:00:00Z")
    assert report.final_status == "failed"
    assert not report.passed
    assert any(r.stage_name == "stage_b" and r.actual_outcome == "failure" for r in report.mismatches)


def test_pipelines_init_exports_execution_plan():
    import project.pipelines as pipelines
    assert hasattr(pipelines, "ExecutionPlan")
    assert hasattr(pipelines, "ExecutionVerificationReport")
    assert hasattr(pipelines, "build_execution_plan")
    assert hasattr(pipelines, "verify_execution")
