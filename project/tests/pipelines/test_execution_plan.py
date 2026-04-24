from __future__ import annotations

from pathlib import Path

from project.pipelines.execution_plan import (
    ExecutionPlan,
    PlannedArtifactObligation,
    PlannedStage,
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


def test_verify_execution_checks_artifact_obligations(tmp_path: Path):
    plan = ExecutionPlan(
        run_id="r1",
        planned_at="2026-04-18T00:00:00Z",
        stages=(
            PlannedStage(
                stage_name="phase2_search_engine",
                script_path="research/phase2_search_engine.py",
                reason_code="selected",
                stage_family="phase2_discovery",
            ),
        ),
        artifact_obligations=(
            PlannedArtifactObligation(
                contract_id="discovery_phase2_candidates",
                producer_stage_family="phase2_discovery",
                schema_id="phase2_candidates",
                schema_version="phase2_candidates_v1",
                strictness="strict",
                required=True,
                expected_path="reports/phase2/r1/phase2_candidates.parquet",
            ),
        ),
    )
    artifact_path = tmp_path / "reports" / "phase2" / "r1" / "phase2_candidates.parquet"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    import pandas as pd
    pd.DataFrame({
        "candidate_id": ["c1"], "hypothesis_id": ["h1"], "event_type": ["E"],
        "symbol": ["BTCUSDT"], "run_id": ["r1"],
    }).to_parquet(artifact_path, index=False)
    manifest = {
        "status": "success",
        "stage_timings_sec": {"phase2_search_engine": 1.0},
        "failed_stage": None,
    }

    report = verify_execution(
        plan,
        manifest,
        verified_at="2026-04-18T01:00:00Z",
        data_root=tmp_path,
    )

    assert report.passed
    assert report.artifact_results[0].status == "conformant"


def test_verify_execution_schema_violation_for_bad_parquet(tmp_path: Path):
    """A parquet that exists but is missing required columns yields schema_violation, not conformant."""
    plan = ExecutionPlan(
        run_id="r2",
        planned_at="2026-04-18T00:00:00Z",
        stages=(
            PlannedStage(
                stage_name="phase2_search_engine",
                script_path="research/phase2_search_engine.py",
                reason_code="selected",
                stage_family="phase2_discovery",
            ),
        ),
        artifact_obligations=(
            PlannedArtifactObligation(
                contract_id="discovery_phase2_candidates",
                producer_stage_family="phase2_discovery",
                schema_id="phase2_candidates",
                schema_version="phase2_candidates_v1",
                strictness="strict",
                required=True,
                expected_path="reports/phase2/r2/phase2_candidates.parquet",
            ),
        ),
    )
    artifact_path = tmp_path / "reports" / "phase2" / "r2" / "phase2_candidates.parquet"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    import pandas as pd
    # Parquet with wrong columns — missing all required fields
    pd.DataFrame({"unrelated_col": [1, 2]}).to_parquet(artifact_path, index=False)
    manifest = {
        "status": "success",
        "stage_timings_sec": {"phase2_search_engine": 1.0},
        "failed_stage": None,
    }

    report = verify_execution(plan, manifest, verified_at="2026-04-18T01:00:00Z", data_root=tmp_path)

    assert not report.passed
    assert report.artifact_results[0].status == "schema_violation"
    assert "candidate_id" in report.artifact_results[0].notes


def test_verify_execution_schema_violation_for_bad_json(tmp_path: Path):
    """A JSON payload missing required fields yields schema_violation, not conformant."""
    plan = ExecutionPlan(
        run_id="r3",
        planned_at="2026-04-18T00:00:00Z",
        stages=(
            PlannedStage(
                stage_name="promote",
                script_path="research/promote.py",
                reason_code="selected",
                stage_family="promotion",
            ),
        ),
        artifact_obligations=(
            PlannedArtifactObligation(
                contract_id="promoted_theses",
                producer_stage_family="promotion",
                schema_id="promoted_theses_payload",
                schema_version="promoted_theses_v1",
                strictness="strict",
                required=True,
                expected_path="live/theses/r3/promoted_theses.json",
            ),
        ),
    )
    artifact_path = tmp_path / "live" / "theses" / "r3" / "promoted_theses.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    import json
    artifact_path.write_text(json.dumps({"run_id": "r3"}), encoding="utf-8")  # missing most fields
    manifest = {
        "status": "success",
        "stage_timings_sec": {"promote": 1.0},
        "failed_stage": None,
    }

    report = verify_execution(plan, manifest, verified_at="2026-04-18T01:00:00Z", data_root=tmp_path)

    assert not report.passed
    assert report.artifact_results[0].status == "schema_violation"
    assert "schema_version" in report.artifact_results[0].notes


def test_pipelines_init_exports_execution_plan():
    import project.pipelines as pipelines
    assert hasattr(pipelines, "ExecutionPlan")
    assert hasattr(pipelines, "ExecutionVerificationReport")
    assert hasattr(pipelines, "build_execution_plan")
    assert hasattr(pipelines, "verify_execution")
