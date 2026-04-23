from __future__ import annotations

import os
from pathlib import Path

from project.pipelines import pipeline_execution
from project.pipelines.pipeline_planning import build_contract_backed_execution_plan
from project.pipelines.planner import StageDefinition


def test_run_stage_does_not_mutate_parent_stage_env(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_engine_run_stage(**kwargs):
        captured.update(kwargs)
        return True

    monkeypatch.setattr(pipeline_execution, "_engine_run_stage", _fake_engine_run_stage)
    monkeypatch.delenv("BACKTEST_RUN_ID", raising=False)
    monkeypatch.delenv("BACKTEST_STAGE_INSTANCE_ID", raising=False)

    ok = pipeline_execution.run_stage(
        "build_features",
        Path("fake_stage.py"),
        ["--symbols", "BTCUSDT"],
        "run_env_isolation",
        stage_instance_id="build_features__worker_a",
    )

    assert ok is True
    assert "BACKTEST_RUN_ID" not in os.environ
    assert "BACKTEST_STAGE_INSTANCE_ID" not in os.environ
    assert captured["current_stage_instance_id"] == "build_features__worker_a"


def test_execute_pipeline_stages_returns_checklist_decision(monkeypatch):
    def _fake_run_dag(**kwargs):
        return True, [("stage_a", "stage_a", 1.25, {})]

    monkeypatch.setattr(pipeline_execution, "run_dag", _fake_run_dag)

    stage_execution = pipeline_execution.execute_pipeline_stages(
        args=type("Args", (), {"max_analyzer_workers": 1, "strict_recommendations_checklist": 0})(),
        run_id="run_checklist",
        stages={"stage_a": object()},
        planned_stage_instances=["stage_a"],
        resume_from_index=0,
        execution_requested=True,
        run_manifest={},
        stage_timings=[],
        stage_instance_timings=[],
        write_run_manifest=lambda *_args, **_kwargs: None,
        write_run_kpi_scorecard=lambda *_args, **_kwargs: None,
        apply_run_terminal_audit=lambda *_args, **_kwargs: None,
        load_checklist_decision=lambda _run_id: "KEEP_RESEARCH",
        last_stage_cache_meta={},
        feature_schema_version="feature_schema_v2",
        current_pipeline_session_id="pipeline_session_id",
        run_stage_fn=lambda **_kwargs: True,
    )

    assert stage_execution["status"] == "ok"
    assert stage_execution["checklist_decision"] == "KEEP_RESEARCH"


def test_build_contract_backed_execution_plan_derives_stage_families_and_obligations():
    args = type(
        "Args",
        (),
        {
            "mode": "research",
            "symbols": "BTCUSDT",
            "timeframes": "5m",
            "experiment_config": "",
            "registry_root": "project/configs/registries",
        },
    )()
    stages = {
        "phase2_search_engine": StageDefinition(
            name="phase2_search_engine",
            script_path=Path("research/phase2_search_engine.py"),
            args=["--run_id", "r1"],
        ),
        "promote_candidates": StageDefinition(
            name="promote_candidates",
            script_path=Path("research/cli/promotion_cli.py"),
            args=["--run_id", "r1"],
        ),
    }
    artifact_contracts = {
        "phase2_search_engine": type(
            "Resolved",
            (),
            {
                "inputs": (),
                "optional_inputs": (),
                "outputs": ("phase2.candidates",),
                "external_inputs": (),
            },
        )(),
        "promote_candidates": type(
            "Resolved",
            (),
            {
                "inputs": ("validation.bundle",),
                "optional_inputs": (),
                "outputs": ("promotion.bundle",),
                "external_inputs": (),
            },
        )(),
    }

    plan = build_contract_backed_execution_plan(
        run_id="r1",
        args=args,
        stages=stages,
        artifact_contracts=artifact_contracts,
        planned_at="2026-04-18T00:00:00Z",
    )

    assert [stage.stage_family for stage in plan.stages] == ["phase2_discovery", "promotion"]
    assert {item.contract_id for item in plan.artifact_obligations} >= {
        "run_manifest",
        "discovery_phase2_candidates",
        "promoted_theses",
        "live_thesis_index",
    }


def test_contract_plan_does_not_require_live_thesis_package_without_promote_stage():
    args = type(
        "Args",
        (),
        {
            "mode": "research",
            "symbols": "BTCUSDT",
            "timeframes": "5m",
            "experiment_config": "",
            "registry_root": "project/configs/registries",
        },
    )()
    stages = {
        "phase2_search_engine": StageDefinition(
            name="phase2_search_engine",
            script_path=Path("research/phase2_search_engine.py"),
            args=["--run_id", "r1"],
        ),
        "export_edge_candidates": StageDefinition(
            name="export_edge_candidates",
            script_path=Path("research/export_edge_candidates.py"),
            args=["--run_id", "r1"],
        ),
    }
    artifact_contracts = {
        "phase2_search_engine": type(
            "Resolved",
            (),
            {
                "inputs": (),
                "optional_inputs": (),
                "outputs": ("phase2.candidates",),
                "external_inputs": (),
            },
        )(),
        "export_edge_candidates": type(
            "Resolved",
            (),
            {
                "inputs": ("phase2.candidates",),
                "optional_inputs": (),
                "outputs": ("edge_candidates.normalized",),
                "external_inputs": (),
            },
        )(),
    }

    plan = build_contract_backed_execution_plan(
        run_id="r1",
        args=args,
        stages=stages,
        artifact_contracts=artifact_contracts,
        planned_at="2026-04-18T00:00:00Z",
    )

    contract_ids = {item.contract_id for item in plan.artifact_obligations}
    assert "discovery_phase2_candidates" in contract_ids
    assert "promoted_theses" not in contract_ids
    assert "live_thesis_index" not in contract_ids
