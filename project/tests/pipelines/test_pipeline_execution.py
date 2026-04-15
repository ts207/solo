from __future__ import annotations

import os
from pathlib import Path

from project.pipelines import pipeline_execution


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
