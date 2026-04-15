from __future__ import annotations

from pathlib import Path

from project import PROJECT_ROOT
from project.pipelines.stages.evaluation import build_evaluation_stages


class _Args:
    experiment_config = None
    run_strategy_blueprint_compiler = 1
    strategy_blueprint_max_per_event = 3
    strategy_blueprint_ignore_checklist = 0
    strategy_blueprint_allow_fallback = 0
    strategy_blueprint_allow_non_executable_conditions = 0
    strategy_blueprint_allow_naive_entry_fail = 0
    strategy_blueprint_min_events_floor = 10
    run_strategy_builder = 1
    strategy_builder_top_k_per_event = 3
    strategy_builder_max_candidates = 12
    strategy_builder_include_alpha_bundle = 1
    strategy_builder_ignore_checklist = 0
    strategy_builder_allow_non_promoted = 0
    strategy_builder_allow_missing_candidate_detail = 0
    strategy_builder_enable_fractional_allocation = 1
    run_profitable_selector = 1


def test_strategy_packaging_stage_generator_points_at_real_scripts() -> None:
    stages = build_evaluation_stages(
        _Args(),
        run_id="run-1",
        symbols="BTCUSDT,ETHUSDT",
        start="2024-01-01",
        end="2024-02-01",
        force_flag="",
        project_root=PROJECT_ROOT,
        data_root=Path("/tmp/data"),
    )

    stage_map = {name: path for name, path, _ in stages}

    assert stage_map["compile_strategy_blueprints"] == (
        PROJECT_ROOT / "research" / "compile_strategy_blueprints.py"
    )
    assert stage_map["build_strategy_candidates"] == (
        PROJECT_ROOT / "research" / "build_strategy_candidates.py"
    )
    assert stage_map["select_profitable_strategies"] == (
        PROJECT_ROOT / "research" / "select_profitable_strategies.py"
    )
    assert all(path.exists() for path in stage_map.values())
