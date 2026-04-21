"""Tests for canonical search-only discovery planning."""

import sys
import types
from pathlib import Path

import yaml

# Add project root to path
sys.path.insert(0, str(Path(__file__).parents[2]))


# Build a comprehensive args namespace that satisfies build_research_stages
def _make_args(**overrides):
    defaults = dict(
        run_phase2_conditional=1,
        phase2_event_type="all",
        phase2_gate_profile_resolved="auto",
        phase2_gate_profile="auto",
        timeframes="15m",
        concept="",
        seed=42,
        discovery_mode="search",
        search_spec="spec/search_space.yaml",
        search_min_n=30,
        registry_root="project/configs/registries",
        config=[],
        market_context_workers=1,
        fees_bps=None,
        slippage_bps=None,
        cost_bps=None,
        skip_ingest_ohlcv=0,
        skip_ingest_funding=0,
        skip_ingest_spot_ohlcv=0,
        run_ingest_liquidation_snapshot=0,
        run_ingest_open_interest_hist=0,
        # Added missing defaults
        phase2_shift_labels_k=0,
        mode="research",
        phase2_cost_calibration_mode="static",
        phase2_cost_min_tob_coverage=0.6,
        phase2_cost_tob_tolerance_minutes=10,
        retail_profile="standard",
        run_bridge_eval_phase2=0,
        bridge_train_frac=0.6,
        bridge_validation_frac=0.2,
        bridge_embargo_days=1,
        bridge_edge_cost_k=2.0,
        bridge_stressed_cost_multiplier=1.5,
        bridge_min_validation_trades=20,
        bridge_candidate_mask="auto",
        run_discovery_quality_summary=0,
        run_naive_entry_eval=0,
        naive_min_trades=20,
        naive_min_expectancy_after_cost=0.0,
        naive_max_drawdown=1.0,
        run_candidate_promotion=0,
        candidate_promotion_max_q_value=0.2,
        candidate_promotion_min_events=20,
        candidate_promotion_min_stability_score=0.6,
        candidate_promotion_min_sign_consistency=0.6,
        candidate_promotion_min_cost_survival_ratio=0.5,
        candidate_promotion_min_tob_coverage=0.6,
        candidate_promotion_max_negative_control_pass_rate=0.1,
        candidate_promotion_require_hypothesis_audit=1,
        candidate_promotion_allow_missing_negative_controls=0,
        run_edge_registry_update=0,
        run_expectancy_analysis=0,
        run_expectancy_robustness=0,
        run_recommendations_checklist=0,
        run_interaction_lift=0,
        event_parameter_overrides={},
    )
    ns = types.SimpleNamespace(**{**defaults, **overrides})
    return ns


def test_discovery_mode_search_includes_search_stage(tmp_path):
    from project.pipelines.stages.research import build_research_stages

    stages = build_research_stages(
        args=_make_args(discovery_mode="search"),
        run_id="r0",
        symbols="BTCUSDT",
        start="2024-01-01",
        end="2024-03-01",
        research_gate_profile="discovery",
        project_root=tmp_path,
        data_root=tmp_path,
        phase2_event_chain=[],
    )
    names = [s[0] for s in stages]
    assert any("phase2_search_engine" in n for n in names)
    assert not any("compare_discovery_paths" in n for n in names)


def test_search_stage_receives_phase2_event_type_pin(tmp_path):
    from project.pipelines.stages.research import build_research_stages

    stages = build_research_stages(
        args=_make_args(phase2_event_type="VOL_SHOCK"),
        run_id="r0",
        symbols="BTCUSDT",
        start="2024-01-01",
        end="2024-03-01",
        research_gate_profile="discovery",
        project_root=tmp_path,
        data_root=tmp_path,
        phase2_event_chain=[],
    )
    names = [s[0] for s in stages]
    _, _, search_args = next(stage for stage in stages if stage[0] == "phase2_search_engine")
    idx = search_args.index("--phase2_event_type")
    assert search_args[idx + 1] == "VOL_SHOCK"
    assert "phase1_correlation_clustering" in names


def test_planner_uses_canonical_research_stage_paths(tmp_path):
    from project.pipelines.stages.research import build_research_stages

    project_root = Path(__file__).parents[3] / "project"
    stages = build_research_stages(
        args=_make_args(
            run_candidate_promotion=1,
            run_edge_registry_update=1,
            run_expectancy_analysis=1,
            run_expectancy_robustness=1,
            run_recommendations_checklist=1,
        ),
        run_id="r0",
        symbols="BTCUSDT",
        start="2024-01-01",
        end="2024-03-01",
        research_gate_profile="discovery",
        project_root=project_root,
        data_root=tmp_path,
        phase2_event_chain=[("VOL_SHOCK", "analyze_events.py", [])],
    )

    stage_paths = {name: path for name, path, _args in stages}
    assert stage_paths["analyze_events__VOL_SHOCK_15m"] == project_root / "research" / "analyze_events.py"
    assert stage_paths["build_event_registry__VOL_SHOCK_15m"] == (
        project_root / "research" / "build_event_registry.py"
    )
    assert stage_paths["phase1_correlation_clustering"] == (
        project_root / "research" / "phase1_correlation_clustering.py"
    )
    assert stage_paths["phase2_search_engine"] == project_root / "research" / "phase2_search_engine.py"
    assert stage_paths["export_edge_candidates"] == project_root / "research" / "export_edge_candidates.py"
    assert stage_paths["promote_candidates"] == project_root / "research" / "cli" / "promotion_cli.py"
    assert stage_paths["update_edge_registry"] == project_root / "research" / "update_edge_registry.py"
    assert stage_paths["analyze_conditional_expectancy"] == (
        project_root / "research" / "analyze_conditional_expectancy.py"
    )
    assert stage_paths["validate_expectancy_traps"] == (
        project_root / "research" / "validate_expectancy_traps.py"
    )
    assert stage_paths["generate_recommendations_checklist"] == (
        project_root / "research" / "generate_recommendations_checklist.py"
    )


def test_universal_event_analyzer_receives_runtime_parameter_overrides(tmp_path):
    from project.pipelines.stages.research import build_research_stages

    project_root = Path(__file__).parents[3] / "project"
    stages = build_research_stages(
        args=_make_args(
            event_parameter_overrides={
                "LIQUIDATION_EXHAUSTION_REVERSAL": {
                    "liquidation_quantile": 0.9,
                    "cooldown_bars": 36,
                }
            },
        ),
        run_id="r0",
        symbols="BTCUSDT",
        start="2024-01-01",
        end="2024-03-01",
        research_gate_profile="discovery",
        project_root=project_root,
        data_root=tmp_path,
        phase2_event_chain=[("LIQUIDATION_EXHAUSTION_REVERSAL", "analyze_events.py", [])],
    )

    _, _, analyze_args = next(
        stage
        for stage in stages
        if stage[0] == "analyze_events__LIQUIDATION_EXHAUSTION_REVERSAL_15m"
    )

    assert analyze_args[analyze_args.index("--liquidation_quantile") + 1] == "0.9"
    assert analyze_args[analyze_args.index("--cooldown_bars") + 1] == "36"


def test_discovery_mode_argument_is_ignored_in_favor_of_canonical_search(tmp_path):
    from project.pipelines.stages.research import build_research_stages

    stages = build_research_stages(
        args=_make_args(discovery_mode="legacy"),
        run_id="r0",
        symbols="BTCUSDT",
        start="2024-01-01",
        end="2024-03-01",
        research_gate_profile="discovery",
        project_root=tmp_path,
        data_root=tmp_path,
        phase2_event_chain=[],
    )
    names = [s[0] for s in stages]
    assert any("phase2_search_engine" in n for n in names)
    assert not any("compare_discovery_paths" in n for n in names)


def test_experiment_config_uses_search_engine_without_legacy_phase2(tmp_path):
    from project.pipelines.stages.research import build_research_stages

    registry_root = Path(__file__).parents[3] / "project" / "configs" / "registries"

    experiment_config = tmp_path / "experiment.yaml"
    experiment_config.write_text(
        yaml.safe_dump(
            {
                "program_id": "single_hypothesis_test",
                "run_mode": "research",
                "description": "single hypothesis",
                "instrument_scope": {
                    "instrument_classes": ["crypto"],
                    "symbols": ["BTCUSDT"],
                    "timeframe": "5m",
                    "start": "2022-11-01",
                    "end": "2022-12-31",
                },
                "trigger_space": {
                    "allowed_trigger_types": ["EVENT"],
                    "events": {"include": ["BASIS_DISLOC"]},
                },
                "templates": {"include": ["mean_reversion"]},
                "evaluation": {
                    "horizons_bars": [12],
                    "directions": ["short"],
                    "entry_lags": [1],
                },
                "contexts": {"include": {}},
                "search_control": {
                    "max_hypotheses_total": 1,
                    "max_hypotheses_per_template": 1,
                    "max_hypotheses_per_event_family": 1,
                    "random_seed": 42,
                },
                "promotion": {
                    "enabled": False,
                    "track": "standard",
                    "multiplicity_scope": "program_id",
                },
                "artifacts": {},
            }
        ),
        encoding="utf-8",
    )

    stages = build_research_stages(
        args=_make_args(
            discovery_mode="search",
            experiment_config=str(experiment_config),
            registry_root=str(registry_root),
        ),
        run_id="r0",
        symbols="BTCUSDT",
        start="2022-11-01",
        end="2022-12-31",
        research_gate_profile="discovery",
        project_root=tmp_path,
        data_root=tmp_path,
        phase2_event_chain=[("BASIS_DISLOC", "basis", [])],
    )

    names = [stage[0] for stage in stages]
    assert "phase2_search_engine" in names
    assert "phase2_conditional_hypotheses__BASIS_DISLOC_15m" not in names
    assert "bridge_evaluate_phase2__BASIS_DISLOC_15m" not in names


def test_preflight_marks_search_engine_active_for_experiment_config(tmp_path):
    from project.pipelines.pipeline_planning import prepare_run_preflight
    from project.pipelines.stages.utils import script_supports_flag

    registry_root = Path(__file__).parents[3] / "project" / "configs" / "registries"
    project_root = Path(__file__).parents[3] / "project"

    experiment_config = tmp_path / "experiment.yaml"
    experiment_config.write_text(
        yaml.safe_dump(
            {
                "program_id": "single_hypothesis_test",
                "run_mode": "research",
                "description": "single hypothesis",
                "instrument_scope": {
                    "instrument_classes": ["crypto"],
                    "symbols": ["BTCUSDT"],
                    "timeframe": "5m",
                    "start": "2022-11-01",
                    "end": "2022-12-31",
                },
                "trigger_space": {
                    "allowed_trigger_types": ["EVENT"],
                    "events": {"include": ["BASIS_DISLOC"]},
                },
                "templates": {"include": ["mean_reversion"]},
                "evaluation": {
                    "horizons_bars": [12],
                    "directions": ["short"],
                    "entry_lags": [1],
                },
                "contexts": {"include": {}},
                "search_control": {
                    "max_hypotheses_total": 1,
                    "max_hypotheses_per_template": 1,
                    "max_hypotheses_per_event_family": 1,
                    "random_seed": 42,
                },
                "promotion": {
                    "enabled": False,
                    "track": "standard",
                    "multiplicity_scope": "program_id",
                },
                "artifacts": {},
            }
        ),
        encoding="utf-8",
    )

    args = _make_args(
        run_id="r0",
        symbols="BTCUSDT",
        start="2022-11-01",
        end="2022-12-31",
        phase2_event_type="VOL_SHOCK",
        templates={"include": ["mean_reversion"]},
        experiment_config=str(experiment_config),
        registry_root=str(registry_root),
        objective_name="retail_profitability",
        objective_spec=None,
        retail_profiles_spec=None,
        force=0,
        allow_missing_funding=0,
        enable_cross_venue_spot_pipeline=0,
        runtime_invariants_mode="warn",
        emit_run_hash=0,
        determinism_replay_checks=0,
        oms_replay_checks=0,
        performance_mode=0,
        run_strategy_blueprint_compiler=0,
        run_strategy_builder=0,
    )

    preflight = prepare_run_preflight(
        args=args,
        project_root=project_root,
        data_root=project_root.parent / "data",
        cli_flag_present=lambda _flag: False,
        run_id_default=lambda: "unused",
        script_supports_flag=script_supports_flag,
    )

    assert preflight["effective_behavior"]["runs_search_engine"] is True
    assert preflight["effective_behavior"]["runs_legacy_phase2_conditional"] is False
    assert preflight["effective_behavior"]["phase2_event_type"] == "BASIS_DISLOC"
    assert preflight["effective_behavior"]["phase2_event_type_source"] == "experiment_config_event_pin"


def test_preflight_keeps_basis_regime_experiment_on_bybit_only_pipeline(tmp_path):
    from project.pipelines.pipeline_planning import prepare_run_preflight
    from project.pipelines.stages.utils import script_supports_flag

    registry_root = Path(__file__).parents[3] / "project" / "configs" / "registries"
    project_root = Path(__file__).parents[3] / "project"

    experiment_config = tmp_path / "experiment.yaml"
    experiment_config.write_text(
        yaml.safe_dump(
            {
                "program_id": "edge_basis_funding_dislocation",
                "run_mode": "research",
                "description": "basis/funding dislocation spot coverage check",
                "instrument_scope": {
                    "instrument_classes": ["crypto"],
                    "symbols": ["BTCUSDT", "ETHUSDT"],
                    "timeframe": "5m",
                    "start": "2023-01-15",
                    "end": "2023-02-28",
                },
                "trigger_space": {
                    "allowed_trigger_types": ["EVENT"],
                    "events": {"include": ["BASIS_DISLOC"]},
                    "canonical_regimes": ["BASIS_FUNDING_DISLOCATION"],
                },
                "templates": {"include": ["mean_reversion"]},
                "evaluation": {
                    "horizons_bars": [12],
                    "directions": ["long", "short"],
                    "entry_lags": [1],
                },
                "contexts": {"include": {}},
                "search_control": {
                    "max_hypotheses_total": 4,
                    "max_hypotheses_per_template": 4,
                    "max_hypotheses_per_event_family": 4,
                    "random_seed": 42,
                },
                "promotion": {
                    "enabled": False,
                    "track": "standard",
                    "multiplicity_scope": "program_id",
                },
                "artifacts": {},
            }
        ),
        encoding="utf-8",
    )

    args = _make_args(
        run_id="r0",
        symbols="BTCUSDT,ETHUSDT",
        start="2023-01-15",
        end="2023-02-28",
        timeframes="5m",
        experiment_config=str(experiment_config),
        registry_root=str(registry_root),
        objective_name="retail_profitability",
        objective_spec=None,
        retail_profiles_spec=None,
        force=0,
        allow_missing_funding=0,
        enable_cross_venue_spot_pipeline=0,
        runtime_invariants_mode="warn",
        emit_run_hash=0,
        determinism_replay_checks=0,
        oms_replay_checks=0,
        performance_mode=0,
        run_strategy_blueprint_compiler=0,
        run_strategy_builder=0,
    )

    preflight = prepare_run_preflight(
        args=args,
        project_root=project_root,
        data_root=project_root.parent / "data",
        cli_flag_present=lambda _flag: False,
        run_id_default=lambda: "unused",
        script_supports_flag=script_supports_flag,
    )

    stage_names = list(preflight["stages"].keys())
    assert args.enable_cross_venue_spot_pipeline == 0
    assert args.skip_ingest_spot_ohlcv == 1
    assert "ingest_binance_spot_ohlcv_5m" not in stage_names
    assert "build_cleaned_5m_spot" not in stage_names
    assert "build_features_5m_spot" not in stage_names


def test_preflight_respects_experiment_promotion_toggle_and_adds_promotion_stages(tmp_path):
    from project.pipelines.pipeline_planning import prepare_run_preflight
    from project.pipelines.stages.utils import script_supports_flag

    registry_root = Path(__file__).parents[3] / "project" / "configs" / "registries"
    project_root = Path(__file__).parents[3] / "project"

    experiment_config = tmp_path / "experiment.yaml"
    experiment_config.write_text(
        yaml.safe_dump(
            {
                "program_id": "single_hypothesis_test",
                "run_mode": "research",
                "description": "single hypothesis",
                "instrument_scope": {
                    "instrument_classes": ["crypto"],
                    "symbols": ["BTCUSDT"],
                    "timeframe": "5m",
                    "start": "2022-11-01",
                    "end": "2022-12-31",
                },
                "trigger_space": {
                    "allowed_trigger_types": ["EVENT"],
                    "events": {"include": ["BASIS_DISLOC"]},
                },
                "templates": {"include": ["mean_reversion"]},
                "evaluation": {
                    "horizons_bars": [12],
                    "directions": ["short"],
                    "entry_lags": [1],
                },
                "contexts": {"include": {}},
                "search_control": {
                    "max_hypotheses_total": 1,
                    "max_hypotheses_per_template": 1,
                    "max_hypotheses_per_event_family": 1,
                    "random_seed": 42,
                },
                "promotion": {
                    "enabled": True,
                    "track": "standard",
                    "multiplicity_scope": "program_id",
                },
                "artifacts": {},
            }
        ),
        encoding="utf-8",
    )

    args = _make_args(
        run_id="r0",
        symbols="BTCUSDT",
        start="2022-11-01",
        end="2022-12-31",
        experiment_config=str(experiment_config),
        registry_root=str(registry_root),
        objective_name="retail_profitability",
        objective_spec=None,
        retail_profiles_spec=None,
        force=0,
        allow_missing_funding=0,
        enable_cross_venue_spot_pipeline=0,
        runtime_invariants_mode="warn",
        emit_run_hash=0,
        determinism_replay_checks=0,
        oms_replay_checks=0,
        performance_mode=0,
        run_candidate_promotion=0,
        run_edge_registry_update=0,
        run_strategy_blueprint_compiler=0,
        run_strategy_builder=0,
    )

    preflight = prepare_run_preflight(
        args=args,
        project_root=project_root,
        data_root=project_root.parent / "data",
        cli_flag_present=lambda _flag: False,
        run_id_default=lambda: "unused",
        script_supports_flag=script_supports_flag,
    )

    stage_names = list(preflight["stages"].keys())
    assert args.run_candidate_promotion == 1
    assert args.run_edge_registry_update == 1
    assert "promote_candidates" in stage_names
    assert "update_edge_registry" in stage_names


def test_preflight_disables_promotion_when_experiment_config_turns_it_off(tmp_path):
    from project.pipelines.pipeline_planning import prepare_run_preflight
    from project.pipelines.stages.utils import script_supports_flag

    registry_root = Path(__file__).parents[3] / "project" / "configs" / "registries"
    project_root = Path(__file__).parents[3] / "project"

    experiment_config = tmp_path / "experiment.yaml"
    experiment_config.write_text(
        yaml.safe_dump(
            {
                "program_id": "single_hypothesis_test",
                "run_mode": "research",
                "description": "single hypothesis",
                "instrument_scope": {
                    "instrument_classes": ["crypto"],
                    "symbols": ["BTCUSDT"],
                    "timeframe": "5m",
                    "start": "2022-11-01",
                    "end": "2022-12-31",
                },
                "trigger_space": {
                    "allowed_trigger_types": ["EVENT"],
                    "events": {"include": ["BASIS_DISLOC"]},
                },
                "templates": {"include": ["mean_reversion"]},
                "evaluation": {
                    "horizons_bars": [12],
                    "directions": ["short"],
                    "entry_lags": [1],
                },
                "contexts": {"include": {}},
                "search_control": {
                    "max_hypotheses_total": 1,
                    "max_hypotheses_per_template": 1,
                    "max_hypotheses_per_event_family": 1,
                    "random_seed": 42,
                },
                "promotion": {
                    "enabled": False,
                    "track": "standard",
                    "multiplicity_scope": "program_id",
                },
                "artifacts": {},
            }
        ),
        encoding="utf-8",
    )

    args = _make_args(
        run_id="r0",
        symbols="BTCUSDT",
        start="2022-11-01",
        end="2022-12-31",
        experiment_config=str(experiment_config),
        registry_root=str(registry_root),
        objective_name="retail_profitability",
        objective_spec=None,
        retail_profiles_spec=None,
        force=0,
        allow_missing_funding=0,
        enable_cross_venue_spot_pipeline=0,
        runtime_invariants_mode="warn",
        emit_run_hash=0,
        determinism_replay_checks=0,
        oms_replay_checks=0,
        performance_mode=0,
        run_candidate_promotion=1,
        run_edge_registry_update=1,
        run_strategy_blueprint_compiler=0,
        run_strategy_builder=0,
    )

    preflight = prepare_run_preflight(
        args=args,
        project_root=project_root,
        data_root=project_root.parent / "data",
        cli_flag_present=lambda _flag: False,
        run_id_default=lambda: "unused",
        script_supports_flag=script_supports_flag,
    )

    stage_names = list(preflight["stages"].keys())
    assert args.run_candidate_promotion == 0
    assert args.run_edge_registry_update == 0
    assert "promote_candidates" not in stage_names
    assert "update_edge_registry" not in stage_names


def test_preflight_enables_edge_registry_update_for_expectancy_tail_without_promotion(tmp_path):
    from project.pipelines.pipeline_planning import prepare_run_preflight
    from project.pipelines.stages.utils import script_supports_flag

    project_root = Path(__file__).parents[3] / "project"

    args = _make_args(
        run_id="r_expectancy",
        symbols="BTCUSDT",
        start="2022-11-01",
        end="2022-12-31",
        objective_name="retail_profitability",
        objective_spec=None,
        retail_profiles_spec=None,
        force=0,
        allow_missing_funding=0,
        enable_cross_venue_spot_pipeline=0,
        runtime_invariants_mode="warn",
        emit_run_hash=0,
        determinism_replay_checks=0,
        oms_replay_checks=0,
        performance_mode=0,
        run_candidate_promotion=0,
        run_edge_registry_update=0,
        run_expectancy_analysis=1,
        run_expectancy_robustness=1,
        run_recommendations_checklist=1,
        run_strategy_blueprint_compiler=0,
        run_strategy_builder=0,
    )

    preflight = prepare_run_preflight(
        args=args,
        project_root=project_root,
        data_root=project_root.parent / "data",
        cli_flag_present=lambda _flag: False,
        run_id_default=lambda: "unused",
        script_supports_flag=script_supports_flag,
    )

    stage_names = list(preflight["stages"].keys())
    assert args.run_edge_registry_update == 1
    assert "update_edge_registry" in stage_names
