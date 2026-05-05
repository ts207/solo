from __future__ import annotations

import importlib


def _import(name: str):
    return importlib.import_module(name)


def test_cosmetic_strategy_namespace_is_importable():
    strategy_dsl = _import("project.strategy.dsl")
    strategy_runtime = _import("project.strategy.runtime")
    strategy_templates = _import("project.strategy.templates")
    blueprint_mod = _import("project.strategy.models.blueprint")
    executable_spec_mod = _import("project.strategy.models.executable_strategy_spec")
    compile_mod = _import("project.research.compile_strategy_blueprints")

    assert blueprint_mod.Blueprint is not None
    assert executable_spec_mod.ExecutableStrategySpec is not None
    assert callable(compile_mod.main)
    assert strategy_dsl.Blueprint is not None
    assert callable(strategy_runtime.get_strategy)
    assert strategy_templates.StrategySpec is not None


def test_canonical_execution_namespace_is_importable():
    runtime_mod = _import("project.strategy.runtime")
    engine_runner = _import("project.engine.runner")

    assert runtime_mod.DslInterpreterV1 is not None
    assert callable(engine_runner.run_engine)


def test_canonical_io_namespace_is_importable():
    io_utils = _import("project.io.utils")
    run_all = _import("project.pipelines.run_all")

    assert callable(io_utils.ensure_dir)
    assert callable(io_utils.read_parquet)
    assert callable(io_utils.write_parquet)
    assert callable(run_all.main)


def test_canonical_manifest_namespace_is_importable():
    manifest = _import("project.specs.manifest")

    assert callable(manifest.start_manifest)
    assert callable(manifest.finalize_manifest)
    assert callable(manifest.load_run_manifest)


def test_explicit_package_root_surfaces_are_importable():
    artifacts = _import("project.artifacts")
    compilers = _import("project.compilers")
    eval_pkg = _import("project.eval")
    experiments = _import("project.experiments")
    live = _import("project.live")
    pipelines_clean = _import("project.pipelines.clean")
    pipelines_features = _import("project.pipelines.features")
    pipelines_ingest = _import("project.pipelines.ingest")
    pipelines_smoke = _import("project.pipelines.smoke")
    portfolio = _import("project.portfolio")
    research_clustering = _import("project.research.clustering")
    research_reports = _import("project.research.reports")
    research_utils = _import("project.research.utils")
    spec_validation = _import("project.spec_validation")

    assert callable(artifacts.run_manifest_path)
    assert callable(artifacts.load_json_dict)
    assert compilers.ExecutableStrategySpec is not None
    assert callable(compilers.transform_blueprint_to_spec)
    assert callable(experiments.resolve_experiment_config)
    assert experiments.ExperimentConfig is not None
    assert eval_pkg.multiplicity is not None
    assert callable(eval_pkg.build_time_splits)
    assert live.LiveEngineRunner is not None
    assert callable(live.check_kill_switch_triggers)
    assert pipelines_clean.build_cleaned_bars is not None
    assert pipelines_features.build_features is not None
    assert pipelines_ingest.build_universe_snapshots is not None
    assert callable(pipelines_smoke.smoke_offline_main)
    assert portfolio.AllocationSpec is not None
    assert callable(portfolio.calculate_target_notional)
    assert callable(research_clustering.cluster_hypotheses)
    assert callable(research_reports.generate_strategy_summary)
    assert callable(research_utils.fail_closed_bool)
    assert spec_validation.loaders is not None
    assert callable(spec_validation.validate_ontology)
