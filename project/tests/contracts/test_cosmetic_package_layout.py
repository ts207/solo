import project.artifacts as artifacts
import project.compilers as compilers
import project.experiments as experiments
import project.eval as eval_pkg
import project.live as live
import project.pipelines.clean as pipelines_clean
import project.pipelines.features as pipelines_features
import project.pipelines.ingest as pipelines_ingest
import project.pipelines.smoke as pipelines_smoke
import project.portfolio as portfolio
import project.research.clustering as research_clustering
import project.research.reports as research_reports
import project.research.utils as research_utils
import project.spec_validation as spec_validation
from project.engine.runner import run_engine
from project.io.utils import ensure_dir, read_parquet, write_parquet
from project.pipelines.run_all import main as run_all_main
from project.research.compile_strategy_blueprints import main as compile_strategy_blueprints_main
from project.specs.manifest import finalize_manifest, load_run_manifest, start_manifest
from project.strategy.runtime import DslInterpreterV1
import project.strategy.dsl as strategy_dsl
import project.strategy.runtime as strategy_runtime
import project.strategy.templates as strategy_templates
from project.strategy.models.blueprint import Blueprint
from project.strategy.models.executable_strategy_spec import ExecutableStrategySpec


def test_cosmetic_strategy_namespace_is_importable():
    assert Blueprint is not None
    assert ExecutableStrategySpec is not None
    assert callable(compile_strategy_blueprints_main)
    assert strategy_dsl.Blueprint is not None
    assert callable(strategy_runtime.get_strategy)
    assert strategy_templates.StrategySpec is not None


def test_canonical_execution_namespace_is_importable():
    assert DslInterpreterV1 is not None
    assert callable(run_engine)


def test_canonical_io_namespace_is_importable():
    assert callable(ensure_dir)
    assert callable(read_parquet)
    assert callable(write_parquet)
    assert callable(run_all_main)


def test_canonical_manifest_namespace_is_importable():
    assert callable(start_manifest)
    assert callable(finalize_manifest)
    assert callable(load_run_manifest)


def test_explicit_package_root_surfaces_are_importable():
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
