from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from project.contracts.artifacts import list_artifact_contracts
from project.contracts.pipeline_registry import resolve_stage_family_contract
from project.core.timeframes import normalize_timeframe
from project.events.phase2 import PHASE2_EVENT_CHAIN
from project.io.utils import (
    cleaned_dataset_covers_window,
    discover_external_cleaned_root,
    external_cleaned_dataset_dir,
    materialize_external_cleaned_dataset,
    unreadable_parquet_samples,
)
from project.pipelines.effective_config import resolve_effective_args
from project.pipelines.execution_plan import (
    ExecutionPlan,
    PlannedArtifactObligation,
    PlannedStage,
)
from project.pipelines.pipeline_defaults import utc_now_iso
from project.pipelines.pipeline_provenance import (
    objective_spec_metadata,
    resolve_objective_name,
    resolve_retail_profile_name,
    retail_profile_metadata,
)
from project.pipelines.planner import build_pipeline_plan
from project.pipelines.stage_definitions import ResolvedStageArtifactContract
from project.pipelines.stage_dependencies import resolve_stage_artifact_contract
from project.research.feature_surface_viability import analyze_feature_surface_viability
from project.specs.ontology import ontology_spec_hash

_SPOT_PIPELINE_EVENT_HINTS = {
    "BASIS_DISLOC",
    "CROSS_VENUE_DESYNC",
    "FND_DISLOC",
    "SPOT_PERP_BASIS_SHOCK",
}
_SPOT_PIPELINE_REGIME_HINTS = {
    "BASIS_FUNDING_DISLOCATION",
}


def _json_object(value: str) -> dict[str, Any]:
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(
            "--event_parameter_overrides must be a JSON object"
        ) from exc
    if not isinstance(loaded, dict):
        raise argparse.ArgumentTypeError("--event_parameter_overrides must be a JSON object")
    return loaded


def build_parser() -> argparse.ArgumentParser:
    """Builds the ArgumentParser for run_all.py with all necessary flags."""
    parser = argparse.ArgumentParser(
        description="Run discovery-first pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # Core Pipeline Flags
    parser.add_argument("--run_id", help="Unique ID for this pipeline run.")
    parser.add_argument("--experiment_config", help="Path to an experiment YAML config.")
    parser.add_argument(
        "--registry_root",
        default="project/configs/registries",
        help="Path to platform-owned registries.",
    )
    parser.add_argument("--override", action="append", default=[], help="Override config keys.")
    parser.add_argument(
        "--symbols", default="dynamic", help="Comma-separated symbols or 'dynamic'."
    )
    parser.add_argument("--start", help="Start date (YYYY-MM-DD).")
    parser.add_argument("--end", help="End date (YYYY-MM-DD).")
    parser.add_argument(
        "--mode", choices=["research", "production", "certification"], default="research"
    )
    parser.add_argument("--config", action="append", default=[], help="Additional YAML configs.")

    # Ingest / Data Flags
    parser.add_argument("--skip_ingest_ohlcv", type=int, default=0)
    parser.add_argument("--skip_ingest_funding", type=int, default=0)
    parser.add_argument("--skip_ingest_spot_ohlcv", type=int, default=0)
    parser.add_argument(
        "--funding_scale", choices=["auto", "decimal", "percent", "bps"], default="auto"
    )
    parser.add_argument("--enable_cross_venue_spot_pipeline", type=int, default=0)
    parser.add_argument("--allow_constant_funding", type=int, default=0)
    parser.add_argument("--allow_funding_timestamp_rounding", type=int, default=0)
    parser.add_argument("--run_ingest_liquidation_snapshot", type=int, default=0)
    parser.add_argument("--run_ingest_open_interest_hist", type=int, default=0)
    parser.add_argument(
        "--offline_mode",
        type=int,
        default=0,
        help=(
            "Prefer local cleaned-bar artifacts and fail fast when the requested window is not "
            "available offline."
        ),
    )
    parser.add_argument(
        "--offline_cleaned_root",
        default="",
        help="Optional explicit path to an external offline-data/cleaned_bars root.",
    )
    # LT-002: Hardcoded Open Interest to only use 5m archive to prevent API trailing gaps and distribution mismatches
    parser.add_argument(
        "--timeframes", default="5m", help="Comma-separated list of timeframes (e.g., '1m,5m,15m')"
    )

    # Concept / Strategy Flags
    parser.add_argument("--concept", default="", help="Path to a Unified ControlSpec YAML file.")
    parser.add_argument("--objective_name", default="")
    parser.add_argument("--objective_spec", default=None)
    parser.add_argument("--retail_profile", default="")
    parser.add_argument("--retail_profiles_spec", default=None)
    parser.add_argument("--allow_ontology_hash_mismatch", type=int, default=0)
    parser.add_argument("--run_ontology_consistency_audit", type=int, default=1)
    parser.add_argument("--ontology_consistency_fail_on_missing", type=int, default=1)

    # Hypothesis / Research Flags
    parser.add_argument("--hypothesis_datasets", default="auto")
    parser.add_argument("--hypothesis_max_fused", type=int, default=24)

    # Phase 2 Flags
    parser.add_argument("--run_phase2_conditional", type=int, default=1)
    parser.add_argument(
        "--phase2_event_type",
        default="VOL_SHOCK",
        help=(
            "Primary phase2 event family. Template-only runs without an explicit event pin "
            "auto-resolve to 'all' during preflight."
        ),
    )
    parser.add_argument("--events", nargs="+", help="Explicit subset of event IDs to run.")
    parser.add_argument(
        "--templates", nargs="+", help="Explicit subset of strategy templates to run."
    )
    parser.add_argument(
        "--horizons", nargs="+", help="Explicit subset of horizons (e.g., 5m, 15m) to run."
    )
    parser.add_argument(
        "--directions", nargs="+", help="Explicit subset of directions (e.g., long, short) to run."
    )
    parser.add_argument(
        "--contexts", nargs="+", help="Explicit subset of contexts (e.g., session=open) to run."
    )
    parser.add_argument(
        "--entry_lags", nargs="+", type=int, help="Explicit subset of entry lags (bars) to run."
    )
    parser.add_argument("--sequence_max_gap", type=int, help="Max gap for event sequences.")
    parser.add_argument("--program_id", help="Program ID for experiment campaign tracking.")
    parser.add_argument("--search_budget", type=int, help="Limit total candidate expansions.")
    parser.add_argument(
        "--event_parameter_overrides",
        type=_json_object,
        default={},
        help=(
            "Research-only JSON object of event_id -> detector parameter overrides. "
            "This changes detector thresholds, not Phase 2 gates."
        ),
    )

    parser.add_argument("--phase2_max_conditions", type=int, default=20)
    parser.add_argument("--phase2_max_actions", type=int, default=9)
    parser.add_argument("--phase2_min_regime_stable_splits", type=int, default=2)
    parser.add_argument("--phase2_require_phase1_pass", type=int, default=1)
    parser.add_argument("--phase2_min_ess", type=float, default=150.0)
    parser.add_argument("--phase2_ess_max_lag", type=int, default=24)
    parser.add_argument("--phase2_multiplicity_k", type=float, default=1.0)
    parser.add_argument("--phase2_parameter_curvature_max_penalty", type=float, default=0.50)
    parser.add_argument("--phase2_delay_grid_bars", default="0,4,8,16,30")
    parser.add_argument("--phase2_min_delay_positive_ratio", type=float, default=0.60)
    parser.add_argument("--phase2_min_delay_robustness_score", type=float, default=0.60)
    parser.add_argument("--phase2_shift_labels_k", type=int, default=0)
    parser.add_argument(
        "--phase2_cost_calibration_mode", choices=["static", "tob_regime"], default="static"
    )
    parser.add_argument("--phase2_cost_min_tob_coverage", type=float, default=0.60)
    parser.add_argument("--phase2_cost_tob_tolerance_minutes", type=int, default=10)
    parser.add_argument(
        "--phase2_gate_profile",
        choices=["auto", "discovery", "promotion", "synthetic"],
        default="auto",
    )
    parser.add_argument(
        "--discovery_profile", choices=["standard", "exploratory", "synthetic"], default="standard"
    )
    parser.add_argument(
        "--discovery-mode",
        choices=["search"],
        default="search",
        help="Canonical discovery path. Only the search-backed discovery engine is supported.",
    )
    parser.add_argument(
        "--search_spec",
        default="spec/search_space.yaml",
        help="Search spec name or path (default: spec/search_space.yaml).",
    )
    parser.add_argument(
        "--search_min_n",
        type=int,
        default=30,
        help="Min sample size for new search engine discovery.",
    )

    # Bridge / Eval Flags
    parser.add_argument("--run_bridge_eval_phase2", type=int, default=1)
    parser.add_argument("--bridge_edge_cost_k", type=float, default=2.0)
    parser.add_argument("--bridge_stressed_cost_multiplier", type=float, default=1.5)
    parser.add_argument("--bridge_min_validation_trades", type=int, default=20)
    parser.add_argument("--bridge_train_frac", type=float, default=0.6)
    parser.add_argument("--bridge_validation_frac", type=float, default=0.2)
    parser.add_argument("--bridge_embargo_days", type=int, default=1)
    parser.add_argument(
        "--bridge_candidate_mask", choices=["auto", "research", "final", "all"], default="auto"
    )
    parser.add_argument("--run_discovery_quality_summary", type=int, default=1)
    parser.add_argument("--run_naive_entry_eval", type=int, default=1)
    parser.add_argument("--naive_min_trades", type=int, default=20)
    parser.add_argument("--naive_min_expectancy_after_cost", type=float, default=0.0)
    parser.add_argument("--naive_max_drawdown", type=float, default=1.0)

    # Execution / Performance Flags
    parser.add_argument("--max_analyzer_workers", type=int, default=8)
    parser.add_argument("--market_context_workers", type=int, default=1)
    parser.add_argument("--analyzer_symbol_workers", type=int, default=1)
    parser.add_argument("--phase2_parallel_workers", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--performance_mode", type=int, default=0)
    parser.add_argument("--enable_event_stage_cache", type=int, default=1)
    parser.add_argument("--resume_from_failed_stage", type=int, default=0)
    parser.add_argument(
        "--feature_schema_version", default="", help="Leave empty for canonical default."
    )

    # Strategy / Promotion Flags
    parser.add_argument("--run_candidate_promotion", type=int, default=1)
    parser.add_argument(
        "--run_recommendations_checklist",
        type=int,
        default=1,
        help=(
            "Run the recommendations checklist. When enabled, preflight auto-enables "
            "expectancy analysis and robustness unless those flags are explicitly set."
        ),
    )
    parser.add_argument(
        "--run_expectancy_analysis",
        type=int,
        default=0,
        help="Run expectancy analysis. May be auto-enabled by the checklist preflight.",
    )
    parser.add_argument(
        "--run_expectancy_robustness",
        type=int,
        default=0,
        help="Run expectancy robustness. May be auto-enabled by the checklist preflight.",
    )
    parser.add_argument("--run_strategy_builder", type=int, default=1)
    parser.add_argument("--strategy_builder_top_k_per_event", type=int, default=2)
    parser.add_argument("--strategy_builder_max_candidates", type=int, default=20)
    parser.add_argument("--strategy_builder_include_alpha_bundle", type=int, default=1)
    parser.add_argument("--strategy_builder_allow_non_promoted", type=int, default=0)
    parser.add_argument("--strategy_builder_allow_missing_candidate_detail", type=int, default=0)
    parser.add_argument("--strategy_builder_enable_fractional_allocation", type=int, default=1)
    parser.add_argument("--run_strategy_blueprint_compiler", type=int, default=0)
    parser.add_argument("--strategy_blueprint_max_per_event", type=int, default=5)
    parser.add_argument("--strategy_blueprint_min_events_floor", type=int, default=20)
    parser.add_argument("--strategy_blueprint_allow_fallback", type=int, default=0)
    parser.add_argument("--strategy_blueprint_allow_non_executable_conditions", type=int, default=0)
    parser.add_argument("--strategy_blueprint_allow_naive_entry_fail", type=int, default=0)
    parser.add_argument("--strategy_blueprint_ignore_checklist", type=int, default=0)
    parser.add_argument("--strategy_builder_ignore_checklist", type=int, default=0)
    parser.add_argument("--run_profitable_selector", type=int, default=0)
    parser.add_argument("--run_interaction_lift", type=int, default=0)
    parser.add_argument("--run_promotion_audit", type=int, default=1)
    parser.add_argument("--run_edge_registry_update", type=int, default=0)
    parser.add_argument("--run_campaign_memory_update", type=int, default=1)
    parser.add_argument("--campaign_memory_promising_top_k", type=int, default=5)
    parser.add_argument("--campaign_memory_avoid_top_k", type=int, default=5)
    parser.add_argument("--campaign_memory_repair_top_k", type=int, default=5)
    parser.add_argument("--campaign_memory_exploit_top_k", type=int, default=3)
    parser.add_argument("--campaign_memory_frontier_untested_top_k", type=int, default=3)
    parser.add_argument("--campaign_memory_frontier_repair_top_k", type=int, default=2)
    parser.add_argument("--campaign_memory_exhausted_failure_threshold", type=int, default=3)
    parser.add_argument("--run_edge_candidate_universe", type=int, default=1)
    parser.add_argument("--strict_recommendations_checklist", type=int, default=0)
    parser.add_argument("--auto_continue_on_keep_research", type=int, default=0)
    parser.add_argument("--ci_fail_on_non_production_overrides", type=int, default=0)
    parser.add_argument("--candidate_promotion_max_q_value", type=float, default=None)
    parser.add_argument(
        "--promotion_profile",
        choices=["auto", "research", "deploy", "disabled"],
        default="auto",
    )
    parser.add_argument(
        "--candidate_promotion_profile",
        choices=["auto", "research", "deploy"],
        default="auto",
    )
    parser.add_argument("--candidate_promotion_min_events", type=int, default=20)
    parser.add_argument("--candidate_promotion_min_stability_score", type=float, default=0.60)
    parser.add_argument("--candidate_promotion_min_sign_consistency", type=float, default=0.60)
    parser.add_argument("--candidate_promotion_min_cost_survival_ratio", type=float, default=0.50)
    parser.add_argument("--candidate_promotion_min_tob_coverage", type=float, default=0.60)
    parser.add_argument(
        "--candidate_promotion_max_negative_control_pass_rate", type=float, default=0.10
    )
    parser.add_argument("--candidate_promotion_require_hypothesis_audit", type=int, default=1)
    parser.add_argument(
        "--candidate_promotion_allow_missing_negative_controls", type=int, default=0
    )
    parser.add_argument("--promotion_allow_fallback_evidence", type=int, default=0)

    # Runtime Invariants / Replay Flags
    parser.add_argument(
        "--runtime_invariants_mode", choices=["off", "audit", "enforce"], default="audit"
    )
    parser.add_argument("--emit_run_hash", type=int, default=0)
    parser.add_argument("--determinism_replay_checks", type=int, default=0)
    parser.add_argument("--oms_replay_checks", type=int, default=0)
    parser.add_argument("--runtime_max_events", type=int, default=250000)
    parser.add_argument(
        "--research_compare_baseline_run_id",
        default="",
        help="Optional baseline research run_id for automatic phase2/promotion diagnostic comparison.",
    )
    parser.add_argument(
        "--research_compare_drift_mode",
        choices=["off", "warn", "enforce"],
        default="warn",
        help="How to treat research comparison drift when a baseline run is configured.",
    )
    parser.add_argument(
        "--research_compare_max_phase2_candidate_count_delta_abs", type=float, default=10.0
    )
    parser.add_argument(
        "--research_compare_max_phase2_survivor_count_delta_abs", type=float, default=2.0
    )
    parser.add_argument(
        "--research_compare_max_phase2_zero_eval_rows_increase", type=float, default=0.0
    )
    parser.add_argument(
        "--research_compare_max_phase2_survivor_q_value_increase", type=float, default=0.05
    )
    parser.add_argument(
        "--research_compare_max_phase2_survivor_estimate_bps_drop", type=float, default=3.0
    )
    parser.add_argument(
        "--research_compare_max_promotion_promoted_count_delta_abs", type=float, default=2.0
    )
    parser.add_argument("--research_compare_max_reject_reason_shift_abs", type=float, default=3.0)
    parser.add_argument(
        "--research_compare_max_edge_tradable_count_delta_abs", type=float, default=2.0
    )
    parser.add_argument(
        "--research_compare_max_edge_candidate_count_delta_abs", type=float, default=2.0
    )
    parser.add_argument(
        "--research_compare_max_edge_after_cost_positive_validation_count_delta_abs",
        type=float,
        default=2.0,
    )
    parser.add_argument(
        "--research_compare_max_edge_median_resolved_cost_bps_delta_abs", type=float, default=0.25
    )
    parser.add_argument(
        "--research_compare_max_edge_median_expectancy_bps_delta_abs", type=float, default=0.25
    )
    parser.add_argument("--phase2_min_validation_n_obs", type=int, default=None)
    parser.add_argument("--phase2_min_test_n_obs", type=int, default=None)
    parser.add_argument("--phase2_min_total_n_obs", type=int, default=None)

    # Execution Flow / Smoke Flags
    parser.add_argument(
        "--dry_run", type=int, default=0, help="Plan and manifest only, no execution."
    )
    parser.add_argument("--plan_only", type=int, default=0, help="Exit after printing the plan.")
    parser.add_argument("--smoke", type=int, default=0, help="Run a minimal single-symbol slice.")
    parser.add_argument("--fees_bps", type=float, default=None)
    parser.add_argument("--slippage_bps", type=float, default=None)
    parser.add_argument("--cost_bps", type=float, default=None)

    return parser


def resolve_experiment_context(
    parser: argparse.ArgumentParser, raw_argv: list[str], **kwargs
) -> tuple[argparse.Namespace, dict, str, Path]:
    """Resolves effective configuration and experiment context."""
    experiment_id = "default"
    experiment_results_dir = kwargs.get("data_root", Path("/tmp")) / "experiments" / experiment_id
    args, resolved_config = resolve_effective_args(parser, raw_argv)
    return args, resolved_config, experiment_id, experiment_results_dir


def parse_symbols_csv(symbols_csv: str) -> list[str]:
    """Parses a comma-separated string of symbols into a list of unique symbols."""
    out: list[str] = []
    seen = set()
    for raw in str(symbols_csv).split(","):
        symbol = raw.strip().upper()
        if symbol and symbol not in seen:
            out.append(symbol)
            seen.add(symbol)
    return out


def parse_timeframes_csv(timeframes_csv: str) -> list[str]:
    """Parse comma-separated timeframe input to canonical, unique values."""
    out: list[str] = []
    seen = set()
    for raw in str(timeframes_csv or "").split(","):
        token = str(raw).strip()
        if not token:
            continue
        normalized = normalize_timeframe(token)
        if normalized not in seen:
            out.append(normalized)
            seen.add(normalized)
    if not out:
        out.append(normalize_timeframe("5m"))
    return out


def _normalized_tokens(values: Any) -> set[str]:
    if values is None:
        return set()
    if isinstance(values, str):
        token = values.strip().upper()
        return {token} if token else set()
    if not isinstance(values, (list, tuple, set)):
        return set()
    return {
        str(value).strip().upper()
        for value in values
        if str(value).strip()
    }


def _experiment_trigger_hints(
    args: argparse.Namespace,
    *,
    include_phase2_event_type: bool = True,
) -> tuple[set[str], set[str]]:
    events = _normalized_tokens(getattr(args, "events", None))
    phase2_event_type = str(getattr(args, "phase2_event_type", "") or "").strip().upper()
    if include_phase2_event_type and phase2_event_type and phase2_event_type != "ALL":
        events.add(phase2_event_type)

    regimes: set[str] = set()
    experiment_config_path = str(getattr(args, "experiment_config", "") or "").strip()
    if not experiment_config_path:
        return events, regimes

    try:
        payload = yaml.safe_load(Path(experiment_config_path).read_text(encoding="utf-8")) or {}
    except Exception:
        return events, regimes
    if not isinstance(payload, dict):
        return events, regimes

    trigger_space = payload.get("trigger_space", {})
    if not isinstance(trigger_space, Mapping):
        return events, regimes

    event_block = trigger_space.get("events", {})
    if isinstance(event_block, Mapping):
        events |= _normalized_tokens(event_block.get("include", []))
    regimes |= _normalized_tokens(trigger_space.get("canonical_regimes", []))
    return events, regimes


def _experiment_promotion_enabled(args: argparse.Namespace) -> bool | None:
    experiment_config_path = str(getattr(args, "experiment_config", "") or "").strip()
    if not experiment_config_path:
        return None

    try:
        payload = yaml.safe_load(Path(experiment_config_path).read_text(encoding="utf-8")) or {}
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None

    promotion = payload.get("promotion", {})
    if not isinstance(promotion, Mapping):
        return None
    enabled = promotion.get("enabled")
    if enabled is None:
        return None
    return bool(enabled)


def _requires_cross_venue_spot_pipeline(args: argparse.Namespace) -> bool:
    events, regimes = _experiment_trigger_hints(args)
    return bool(
        (events & _SPOT_PIPELINE_EVENT_HINTS)
        or (regimes & _SPOT_PIPELINE_REGIME_HINTS)
    )


def resolve_pipeline_artifact_contracts(
    stages: Mapping[str, Any],
) -> tuple[dict[str, ResolvedStageArtifactContract], list[str]]:
    """Resolve artifact contracts for each planned stage."""
    resolved: dict[str, ResolvedStageArtifactContract] = {}
    issues: list[str] = []
    for stage_name, stage_def in stages.items():
        contract, contract_issues = resolve_stage_artifact_contract(
            stage_name,
            list(getattr(stage_def, "args", [])),
        )
        if contract is not None:
            resolved[stage_name] = contract
        if contract_issues:
            issues.extend(contract_issues)
    return resolved, issues


def build_contract_backed_execution_plan(
    *,
    run_id: str,
    args: argparse.Namespace,
    stages: Mapping[str, Any],
    artifact_contracts: Mapping[str, ResolvedStageArtifactContract],
    planned_at: str | None = None,
    skipped_stage_specs: list[Mapping[str, str]] | None = None,
) -> ExecutionPlan:
    selected_stage_families: dict[str, list[str]] = {"run_orchestration": ["run_manifest"]}
    plan_stages: list[PlannedStage] = []

    for stage_name, stage_def in stages.items():
        family_contract = resolve_stage_family_contract(stage_name)
        family_name = family_contract.family if family_contract is not None else ""
        if family_name:
            selected_stage_families.setdefault(family_name, []).append(stage_name)
        artifact_surface = artifact_contracts.get(stage_name)
        required_artifact_contract_ids = tuple(
            contract.contract_id
            for contract in list_artifact_contracts()
            if contract.producer_stage_family == family_name
        )
        plan_stages.append(
            PlannedStage(
                stage_name=str(stage_name),
                stage_instance_id=str(stage_name),
                script_path=str(getattr(stage_def, "script_path", "")),
                base_args=tuple(str(arg) for arg in getattr(stage_def, "args", [])),
                reason_code="selected",
                stage_family=family_name,
                owner_service=family_contract.owner_service if family_contract is not None else "",
                artifact_inputs=artifact_surface.inputs if artifact_surface is not None else (),
                artifact_optional_inputs=(
                    artifact_surface.optional_inputs if artifact_surface is not None else ()
                ),
                artifact_outputs=artifact_surface.outputs if artifact_surface is not None else (),
                artifact_external_inputs=(
                    artifact_surface.external_inputs if artifact_surface is not None else ()
                ),
                required_artifact_contract_ids=required_artifact_contract_ids,
            )
        )

    for skipped in skipped_stage_specs or []:
        stage_name = str(skipped.get("stage_name", "") or "")
        family_contract = resolve_stage_family_contract(stage_name)
        plan_stages.append(
            PlannedStage(
                stage_name=stage_name,
                stage_instance_id=stage_name,
                script_path=str(skipped.get("script_path", "") or ""),
                base_args=(),
                reason_code="skipped",
                notes=str(skipped.get("notes", "") or ""),
                stage_family=family_contract.family if family_contract is not None else "",
                owner_service=family_contract.owner_service if family_contract is not None else "",
            )
        )

    obligations: list[PlannedArtifactObligation] = []
    for contract in list_artifact_contracts():
        producing_stages = tuple(
            selected_stage_families.get(contract.producer_stage_family, [])
        )
        if not producing_stages:
            continue
        if contract.contract_id in {"promoted_theses", "live_thesis_index"} and (
            "promote_candidates" not in producing_stages
        ):
            continue
        obligations.append(
            PlannedArtifactObligation(
                contract_id=contract.contract_id,
                producer_stage_family=contract.producer_stage_family,
                schema_id=contract.schema_id,
                schema_version=contract.schema_version,
                strictness=contract.strictness,
                required=contract.required,
                expected_path=contract.path_pattern.format(run_id=run_id),
                legacy_paths=tuple(
                    alias.format(run_id=run_id) for alias in contract.legacy_aliases
                ),
                producing_stage_names=producing_stages,
            )
        )

    timeframes = parse_timeframes_csv(getattr(args, "timeframes", "5m"))
    return ExecutionPlan(
        run_id=run_id,
        planned_at=planned_at or utc_now_iso(),
        stages=tuple(plan_stages),
        run_mode=str(getattr(args, "mode", "research") or "research"),
        symbols=tuple(parse_symbols_csv(getattr(args, "symbols", ""))),
        timeframe=",".join(timeframes),
        experiment_config=str(getattr(args, "experiment_config", "") or ""),
        registry_root=str(getattr(args, "registry_root", "") or ""),
        raw_args=dict(vars(args)),
        artifact_obligations=tuple(obligations),
    )


def _negative_control_summary_path(*, data_root: Path, run_id: str) -> Path:
    return data_root / "reports" / "negative_control" / run_id / "negative_control_summary.json"


def _strict_promotion_requires_negative_controls(args: argparse.Namespace) -> bool:
    if not int(getattr(args, "run_candidate_promotion", 0) or 0):
        return False
    if not int(getattr(args, "run_phase2_conditional", 0) or 0):
        return False
    if bool(int(getattr(args, "candidate_promotion_allow_missing_negative_controls", 0) or 0)):
        return False

    mode = str(getattr(args, "mode", "research") or "research").strip().lower()
    profile = str(getattr(args, "promotion_profile", "auto") or "auto").strip().lower()
    if profile == "auto":
        profile = str(getattr(args, "candidate_promotion_profile", "auto") or "auto").strip().lower()
    return mode in {"production", "certification"} or profile == "deploy"


def _validate_negative_control_contract(
    *,
    args: argparse.Namespace,
    run_id: str,
    stages: Mapping[str, Any],
    data_root: Path,
) -> list[str]:
    if not _strict_promotion_requires_negative_controls(args):
        return []

    if any("negative_control" in str(stage_name) for stage_name in stages.keys()):
        return []

    summary_path = _negative_control_summary_path(data_root=data_root, run_id=run_id)
    if summary_path.exists():
        return []

    return [
        "Strict candidate promotion requires negative-control evidence, but no "
        f"negative-control-producing stage is planned and {summary_path} does not exist. "
        "Either add negative-control production upstream or rerun with "
        "--candidate_promotion_allow_missing_negative_controls 1."
    ]


def compute_stage_instance_ids(
    stages: list[tuple[str, Path, list[str]]] | Mapping[str, Any],
) -> list[str]:
    """Computes unique instance IDs for stages, handling multiple occurrences of the same stage."""
    from project.pipelines.execution_engine import stage_instance_base

    counts: dict[str, int] = {}
    out: list[str] = []

    if isinstance(stages, Mapping):
        # For DAG, stage names are already unique keys
        return list(stages.keys())

    for stage, _, base_args in stages:
        base = stage_instance_base(stage, base_args)
        n = counts.get(base, 0) + 1
        counts[base] = n
        out.append(base if n == 1 else f"{base}__{n}")
    return out


def load_historical_universe(project_root: Path) -> list[str]:
    """Loads symbols from spec/historical_universe.csv."""
    path = project_root / "spec" / "historical_universe.csv"
    if not path.exists():
        return ["BTCUSDT"]
    try:
        import pandas as pd

        df = pd.read_csv(path)
        if "symbol" in df.columns:
            return [
                str(s).strip().upper() for s in df["symbol"].dropna().unique() if str(s).strip()
            ]
    except Exception:
        pass
    return ["BTCUSDT"]


def collect_startup_non_production_overrides(
    *,
    args: argparse.Namespace,
    existing_manifest_path: Path,
    allow_ontology_hash_mismatch: bool,
    existing_ontology_hash: str,
    ontology_hash: str,
) -> list[str]:
    """Collects overrides that are considered non-production."""
    overrides = []
    if (
        allow_ontology_hash_mismatch
        and existing_ontology_hash
        and existing_ontology_hash != ontology_hash
    ):
        overrides.append(
            f"allow_ontology_hash_mismatch: {existing_ontology_hash} -> {ontology_hash}"
        )

    if args.mode not in {"production", "research"}:
        overrides.append(f"mode: {args.mode}")

    override_fields = (
        "strategy_builder_allow_non_promoted",
        "strategy_builder_allow_missing_candidate_detail",
        "strategy_blueprint_allow_fallback",
        "strategy_blueprint_allow_non_executable_conditions",
        "strategy_blueprint_allow_naive_entry_fail",
        "promotion_allow_fallback_evidence",
    )
    for field in override_fields:
        raw = getattr(args, field, 0)
        try:
            enabled = bool(int(raw or 0))
        except (TypeError, ValueError):
            enabled = bool(raw)
        if enabled:
            overrides.append(f"{field}: {raw}")

    return overrides



def _discover_local_cleaned_coverage(
    *,
    args: argparse.Namespace,
    data_root: Path,
    parsed_symbols: list[str],
) -> dict[str, object]:
    timeframes = parse_timeframes_csv(getattr(args, "timeframes", "5m"))
    explicit_offline = bool(int(getattr(args, "offline_mode", 0) or 0))
    external_root = discover_external_cleaned_root(
        data_root,
        explicit_root=str(getattr(args, "offline_cleaned_root", "") or "").strip() or None,
    )
    result: dict[str, object] = {
        "external_root": str(external_root) if external_root is not None else "",
        "covered_perp_timeframes": [],
        "coverage_gaps": [],
        "materialized_paths": [],
        "issues": [],
        "explicit_offline": explicit_offline,
    }
    if external_root is None:
        if explicit_offline:
            result["issues"] = [
                "offline_mode requested but no external cleaned-bars root was found. "
                "Set --offline_cleaned_root or EDGE_OFFLINE_CLEANED_BARS_ROOT."
            ]
        return result

    covered_perp: list[str] = []
    coverage_gaps: list[str] = []
    materialized: list[str] = []
    for tf in timeframes:
        timeframe_ok = True
        for symbol in parsed_symbols:
            dataset_root = external_cleaned_dataset_dir(
                external_root,
                market="perp",
                symbol=symbol,
                timeframe=tf,
            )
            if not cleaned_dataset_covers_window(dataset_root, start=str(args.start), end=str(args.end)):
                timeframe_ok = False
                coverage_gaps.append(
                    f"perp/{symbol}/bars_{tf} does not cover requested window {args.start}..{args.end}"
                )
                continue
            unreadable_samples = unreadable_parquet_samples(dataset_root, limit=2)
            if unreadable_samples:
                timeframe_ok = False
                sample_text = ", ".join(str(path) for path in unreadable_samples)
                coverage_gaps.append(
                    f"perp/{symbol}/bars_{tf} uses native parquet bytes that are unreadable in the current runtime: {sample_text}"
                )
        if timeframe_ok:
            covered_perp.append(tf)
            for symbol in parsed_symbols:
                linked = materialize_external_cleaned_dataset(
                    data_root,
                    external_root,
                    market="perp",
                    symbol=symbol,
                    timeframe=tf,
                )
                if linked is not None:
                    materialized.append(str(linked))

    if explicit_offline:
        missing = [tf for tf in timeframes if tf not in covered_perp]
        if missing:
            result["issues"] = [
                "offline_mode requested but local cleaned bars do not fully satisfy the requested "
                f"timeframes {missing} for symbols {parsed_symbols}."
            ] + coverage_gaps

    result["covered_perp_timeframes"] = covered_perp
    result["coverage_gaps"] = coverage_gaps
    result["materialized_paths"] = sorted(set(materialized))
    return result


def _apply_local_cleaned_stage_shortcuts(
    *,
    stages: Mapping[str, Any],
    args: argparse.Namespace,
    local_cleaned: Mapping[str, object],
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    covered_perp = {str(tf) for tf in local_cleaned.get("covered_perp_timeframes", [])}
    if not covered_perp:
        return dict(stages), []

    external_root = str(local_cleaned.get("external_root", "") or "")
    notes = (
        "local cleaned bars satisfy the contract; stage skipped"
        + (f" (source={external_root})" if external_root else "")
    )

    pruned: dict[str, Any] = dict(stages)
    skipped: list[dict[str, str]] = []

    def _pop(name: str, *, reason: str = notes) -> None:
        stage_def = pruned.pop(name, None)
        if stage_def is None:
            return
        skipped.append(
            {
                "stage_name": str(name),
                "script_path": str(getattr(stage_def, "script_path", "")),
                "notes": reason,
            }
        )

    for tf in sorted(covered_perp):
        _pop(f"build_cleaned_{tf}")
        # If cleaned bars are already available for this timeframe, raw OHLCV ingest is unnecessary.
        _pop(f"ingest_bybit_derivatives_ohlcv_{tf}")
        _pop(f"ingest_binance_um_ohlcv_{tf}")

    # Funding ingest is only needed to support build_cleaned for perp. If all remaining
    # perp cleaned stages are satisfied locally, skip funding ingest too.
    if not any(name.startswith("build_cleaned_") and not name.endswith("_spot") for name in pruned):
        _pop(
            "ingest_bybit_derivatives_funding",
            reason=(
                "local cleaned bars satisfy perp feature prerequisites; funding ingest skipped"
                + (f" (source={external_root})" if external_root else "")
            ),
        )
        _pop(
            "ingest_binance_um_funding",
            reason=(
                "local cleaned bars satisfy perp feature prerequisites; funding ingest skipped"
                + (f" (source={external_root})" if external_root else "")
            ),
        )

    active_names = set(pruned.keys())
    for stage_def in pruned.values():
        deps = [dep for dep in getattr(stage_def, "depends_on", []) if dep in active_names]
        stage_def.depends_on = deps

    return pruned, skipped


def prepare_run_preflight(
    *,
    args: argparse.Namespace,
    project_root: Path,
    data_root: Path,
    cli_flag_present: Any,
    run_id_default: Any,
    script_supports_flag: Any,
) -> dict[str, object]:
    """Performs preflight checks and plans the pipeline execution."""
    run_id = args.run_id or run_id_default()

    # Resolve Symbols
    if args.symbols == "dynamic":
        parsed_symbols = load_historical_universe(project_root)
    else:
        parsed_symbols = parse_symbols_csv(args.symbols)

    if not args.start or not args.end:
        print(
            "ERROR: --start and --end date flags are required for all pipeline runs.",
            file=sys.stderr,
        )
        return {"exit_code": 2, "run_id": run_id}

    args.timeframes = ",".join(parse_timeframes_csv(getattr(args, "timeframes", "5m")))

    local_cleaned = _discover_local_cleaned_coverage(
        args=args,
        data_root=data_root,
        parsed_symbols=parsed_symbols,
    )
    if local_cleaned.get("issues"):
        for issue in list(local_cleaned.get("issues", [])):
            print(f"ERROR: {issue}", file=sys.stderr)
        return {"exit_code": 2, "run_id": run_id}

    if (
        not cli_flag_present("--enable_cross_venue_spot_pipeline")
        and _requires_cross_venue_spot_pipeline(args)
    ):
        args.enable_cross_venue_spot_pipeline = 0
        args.skip_ingest_spot_ohlcv = 1

    if bool(int(getattr(args, "performance_mode", 0) or 0)):
        if not cli_flag_present("--runtime_invariants_mode"):
            args.runtime_invariants_mode = "off"
        if not cli_flag_present("--emit_run_hash"):
            args.emit_run_hash = 0

    experiment_promotion_enabled = _experiment_promotion_enabled(args)
    if experiment_promotion_enabled is not None and not cli_flag_present("--run_candidate_promotion"):
        args.run_candidate_promotion = 1 if experiment_promotion_enabled else 0
        if not experiment_promotion_enabled and not cli_flag_present("--run_edge_registry_update"):
            args.run_edge_registry_update = 0

    if str(getattr(args, "mode", "research")).strip().lower() in {"production", "certification"}:
        if not cli_flag_present("--run_phase2_conditional"):
            args.run_phase2_conditional = 1
    hinted_events, _hinted_regimes = _experiment_trigger_hints(
        args,
        include_phase2_event_type=False,
    )
    if (
        int(getattr(args, "run_phase2_conditional", 0) or 0)
        and getattr(args, "templates", None)
        and not getattr(args, "events", None)
        and not cli_flag_present("--phase2_event_type")
    ):
        if len(hinted_events) == 1:
            args.phase2_event_type = next(iter(hinted_events))
        else:
            # Template-only runs should fan out over the canonical event chain unless the user
            # explicitly pins a single event family. Otherwise the parser default of VOL_SHOCK
            # silently narrows the run and breaks calibration parity with event-level reruns.
            args.phase2_event_type = "all"
    # When --events is specified without --phase2_event_type, pin the search engine to the
    # same event(s). A single-event list becomes the event type; multiple events use "all".
    if (
        getattr(args, "events", None)
        and not cli_flag_present("--phase2_event_type")
    ):
        events_list = [e.strip().upper() for e in args.events if str(e).strip()]
        if len(events_list) == 1:
            args.phase2_event_type = events_list[0]
        else:
            args.phase2_event_type = "all"

    expectancy_script = (
        project_root / "research" / "analyze_conditional_expectancy.py"
    )
    expectancy_tail_requested = any(
        int(getattr(args, attr, 0) or 0)
        for attr in (
            "run_expectancy_analysis",
            "run_expectancy_robustness",
            "run_recommendations_checklist",
            "run_strategy_blueprint_compiler",
            "run_strategy_builder",
        )
    )
    if expectancy_tail_requested and not expectancy_script.exists():
        print(
            "WARNING: Disabling recommendations checklist and expectancy robustness "
            "because analyze_conditional_expectancy.py is unavailable.",
            file=sys.stderr,
        )
        args.run_expectancy_analysis = 0
        args.run_expectancy_robustness = 0
        args.run_recommendations_checklist = 0
        args.run_strategy_blueprint_compiler = 0
        args.run_strategy_builder = 0

    if int(getattr(args, "run_recommendations_checklist", 0)):
        if not cli_flag_present("--run_expectancy_analysis"):
            args.run_expectancy_analysis = 1
        if not cli_flag_present("--run_expectancy_robustness"):
            args.run_expectancy_robustness = 1
    if expectancy_tail_requested and int(getattr(args, "run_phase2_conditional", 0)):
        if not cli_flag_present("--run_edge_registry_update"):
            args.run_edge_registry_update = 1
    if int(getattr(args, "run_candidate_promotion", 0)) and int(
        getattr(args, "run_phase2_conditional", 0)
    ):
        if not cli_flag_present("--run_edge_registry_update"):
            args.run_edge_registry_update = 1

    # Resolve Metadata
    objective_name = resolve_objective_name(args.objective_name)
    objective_spec, objective_spec_hash, objective_spec_path = objective_spec_metadata(
        objective_name, args.objective_spec
    )

    retail_profile_name = resolve_retail_profile_name(args.retail_profile)
    retail_profile, retail_profile_spec_hash, retail_profile_spec_path = retail_profile_metadata(
        retail_profile_name, args.retail_profiles_spec
    )
    args.phase2_gate_profile_resolved = (
        str(getattr(args, "phase2_gate_profile", "auto") or "auto").strip().lower()
    )

    viability_target_events, _ = _experiment_trigger_hints(args, include_phase2_event_type=True)
    feature_surface_viability = {
        "schema_version": "feature_surface_viability_v1",
        "status": "unknown",
        "event_types": sorted(viability_target_events),
        "symbols": {},
        "detectors": {},
        "issues": [],
    }
    if int(getattr(args, "run_phase2_conditional", 0) or 0) and viability_target_events and not int(getattr(args, "dry_run", 0) or 0):
        analysis_timeframe = parse_timeframes_csv(getattr(args, "timeframes", "5m"))[0]
        feature_surface_viability = analyze_feature_surface_viability(
            data_root=data_root,
            run_id=run_id,
            symbols=parsed_symbols,
            timeframe=analysis_timeframe,
            start=str(args.start),
            end=str(args.end),
            event_types=sorted(viability_target_events),
            market="perp",
        )
        if str(feature_surface_viability.get("status", "unknown") or "unknown").strip().lower() == "block":
            for event_name, payload in sorted(feature_surface_viability.get("detectors", {}).items()):
                blocked_symbols = list(payload.get("block_symbols", []))
                if blocked_symbols:
                    print(
                        f"ERROR: feature surface viability gate blocked detector {event_name} for symbols {blocked_symbols}.",
                        file=sys.stderr,
                    )
            for issue in list(feature_surface_viability.get("issues", [])):
                print(f"ERROR: {issue}", file=sys.stderr)
            return {
                "exit_code": 2,
                "run_id": run_id,
                "local_cleaned": local_cleaned,
        "feature_surface_viability": feature_surface_viability,
                "feature_surface_viability": feature_surface_viability,
            }

    # Build Plan
    stages = build_pipeline_plan(
        args=args,
        run_id=run_id,
        symbols=",".join(parsed_symbols),
        start=args.start,
        end=args.end,
        run_spot_pipeline=bool(args.enable_cross_venue_spot_pipeline),
        research_gate_profile="discovery" if args.mode == "research" else "promotion",
        project_root=project_root,
        data_root=data_root,
        phase2_event_chain=PHASE2_EVENT_CHAIN,
        script_supports_flag=script_supports_flag,
        retail_profile_name=retail_profile_name,
    )
    stages, skipped_stage_specs = _apply_local_cleaned_stage_shortcuts(
        stages=stages,
        args=args,
        local_cleaned=local_cleaned,
    )
    artifact_contracts, artifact_contract_issues = resolve_pipeline_artifact_contracts(stages)
    artifact_contract_issues.extend(
        _validate_negative_control_contract(
            args=args,
            run_id=run_id,
            stages=stages,
            data_root=data_root,
        )
    )

    if isinstance(stages, dict):
        stage_names = [str(name) for name in stages.keys()]
    else:
        stage_names = [str(stage[0]) for stage in stages]
    runs_search_engine = "phase2_search_engine" in stage_names
    runs_legacy_phase2_conditional = any(
        name.startswith("phase2_conditional_hypotheses__") for name in stage_names
    )
    phase2_event_type_source = "explicit" if cli_flag_present("--phase2_event_type") else "implicit"
    if (
        not cli_flag_present("--phase2_event_type")
        and int(getattr(args, "run_phase2_conditional", 0) or 0)
        and getattr(args, "templates", None)
        and not getattr(args, "events", None)
        and len(hinted_events) == 1
    ):
        phase2_event_type_source = "experiment_config_event_pin"
    elif (
        not cli_flag_present("--phase2_event_type")
        and int(getattr(args, "run_phase2_conditional", 0) or 0)
        and getattr(args, "templates", None)
        and not getattr(args, "events", None)
        and str(getattr(args, "phase2_event_type", "")).strip().lower() == "all"
    ):
        phase2_event_type_source = "template_only_auto_widen"
    elif not cli_flag_present("--phase2_event_type"):
        phase2_event_type_source = "parser_default_or_config"

    effective_behavior = {
        "phase2_event_type": str(getattr(args, "phase2_event_type", "") or "").strip(),
        "phase2_event_type_source": phase2_event_type_source,
        "run_expectancy_analysis": bool(int(getattr(args, "run_expectancy_analysis", 0) or 0)),
        "run_expectancy_robustness": bool(
            int(getattr(args, "run_expectancy_robustness", 0) or 0)
        ),
        "run_recommendations_checklist": bool(
            int(getattr(args, "run_recommendations_checklist", 0) or 0)
        ),
        "run_strategy_builder": bool(int(getattr(args, "run_strategy_builder", 0) or 0)),
        "run_strategy_blueprint_compiler": bool(
            int(getattr(args, "run_strategy_blueprint_compiler", 0) or 0)
        ),
        "runs_search_engine": runs_search_engine,
        "runs_legacy_phase2_conditional": runs_legacy_phase2_conditional,
    }

    return {
        "exit_code": None,
        "run_id": run_id,
        "stages": stages,
        "parsed_symbols": parsed_symbols,
        "ontology_hash": ontology_spec_hash(project_root.parent),
        "runtime_invariants_mode": args.runtime_invariants_mode,
        "emit_run_hash_requested": bool(args.emit_run_hash),
        "determinism_replay_checks_requested": bool(args.determinism_replay_checks),
        "oms_replay_checks_requested": bool(args.oms_replay_checks),
        "objective_name": objective_name,
        "objective_spec_hash": objective_spec_hash,
        "objective_spec_path": objective_spec_path,
        "retail_profile_name": retail_profile_name,
        "retail_profile": retail_profile,
        "retail_profile_spec_hash": retail_profile_spec_hash,
        "retail_profile_spec_path": retail_profile_spec_path,
        "runtime_invariants_status": "configured",
        "artifact_contracts": artifact_contracts,
        "artifact_contract_issues": artifact_contract_issues,
        "effective_behavior": effective_behavior,
        "execution_requested": True,
        "local_cleaned": local_cleaned,
        "skipped_stage_specs": skipped_stage_specs,
        "search_spec": getattr(args, "search_spec", "spec/search_space.yaml"),
        "research_compare_baseline_run_id": str(
            getattr(args, "research_compare_baseline_run_id", "") or ""
        ).strip(),
        "research_compare_drift_mode": str(
            getattr(args, "research_compare_drift_mode", "warn") or "warn"
        ).strip(),
        "research_compare_thresholds": {
            "max_phase2_candidate_count_delta_abs": float(
                getattr(args, "research_compare_max_phase2_candidate_count_delta_abs", 10.0)
            ),
            "max_phase2_survivor_count_delta_abs": float(
                getattr(args, "research_compare_max_phase2_survivor_count_delta_abs", 2.0)
            ),
            "max_phase2_zero_eval_rows_increase": float(
                getattr(args, "research_compare_max_phase2_zero_eval_rows_increase", 0.0)
            ),
            "max_phase2_survivor_q_value_increase": float(
                getattr(args, "research_compare_max_phase2_survivor_q_value_increase", 0.05)
            ),
            "max_phase2_survivor_estimate_bps_drop": float(
                getattr(args, "research_compare_max_phase2_survivor_estimate_bps_drop", 3.0)
            ),
            "max_promotion_promoted_count_delta_abs": float(
                getattr(args, "research_compare_max_promotion_promoted_count_delta_abs", 2.0)
            ),
            "max_reject_reason_shift_abs": float(
                getattr(args, "research_compare_max_reject_reason_shift_abs", 3.0)
            ),
            "max_edge_tradable_count_delta_abs": float(
                getattr(args, "research_compare_max_edge_tradable_count_delta_abs", 2.0)
            ),
            "max_edge_candidate_count_delta_abs": float(
                getattr(args, "research_compare_max_edge_candidate_count_delta_abs", 2.0)
            ),
            "max_edge_after_cost_positive_validation_count_delta_abs": float(
                getattr(
                    args,
                    "research_compare_max_edge_after_cost_positive_validation_count_delta_abs",
                    2.0,
                )
            ),
            "max_edge_median_resolved_cost_bps_delta_abs": float(
                getattr(args, "research_compare_max_edge_median_resolved_cost_bps_delta_abs", 0.25)
            ),
            "max_edge_median_expectancy_bps_delta_abs": float(
                getattr(args, "research_compare_max_edge_median_expectancy_bps_delta_abs", 0.25)
            ),
        },
        "normalized_timeframes_csv": args.timeframes,
        "start": args.start,
        "end": args.end,
    }
