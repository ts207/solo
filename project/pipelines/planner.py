from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Tuple, Dict, Mapping

from project.pipelines.stages import (
    build_core_stages,
    build_evaluation_stages,
    build_ingest_stages,
    build_research_stages,
)
from project.pipelines.stage_registry import assert_stage_registry_contract


@dataclass
class StageDefinition:
    name: str
    script_path: Path
    args: List[str]
    depends_on: List[str] = field(default_factory=list)


def _upsert_cli_flag(base_args: List[str], flag: str, value: str) -> None:
    try:
        idx = base_args.index(flag)
    except ValueError:
        base_args.extend([flag, value])
        return
    if idx + 1 < len(base_args):
        base_args[idx + 1] = value
    else:
        base_args.append(value)


# Explicit Dependency Mapping
# Patterns can use {tf} as a placeholder for timeframe, {event} for event type.
# String literals match exactly.
# `phase2_search_engine` is the canonical planner-owned phase-2 discovery stage.
# Legacy per-event phase-2 stage names remain only for compatibility with
# historical artifacts, replay tooling, and old tests.
DEPENDENCY_PATTERNS: List[Tuple[str, List[str]]] = [
    ("build_cleaned_{tf}", ["@OHLCV_INGEST_STAGES"]),
    ("build_features_{tf}", ["build_cleaned_{tf}", "@FUNDING_INGEST_STAGES"]),
    ("build_features_{tf}_spot", ["build_cleaned_{tf}_spot"]),
    ("build_universe_snapshots", ["@CLEANED_STAGES"]),
    ("build_market_context_{tf}", ["build_features_{tf}"]),
    ("build_microstructure_rollup_{tf}", ["build_features_{tf}"]),
    ("validate_feature_integrity_{tf}", ["build_features_{tf}"]),
    ("validate_data_coverage_{tf}", ["build_cleaned_{tf}"]),
    ("build_normalized_replay_stream", ["build_universe_snapshots"]),
    ("run_causal_lane_ticks", ["build_normalized_replay_stream"]),
    ("run_determinism_replay_checks", ["run_causal_lane_ticks"]),
    ("run_oms_replay_validation", ["run_causal_lane_ticks"]),
    # Research Chain
    ("analyze_{script}__{event}_{tf}", ["build_features_{tf}", "build_market_context_{tf}"]),
    ("analyze_events__{event}_{tf}", ["build_features_{tf}", "build_market_context_{tf}"]),
    ("build_event_registry__{event}_{tf}", ["@ANALYZERS_FOR_EVENT"]),
    ("canonicalize_event_episodes__{event}_{tf}", ["build_event_registry__{event}_{tf}"]),
    ("phase2_conditional_hypotheses__{event}_{tf}", ["canonicalize_event_episodes__{event}_{tf}"]),
    ("bridge_evaluate_phase2__{event}_{tf}", ["phase2_conditional_hypotheses__{event}_{tf}"]),
    ("phase2_search_engine", ["@MARKET_CONTEXT_STAGES", "@REGISTRY_STAGES"]),
    ("analyze_interaction_lift", ["@PHASE2_DISCOVERY_STAGES"]),
    # Global Stages
    ("phase1_correlation_clustering", ["@REGISTRY_STAGES"]),
    ("export_edge_candidates", ["@PHASE2_DISCOVERY_STAGES"]),
    ("generate_negative_control_summary", ["export_edge_candidates"]),
    ("promote_candidates", ["generate_negative_control_summary"]),
    ("update_edge_registry", ["export_edge_candidates", "promote_candidates"]),
    ("update_campaign_memory", ["update_edge_registry", "export_edge_candidates"]),
    ("analyze_conditional_expectancy", ["update_edge_registry"]),
    ("validate_expectancy_traps", ["analyze_conditional_expectancy"]),
    ("generate_recommendations_checklist", ["validate_expectancy_traps"]),
    ("summarize_discovery_quality", ["@PHASE2_DISCOVERY_STAGES"]),
    ("evaluate_naive_entry", ["@PHASE2_DISCOVERY_STAGES"]),
    (
        "finalize_experiment",
        [
            "@PHASE2_DISCOVERY_STAGES",
            "summarize_discovery_quality",
            "update_campaign_memory",
            "generate_recommendations_checklist",
            "analyze_interaction_lift",
        ],
    ),
    ("select_profitable_strategies", ["build_strategy_candidates"]),
    ("compile_strategy_blueprints", ["promote_candidates", "generate_recommendations_checklist"]),
    ("build_strategy_candidates", ["compile_strategy_blueprints"]),
]


def _resolve_dependencies(name: str, all_stage_names: List[str]) -> List[str]:
    """Resolve dependencies for a stage name based on patterns and special tokens."""
    deps = []

    sep_idx = name.find("__")
    prefix_part = name[:sep_idx] if sep_idx >= 0 else name
    suffix_part = name[sep_idx + 2 :] if sep_idx >= 0 else ""

    # Parse name for tf and event
    tf = next(
        (p for p in name.split("_") if p in {"1m", "5m", "15m", "30m", "1h", "4h", "1d"}), "5m"
    )
    event = None
    if sep_idx >= 0:
        # e.g. analyze_vol_shock_relaxation__VOL_SHOCK_5m
        event_part = suffix_part
        if tf in event_part:
            event = event_part.rsplit("_", 1)[0]
        else:
            event = event_part

    # 1. Pattern-based resolution
    for pattern, dep_patterns in DEPENDENCY_PATTERNS:
        if "{tf}" in pattern or "{event}" in pattern or "{script}" in pattern:
            if "{script}" in pattern and name.startswith("analyze_") and sep_idx >= 0:
                script_part = prefix_part.replace("analyze_", "")
                if pattern.startswith("analyze_{script}__"):
                    for dp in dep_patterns:
                        deps.append(
                            dp.replace("{script}", script_part)
                            .replace("{event}", event or "")
                            .replace("{tf}", tf)
                        )
            elif "{event}" in pattern:
                p_base = pattern.split("{event}")[0]
                if name.startswith(p_base):
                    # If we already extracted event from __, use it, otherwise extract it
                    extracted_event = event or name.replace(p_base, "").split("_")[0]
                    for dp in dep_patterns:
                        deps.append(dp.replace("{event}", extracted_event).replace("{tf}", tf))
            elif "{tf}" in pattern:
                suffix = pattern.split("{tf}")[1]
                prefix = pattern.split("{tf}")[0]
                # Ensure spot stages don't match perp patterns by prefix
                if name.startswith(prefix) and name.endswith(suffix):
                    if (
                        not suffix.endswith("_spot")
                        and name.endswith("_spot")
                        and not pattern.endswith("_spot")
                    ):
                        continue
                    for dp in dep_patterns:
                        deps.append(dp.replace("{tf}", tf))
        elif name == pattern:
            deps.extend(dep_patterns)

    # 2. Special Token Resolution
    resolved = []
    for d in deps:
        if d == "@REGISTRY_STAGES":
            resolved.extend([n for n in all_stage_names if "build_event_registry" in n])
        elif d == "@BRIDGE_STAGES":
            resolved.extend([n for n in all_stage_names if "bridge_evaluate_phase2" in n])
        elif d == "@PHASE2_DISCOVERY_STAGES":
            resolved.extend(
                [
                    n
                    for n in all_stage_names
                    if "phase2_conditional_hypotheses" in n
                    or "bridge_evaluate_phase2" in n
                    or n == "phase2_search_engine"
                ]
            )
        elif d == "@ANALYZERS_FOR_EVENT":
            # name is e.g. build_event_registry__VOL_SHOCK_5m
            if event:
                resolved.extend(
                    [n for n in all_stage_names if n.startswith("analyze_") and f"__{event}_" in n]
                )
            else:
                # Fallback extraction if event wasn't parsed via __
                event_part = name.replace("build_event_registry_", "").rsplit("_", 1)[0].lstrip("_")
                resolved.extend(
                    [
                        n
                        for n in all_stage_names
                        if n.startswith("analyze_") and f"_{event_part}_" in n
                    ]
                )
        elif d == "@OHLCV_INGEST_STAGES":
            resolved.extend(
                [
                    n
                    for n in all_stage_names
                    if (
                        n.startswith("ingest_binance_um_ohlcv_")
                        or n.startswith("ingest_bybit_derivatives_ohlcv_")
                    )
                    and n.endswith(f"_{tf}")
                ]
            )
        elif d == "@FUNDING_INGEST_STAGES":
            resolved.extend(
                [
                    n
                    for n in all_stage_names
                    if n.startswith("ingest_binance_um_funding")
                    or n.startswith("ingest_bybit_derivatives_funding")
                ]
            )
        elif d == "@FIRST_OHLCV_STAGE":
            ohlcv_stages = sorted(
                [n for n in all_stage_names if n.startswith("ingest_binance_um_ohlcv_")]
            )
            if ohlcv_stages:
                resolved.append(ohlcv_stages[0])
        elif d == "@CLEANED_STAGES":
            resolved.extend(
                [
                    n
                    for n in all_stage_names
                    if n.startswith("build_cleaned_") and not n.endswith("_spot")
                ]
            )
        elif d == "@MARKET_CONTEXT_STAGES":
            resolved.extend([n for n in all_stage_names if n.startswith("build_market_context_")])
        else:
            resolved.append(d)

    # 3. Serialization logic to prevent race conditions
    # Serialize analyzers for the same script/family
    if name.startswith("analyze_"):
        family_prefix = prefix_part if sep_idx >= 0 else name.rsplit("_", 1)[0]
        family_stages = sorted(
            [n for n in all_stage_names if n.startswith(family_prefix) and n.endswith(f"_{tf}")]
        )
        try:
            idx = family_stages.index(name)
            if idx > 0:
                resolved.append(family_stages[idx - 1])
        except ValueError:
            pass

    # Serialize registry stages
    if "build_event_registry" in name:
        registry_stages = sorted([n for n in all_stage_names if "build_event_registry" in n])
        try:
            idx = registry_stages.index(name)
            if idx > 0:
                resolved.append(registry_stages[idx - 1])
        except ValueError:
            pass

    # Serialize canonicalize stages
    if "canonicalize_event_episodes" in name:
        canonical_stages = sorted(
            [n for n in all_stage_names if "canonicalize_event_episodes" in n]
        )
        try:
            idx = canonical_stages.index(name)
            if idx > 0:
                resolved.append(canonical_stages[idx - 1])
        except ValueError:
            pass

    # Serialize bridge evaluate stages (they write to the same symbol-scoped file)
    if "bridge_evaluate_phase2" in name:
        bridge_stages = sorted([n for n in all_stage_names if "bridge_evaluate_phase2" in n])
        try:
            idx = bridge_stages.index(name)
            if idx > 0:
                resolved.append(bridge_stages[idx - 1])
        except ValueError:
            pass

    # Filter for actually present stages
    return sorted(list(set(d for d in resolved if d in all_stage_names and d != name)))


def build_pipeline_plan(
    *,
    args,
    run_id: str,
    symbols: str,
    start: str,
    end: str,
    run_spot_pipeline: bool,
    research_gate_profile: str,
    project_root: Path,
    data_root: Path,
    phase2_event_chain: List[Tuple[str, str, List[str]]],
    script_supports_flag: Callable[[Path, str], bool],
    retail_profile_name: str,
) -> Dict[str, StageDefinition]:
    force_flag = str(int(getattr(args, "force", 0) or 0))
    # Collect all stages
    raw_stages = []
    raw_stages.extend(
        build_ingest_stages(
            args=args,
            run_id=run_id,
            symbols=symbols,
            start=start,
            end=end,
            force_flag=force_flag,
            run_spot_pipeline=run_spot_pipeline,
            project_root=project_root,
        )
    )
    raw_stages.extend(
        build_core_stages(
            args=args,
            run_id=run_id,
            symbols=symbols,
            start=start,
            end=end,
            run_spot_pipeline=run_spot_pipeline,
            project_root=project_root,
        )
    )
    raw_stages.extend(
        build_research_stages(
            args=args,
            run_id=run_id,
            symbols=symbols,
            start=start,
            end=end,
            research_gate_profile=research_gate_profile,
            project_root=project_root,
            data_root=data_root,
            phase2_event_chain=phase2_event_chain,
        )
    )
    raw_stages.extend(
        build_evaluation_stages(
            args=args,
            run_id=run_id,
            symbols=symbols,
            start=start,
            end=end,
            force_flag=force_flag,
            project_root=project_root,
            data_root=data_root,
        )
    )

    all_stage_names = [s[0] for s in raw_stages]
    plan: Dict[str, StageDefinition] = {}

    for name, path, s_args in raw_stages:
        deps = _resolve_dependencies(name, all_stage_names)
        plan[name] = StageDefinition(name=name, script_path=path, args=s_args, depends_on=deps)

    # Global Post-processing for configs and flags
    stages_with_config_prefixes = (
        "build_cleaned_",
        "build_features",
        "build_market_context",
    )
    for name, stage in plan.items():
        base_args = stage.args
        stage_script_path = stage.script_path

        if script_supports_flag(stage_script_path, "--retail_profile"):
            _upsert_cli_flag(base_args, "--retail_profile", retail_profile_name)

        if (
            name.startswith(stages_with_config_prefixes)
            or name == "compile_strategy_blueprints"
            or "phase2_conditional_hypotheses" in name
        ):
            for config_path in args.config:
                if "--config" not in base_args or config_path not in base_args:
                    base_args.extend(["--config", config_path])

        if "build_market_context" in name:
            if script_supports_flag(stage_script_path, "--max_symbol_workers"):
                _upsert_cli_flag(
                    base_args, "--max_symbol_workers", str(int(args.market_context_workers))
                )

        if name == "select_profitable_strategies":
            if script_supports_flag(stage_script_path, "--objective_name"):
                _upsert_cli_flag(
                    base_args, "--objective_name", str(getattr(args, "objective_name", "") or "")
                )

        if (
            name.startswith("phase2_conditional_hypotheses")
            or name == "compile_strategy_blueprints"
        ):
            if args.fees_bps is not None:
                base_args.extend(["--fees_bps", str(args.fees_bps)])
            if args.slippage_bps is not None:
                base_args.extend(["--slippage_bps", str(args.slippage_bps)])
            if args.cost_bps is not None:
                base_args.extend(["--cost_bps", str(args.cost_bps)])

    # Audit 3.3: Explicit topological sort and cycle detection to validate the DAG
    _validate_pipeline_dag(plan)

    # Return plain list for registry contract check but return plan for orchestration
    assert_stage_registry_contract(
        [(s.name, s.script_path, s.args) for s in plan.values()], project_root
    )
    return plan


def _validate_pipeline_dag(plan: Dict[str, StageDefinition]) -> None:
    """Perform cycle detection and ensure a valid topological order exists."""
    visited = set()
    stack = set()

    def has_cycle(u: str) -> bool:
        visited.add(u)
        stack.add(u)
        for v in plan[u].depends_on:
            if v not in visited:
                if has_cycle(v):
                    return True
            elif v in stack:
                return True
        stack.remove(u)
        return False

    for node in plan:
        if node not in visited:
            if has_cycle(node):
                raise ValueError(f"Pipeline dependency cycle detected involving stage: {node}")
