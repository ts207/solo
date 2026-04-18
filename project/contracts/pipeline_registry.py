from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Dict, List, Sequence, Set, Tuple

from project.core.exceptions import ContractViolationError
from project.core.timeframes import (
    DEFAULT_TIMEFRAME,
    SUPPORTED_TIMEFRAMES,
    make_clean_artifact_token,
    make_feature_artifact_token,
    make_funding_artifact_token,
    make_ohlcv_artifact_token,
    make_spot_ohlcv_artifact_token,
    normalize_timeframe,
    parse_timeframes,
)

StageSpec = Tuple[str, str, List[str]]


@dataclass(frozen=True)
class StageFamilyContract:
    family: str
    stage_patterns: tuple[str, ...]
    script_patterns: tuple[str, ...]


_VALID_STRICTNESS = frozenset({"strict", "transitional", "legacy_compatible", "advisory"})


@dataclass(frozen=True)
class StageArtifactContract:
    stage_patterns: tuple[str, ...]
    inputs: tuple[str, ...] = ()
    optional_inputs: tuple[str, ...] = ()
    outputs: tuple[str, ...] = ()
    external_inputs: tuple[str, ...] = ()
    strictness: str = "strict"

    def __post_init__(self) -> None:
        if self.strictness not in _VALID_STRICTNESS:
            raise ValueError(
                f"StageArtifactContract strictness {self.strictness!r} must be one of {sorted(_VALID_STRICTNESS)}"
            )


@dataclass(frozen=True)
class ResolvedStageArtifactContract:
    inputs: tuple[str, ...]
    optional_inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    external_inputs: tuple[str, ...]


@dataclass(frozen=True)
class StageTimeframeArtifactMapping:
    stage_name: str
    script_path: str
    timeframe: str
    outputs: tuple[str, ...]


def make_ingest_ohlcv_contract(timeframe: str) -> StageArtifactContract:
    return StageArtifactContract(
        stage_patterns=(f"ingest_binance_um_ohlcv_{timeframe}",),
        outputs=(make_ohlcv_artifact_token(timeframe),),
    )


def make_ingest_spot_ohlcv_contract(timeframe: str) -> StageArtifactContract:
    return StageArtifactContract(
        stage_patterns=(f"ingest_binance_spot_ohlcv_{timeframe}",),
        outputs=(make_spot_ohlcv_artifact_token(timeframe),),
    )


def make_build_cleaned_contract(timeframe: str) -> StageArtifactContract:
    return StageArtifactContract(
        stage_patterns=(f"build_cleaned_{timeframe}",),
        inputs=(make_ohlcv_artifact_token(timeframe),),
        outputs=(make_clean_artifact_token(timeframe, market="perp"),),
        external_inputs=(make_ohlcv_artifact_token(timeframe),),
    )


def make_build_cleaned_spot_contract(timeframe: str) -> StageArtifactContract:
    return StageArtifactContract(
        stage_patterns=(f"build_cleaned_{timeframe}_spot",),
        inputs=(make_spot_ohlcv_artifact_token(timeframe),),
        outputs=(make_clean_artifact_token(timeframe, market="spot"),),
        external_inputs=(make_spot_ohlcv_artifact_token(timeframe),),
    )


def make_build_features_contract(timeframe: str) -> StageArtifactContract:
    return StageArtifactContract(
        stage_patterns=(f"build_features_{timeframe}",),
        inputs=(make_clean_artifact_token(timeframe, market="perp"),),
        optional_inputs=(make_funding_artifact_token(timeframe),),
        outputs=(make_feature_artifact_token(timeframe, market="perp"),),
        external_inputs=(
            make_clean_artifact_token(timeframe, market="perp"),
            make_funding_artifact_token(timeframe),
        ),
    )


def make_build_features_spot_contract(timeframe: str) -> StageArtifactContract:
    return StageArtifactContract(
        stage_patterns=(f"build_features_{timeframe}_spot",),
        inputs=(make_clean_artifact_token(timeframe, market="spot"),),
        outputs=(make_feature_artifact_token(timeframe, market="spot"),),
        external_inputs=(make_clean_artifact_token(timeframe, market="spot"),),
    )


def make_ingest_bybit_derivatives_ohlcv_contract(timeframe: str) -> StageArtifactContract:
    return StageArtifactContract(
        stage_patterns=(f"ingest_bybit_derivatives_ohlcv_{timeframe}",),
        outputs=(make_ohlcv_artifact_token(timeframe),),
    )


def make_ingest_bybit_derivatives_funding_contract(timeframe: str) -> StageArtifactContract:
    return StageArtifactContract(
        stage_patterns=("ingest_bybit_derivatives_funding",),
        outputs=(make_funding_artifact_token(timeframe),),
    )


def get_timeframe_aware_contracts(timeframes: List[str]) -> List[StageArtifactContract]:
    contracts: List[StageArtifactContract] = []
    for tf in timeframes:
        contracts.extend(
            [
                make_ingest_ohlcv_contract(tf),
                make_ingest_bybit_derivatives_ohlcv_contract(tf),
                # ingest_bybit_derivatives_funding is not per-timeframe; it is registered
                # once in build_timeframe_artifact_contracts and STAGE_ARTIFACT_REGISTRY.
                make_ingest_spot_ohlcv_contract(tf),
                make_build_cleaned_contract(tf),
                make_build_cleaned_spot_contract(tf),
                make_build_features_contract(tf),
                make_build_features_spot_contract(tf),
            ]
        )
    return contracts


STAGE_FAMILY_REGISTRY: tuple[StageFamilyContract, ...] = (
    StageFamilyContract(
        family="ingest",
        stage_patterns=(
            "ingest_binance_um_ohlcv_*",
            "ingest_binance_um_funding",
            "ingest_binance_spot_ohlcv_*",
            "ingest_binance_um_liquidation_snapshot",
            "ingest_binance_um_open_interest_hist",
            "ingest_bybit_derivatives_ohlcv_*",
            "ingest_bybit_derivatives_funding",
            "ingest_bybit_derivatives_oi",
        ),
        script_patterns=(
            "pipelines/ingest/ingest_binance_um_ohlcv*.py",
            "pipelines/ingest/ingest_binance_um_funding.py",
            "pipelines/ingest/ingest_binance_spot_ohlcv*.py",
            "pipelines/ingest/ingest_binance_um_liquidation_snapshot.py",
            "pipelines/ingest/ingest_binance_um_open_interest_hist.py",
            "pipelines/ingest/ingest_bybit_derivatives_ohlcv.py",
            "pipelines/ingest/ingest_bybit_derivatives_funding.py",
            "pipelines/ingest/ingest_bybit_derivatives_open_interest.py",
        ),
    ),
    StageFamilyContract(
        family="core",
        stage_patterns=(
            "build_cleaned_*",
            "build_features*",
            "build_universe_snapshots",
            "build_market_context*",
            "build_microstructure_rollup*",
            "validate_feature_integrity*",
            "validate_data_coverage*",
            "validate_context_entropy",
        ),
        script_patterns=(
            "pipelines/clean/build_cleaned_bars.py",
            "pipelines/features/build_features.py",
            "pipelines/ingest/build_universe_snapshots.py",
            "pipelines/features/build_market_context.py",
            "pipelines/features/build_microstructure_rollup.py",
            "pipelines/clean/validate_feature_integrity.py",
            "pipelines/clean/validate_data_coverage.py",
            "pipelines/clean/validate_context_entropy.py",
        ),
    ),
    StageFamilyContract(
        family="runtime_invariants",
        stage_patterns=(
            "build_normalized_replay_stream",
            "run_causal_lane_ticks",
            "run_determinism_replay_checks",
            "run_oms_replay_validation",
        ),
        script_patterns=(
            "pipelines/runtime/build_normalized_replay_stream.py",
            "pipelines/runtime/run_causal_lane_ticks.py",
            "pipelines/runtime/run_determinism_replay_checks.py",
            "pipelines/runtime/run_oms_replay_validation.py",
        ),
    ),
    StageFamilyContract(
        family="phase1_analysis",
        stage_patterns=("analyze_*", "phase1_correlation_clustering"),
        script_patterns=(
            "research/analyze_*.py",
            "research/phase1_correlation_clustering.py",
        ),
    ),
    StageFamilyContract(
        family="phase2_event_registry",
        stage_patterns=("build_event_registry*", "canonicalize_event_episodes*"),
        script_patterns=(
            "research/build_event_registry.py",
            "research/canonicalize_event_episodes.py",
        ),
    ),
    StageFamilyContract(
        family="phase2_discovery",
        stage_patterns=(
            "phase2_search_engine",
            "summarize_discovery_quality",
            "analyze_interaction_lift",
            "finalize_experiment",
        ),
        script_patterns=(
            "research/phase2_search_engine.py",
            "research/summarize_discovery_quality.py",
            "research/analyze_interaction_lift.py",
            "research/finalize_experiment.py",
        ),
    ),
    StageFamilyContract(
        family="promotion",
        stage_patterns=(
            "evaluate_naive_entry",
            "generate_negative_control_summary",
            "promote_candidates",
            "update_edge_registry",
            "update_campaign_memory",
            "export_edge_candidates",
        ),
        script_patterns=(
            "research/evaluate_naive_entry.py",
            "research/generate_negative_control_summary.py",
            "research/cli/promotion_cli.py",
            "research/update_edge_registry.py",
            "research/update_campaign_memory.py",
            "research/export_edge_candidates.py",
        ),
    ),
    StageFamilyContract(
        family="research_quality",
        stage_patterns=(
            "analyze_conditional_expectancy",
            "validate_expectancy_traps",
            "generate_recommendations_checklist",
        ),
        script_patterns=(
            "research/analyze_conditional_expectancy.py",
            "research/validate_expectancy_traps.py",
            "research/generate_recommendations_checklist.py",
        ),
    ),
    StageFamilyContract(
        family="strategy_packaging",
        stage_patterns=(
            "compile_strategy_blueprints",
            "build_strategy_candidates",
            "select_profitable_strategies",
        ),
        script_patterns=(
            "research/compile_strategy_blueprints.py",
            "research/build_strategy_candidates.py",
            "research/select_profitable_strategies.py",
        ),
    ),
)


STAGE_ARTIFACT_REGISTRY: tuple[StageArtifactContract, ...] = (
    StageArtifactContract(
        stage_patterns=("ingest_binance_um_funding",), outputs=("raw.perp.funding_5m",)
    ),
    StageArtifactContract(
        stage_patterns=("ingest_bybit_derivatives_funding",), outputs=("raw.perp.funding_5m",)
    ),
    StageArtifactContract(
        stage_patterns=("ingest_binance_um_liquidation_snapshot",),
        outputs=("raw.perp.liquidations",),
    ),
    StageArtifactContract(
        stage_patterns=("ingest_binance_um_open_interest_hist",),
        outputs=("raw.perp.open_interest",),
    ),
    StageArtifactContract(
        stage_patterns=("ingest_bybit_derivatives_oi",),
        outputs=("raw.perp.open_interest",),
    ),
    StageArtifactContract(
        stage_patterns=("ingest_binance_um_ohlcv_{tf}",), outputs=("raw.perp.ohlcv_{tf}",)
    ),
    StageArtifactContract(
        stage_patterns=("ingest_bybit_derivatives_ohlcv_{tf}",), outputs=("raw.perp.ohlcv_{tf}",)
    ),
    StageArtifactContract(
        stage_patterns=("ingest_binance_spot_ohlcv_{tf}",), outputs=("raw.spot.ohlcv_{tf}",)
    ),
    StageArtifactContract(
        stage_patterns=("build_cleaned_{tf}",),
        inputs=("raw.perp.ohlcv_{tf}",),
        outputs=("clean.perp.*",),
        external_inputs=("raw.perp.ohlcv_{tf}",),
    ),
    StageArtifactContract(
        stage_patterns=("build_cleaned_{tf}_spot",),
        inputs=("raw.spot.ohlcv_{tf}",),
        outputs=("clean.spot.*",),
        external_inputs=("raw.spot.ohlcv_{tf}",),
    ),
    StageArtifactContract(
        stage_patterns=("build_features_{tf}",),
        inputs=("clean.perp.*",),
        optional_inputs=(
            "raw.perp.funding_{tf}",
            "raw.perp.liquidations",
            "raw.perp.open_interest",
        ),
        outputs=("features.perp.v2",),
        external_inputs=(
            "clean.perp.*",
            "raw.perp.funding_{tf}",
            "raw.perp.liquidations",
            "raw.perp.open_interest",
        ),
    ),
    StageArtifactContract(
        stage_patterns=("build_features_{tf}_spot",),
        inputs=("clean.spot.*",),
        outputs=("features.spot.v2",),
        external_inputs=("clean.spot.*",),
    ),
    StageArtifactContract(
        stage_patterns=("build_universe_snapshots",),
        inputs=("clean.perp.*",),
        outputs=("metadata.universe_snapshots",),
        external_inputs=("clean.perp.*",),
    ),
    StageArtifactContract(
        stage_patterns=("build_market_context*",),
        inputs=("features.perp.v2",),
        outputs=("context.market_state",),
        external_inputs=("features.perp.v2",),
    ),
    StageArtifactContract(
        stage_patterns=("build_microstructure_rollup*",),
        inputs=("features.perp.v2",),
        outputs=("context.microstructure",),
        external_inputs=("features.perp.v2",),
    ),
    StageArtifactContract(
        stage_patterns=("validate_feature_integrity*",), inputs=("features.perp.v2",)
    ),
    StageArtifactContract(stage_patterns=("validate_data_coverage*",), inputs=("clean.perp.*",)),
    StageArtifactContract(
        stage_patterns=("validate_context_entropy",), inputs=("context.features",)
    ),
    StageArtifactContract(
        stage_patterns=("build_normalized_replay_stream",),
        inputs=("metadata.universe_snapshots",),
        outputs=("runtime.normalized_stream",),
    ),
    StageArtifactContract(
        stage_patterns=("run_causal_lane_ticks",),
        inputs=("runtime.normalized_stream",),
        outputs=("runtime.causal_ticks",),
    ),
    StageArtifactContract(
        stage_patterns=("run_determinism_replay_checks",),
        inputs=("runtime.causal_ticks",),
        outputs=("runtime.determinism_checks",),
    ),
    StageArtifactContract(
        stage_patterns=("run_oms_replay_validation",),
        inputs=("runtime.causal_ticks",),
        outputs=("runtime.oms_replay",),
    ),
    StageArtifactContract(
        stage_patterns=("analyze_*",),
        optional_inputs=("features.perp.v2", "context.market_state", "context.microstructure"),
        outputs=("phase1.events.{event_type}",),
        external_inputs=("features.perp.v2", "context.market_state", "context.microstructure"),
    ),
    StageArtifactContract(
        stage_patterns=("phase1_correlation_clustering",),
        optional_inputs=("phase1.events.*",),
        outputs=("phase1.correlation_clustering",),
    ),
    StageArtifactContract(
        stage_patterns=("build_event_registry*",),
        optional_inputs=("phase1.events.*",),
        outputs=("phase2.event_registry.{event_type}",),
        external_inputs=("phase1.events.*",),
    ),
    StageArtifactContract(
        stage_patterns=("canonicalize_event_episodes*",),
        inputs=("phase2.event_registry.{event_type}",),
        outputs=("phase2.event_episodes.{event_type}",),
        external_inputs=("phase2.event_registry.{event_type}",),
    ),
    StageArtifactContract(
        stage_patterns=("phase2_search_engine",),
        inputs=("features.perp.v2",),
        outputs=("phase2.candidates.search",),
        external_inputs=("features.perp.v2",),
    ),
    StageArtifactContract(
        stage_patterns=("analyze_interaction_lift",),
        inputs=("phase2.candidates.*",),
        outputs=("research.interaction_lift",),
        external_inputs=("phase2.candidates.*",),
    ),
    StageArtifactContract(
        stage_patterns=("finalize_experiment",),
        optional_inputs=("phase2.candidates.*",),
        outputs=("experiment.tested_ledger",),
    ),
    StageArtifactContract(
        stage_patterns=("summarize_discovery_quality",),
        inputs=("phase2.candidates.*",),
        optional_inputs=("phase2.bridge_summary.*",),
        outputs=("phase2.discovery_quality_summary",),
    ),
    StageArtifactContract(
        stage_patterns=("evaluate_naive_entry",),
        inputs=("phase2.candidates.*",),
        outputs=("phase2.naive_entry_eval",),
    ),
    StageArtifactContract(
        stage_patterns=("export_edge_candidates",),
        inputs=("phase2.candidates.*",),
        optional_inputs=("phase2.bridge_metrics.*",),
        outputs=("edge_candidates.normalized",),
        external_inputs=("phase2.candidates.*", "phase2.bridge_metrics.*"),
    ),
    StageArtifactContract(
        stage_patterns=("generate_negative_control_summary",),
        inputs=("edge_candidates.normalized",),
        outputs=("research.negative_control_summary",),
        external_inputs=("edge_candidates.normalized",),
    ),
    StageArtifactContract(
        stage_patterns=("promote_candidates",),
        inputs=("edge_candidates.normalized", "research.negative_control_summary"),
        optional_inputs=("phase2.bridge_metrics.*", "phase2.naive_entry_eval"),
        outputs=("promotion.audit", "promotion.promoted_candidates"),
        external_inputs=(
            "edge_candidates.normalized",
            "research.negative_control_summary",
            "phase2.bridge_metrics.*",
            "phase2.naive_entry_eval",
        ),
    ),
    StageArtifactContract(
        stage_patterns=("update_edge_registry",),
        inputs=("promotion.audit", "promotion.promoted_candidates"),
        outputs=(
            "history.candidate.edge_observations",
            "history.candidate.edge_registry",
            "edge_registry.snapshot",
        ),
        external_inputs=("promotion.audit", "promotion.promoted_candidates"),
    ),
    StageArtifactContract(
        stage_patterns=("update_campaign_memory",),
        optional_inputs=(
            "edge_candidates.normalized",
            "promotion.audit",
            "history.candidate.edge_registry",
            "phase2.discovery_quality_summary",
        ),
        outputs=(
            "experiment.memory.tested_regions",
            "experiment.memory.reflections",
            "experiment.memory.failures",
        ),
        external_inputs=(
            "edge_candidates.normalized",
            "promotion.audit",
            "history.candidate.edge_registry",
            "phase2.discovery_quality_summary",
        ),
    ),
    StageArtifactContract(
        stage_patterns=("analyze_conditional_expectancy",),
        inputs=("history.candidate.edge_registry",),
        outputs=("research.expectancy_analysis",),
        external_inputs=("history.candidate.edge_registry",),
    ),
    StageArtifactContract(
        stage_patterns=("validate_expectancy_traps",),
        inputs=("research.expectancy_analysis",),
        outputs=("research.expectancy_traps",),
    ),
    StageArtifactContract(
        stage_patterns=("generate_recommendations_checklist",),
        inputs=("research.expectancy_traps",),
        outputs=("research.recommendations_checklist",),
    ),
    StageArtifactContract(
        stage_patterns=("compile_strategy_blueprints",),
        inputs=("research.recommendations_checklist",),
        outputs=("strategy.blueprints",),
    ),
    StageArtifactContract(
        stage_patterns=("build_strategy_candidates",),
        optional_inputs=("research.recommendations_checklist", "strategy.blueprints"),
        outputs=("strategy.candidates",),
    ),
    StageArtifactContract(
        stage_patterns=("select_profitable_strategies",),
        optional_inputs=("strategy.candidates",),
        outputs=("strategy.profitable",),
        external_inputs=("strategy.candidates",),
    ),
)


def build_timeframe_artifact_contracts(
    timeframes_str: str = DEFAULT_TIMEFRAME,
) -> tuple[StageArtifactContract, ...]:
    timeframes = parse_timeframes(timeframes_str)
    contracts = [
        StageArtifactContract(
            stage_patterns=("ingest_binance_um_funding",), outputs=("raw.perp.funding_5m",)
        ),
        StageArtifactContract(
            stage_patterns=("ingest_bybit_derivatives_funding",), outputs=("raw.perp.funding_5m",)
        ),
    ]
    contracts.extend(get_timeframe_aware_contracts(timeframes))
    return tuple(contracts)


def match_stage_pattern(stage_name: str, pattern: str) -> bool:
    return fnmatch(stage_name, pattern.replace("{tf}", "*").replace("{event_type}", "*"))


def _flag_value(args: List[str], flag: str) -> str | None:
    try:
        idx = args.index(flag)
    except ValueError:
        return None
    if idx + 1 >= len(args):
        return None
    return str(args[idx + 1]).strip()


def _timeframe_from_stage(stage_name: str, base_args: List[str]) -> str:
    for flag in ("--timeframe", "--universe_timeframe"):
        raw = _flag_value(base_args, flag)
        if raw:
            try:
                return normalize_timeframe(raw)
            except ContractViolationError as exc:
                raise ContractViolationError(
                    f"Stage '{stage_name}' received invalid explicit {flag} value {raw!r}"
                ) from exc
    raw_multi = _flag_value(base_args, "--timeframes")
    if raw_multi:
        token = str(raw_multi).split(",")[0].strip()
        if token:
            return normalize_timeframe(token)
    for tf in SUPPORTED_TIMEFRAMES:
        if stage_name.endswith(f"_{tf}") or f"_{tf}_" in stage_name:
            return tf
    return DEFAULT_TIMEFRAME


def _event_type_from_stage(stage_name: str, base_args: List[str]) -> str:
    raw = _flag_value(base_args, "--event_type")
    if raw:
        return raw.strip().upper()
    if stage_name.startswith("analyze_") and "__" in stage_name:
        return stage_name.rsplit("__", 1)[1].rsplit("_", 1)[0].strip().upper()
    prefixes = (
        "build_event_registry_",
        "canonicalize_event_episodes_",
    )
    for prefix in prefixes:
        if stage_name.startswith(prefix):
            suffix = stage_name[len(prefix) :].rsplit("_", 1)[0]
            if suffix:
                return suffix.strip().upper()
    return "ALL"


def _materialize_artifact_keys(
    templates: tuple[str, ...],
    *,
    stage_name: str,
    base_args: List[str],
) -> tuple[str, ...]:
    timeframe = _timeframe_from_stage(stage_name, base_args)
    event_type = _event_type_from_stage(stage_name, base_args)
    out: list[str] = []
    for template in templates:
        key = str(template).replace("{tf}", timeframe).replace("{event_type}", event_type)
        if key and key not in out:
            out.append(key)
    return tuple(out)


def _matching_artifact_contracts(stage_name: str) -> List[Any]:
    matched: list[Any] = []
    for contract in STAGE_ARTIFACT_REGISTRY:
        for pattern in contract.stage_patterns:
            expanded_patterns = [pattern]
            if "{tf}" in pattern:
                expanded_patterns = [pattern.replace("{tf}", tf) for tf in SUPPORTED_TIMEFRAMES]
            for actual_pattern in expanded_patterns:
                if fnmatch(stage_name, actual_pattern):
                    matched.append(contract)
                    break
            else:
                continue
            break
    return matched


def resolve_stage_artifact_contract(
    stage_name: str,
    base_args: List[str],
) -> tuple[ResolvedStageArtifactContract | None, List[str]]:
    issues: List[str] = []
    matches = _matching_artifact_contracts(stage_name)
    if not matches:
        issues.append(f"missing artifact contract for stage '{stage_name}'")
        return None, issues

    def _score(contract: Any) -> tuple[int, int]:
        best: tuple[int, int] | None = None
        for pattern in contract.stage_patterns:
            actual_patterns = [pattern]
            if "{tf}" in pattern:
                actual_patterns = [pattern.replace("{tf}", tf) for tf in SUPPORTED_TIMEFRAMES]
            for actual_pattern in actual_patterns:
                if not fnmatch(stage_name, actual_pattern):
                    continue
                score = (1 if _is_glob_pattern(actual_pattern) else 0, -len(actual_pattern))
                if best is None or score < best:
                    best = score
        return best or (1, 0)

    contract = sorted(matches, key=_score)[0]
    return (
        ResolvedStageArtifactContract(
            inputs=_materialize_artifact_keys(
                contract.inputs, stage_name=stage_name, base_args=base_args
            ),
            optional_inputs=_materialize_artifact_keys(
                contract.optional_inputs, stage_name=stage_name, base_args=base_args
            ),
            outputs=_materialize_artifact_keys(
                contract.outputs, stage_name=stage_name, base_args=base_args
            ),
            external_inputs=_materialize_artifact_keys(
                contract.external_inputs, stage_name=stage_name, base_args=base_args
            ),
        ),
        issues,
    )


def validate_stage_dataflow_dag(stages: Sequence[StageSpec]) -> List[str]:
    issues: List[str] = []
    resolved: dict[int, ResolvedStageArtifactContract] = {}
    for idx, (stage_name, _script_path, base_args) in enumerate(stages):
        contract, contract_issues = resolve_stage_artifact_contract(stage_name, list(base_args))
        if contract_issues:
            issues.extend(contract_issues)
            continue
        if contract is not None:
            resolved[idx] = contract

    produced_by: Dict[str, List[str]] = {}
    for idx, (stage_name, _script_path, base_args) in enumerate(stages):
        contract = resolved.get(idx)
        if contract is None:
            continue
        timeframe = _timeframe_from_stage(stage_name, list(base_args))
        for artifact in contract.outputs:
            key = f"{artifact}@@{timeframe}"
            produced_by.setdefault(key, []).append(stage_name)

    for artifact, producers in produced_by.items():
        if len(producers) > 1:
            issues.append(f"duplicate artifact producer for '{artifact}': {producers}")

    available: Set[str] = set()
    for idx, (stage_name, _script_path, _base_args) in enumerate(stages):
        contract = resolved.get(idx)
        if contract is None:
            continue
        external = set(contract.external_inputs)
        for artifact in contract.inputs:
            if artifact in external:
                continue
            if not any(fnmatch(avail, artifact) or fnmatch(artifact, avail) for avail in available):
                issues.append(
                    f"stage '{stage_name}' requires input artifact '{artifact}' "
                    "which is not produced upstream"
                )
        for artifact in contract.outputs:
            available.add(artifact)

    return issues


def _is_glob_pattern(value: str) -> bool:
    return "*" in value or "?" in value or "[" in value


def _to_rel_posix(path: Path, project_root: Path) -> str:
    try:
        rel = Path(path).resolve().relative_to(Path(project_root).resolve())
    except ValueError:
        rel = Path(path)
    return str(rel).replace("\\", "/")


def _resolve_existing_script_path(script_path: Path, project_root: Path) -> Path | None:
    path = Path(script_path)
    candidates = [path]
    if not path.is_absolute():
        candidates.extend(
            [
                project_root / path,
                project_root.parent / path,
            ]
        )
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve() if candidate.is_absolute() else candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.exists():
            return resolved
    return None


def _matching_family_contracts(stage_name: str) -> List[StageFamilyContract]:
    matched = []
    for contract in STAGE_FAMILY_REGISTRY:
        if any(fnmatch(stage_name, pattern) for pattern in contract.stage_patterns):
            matched.append(contract)
    return matched


def validate_stage_plan_contract(stages: Sequence[StageSpec], project_root: Path) -> List[str]:
    issues: List[str] = []
    for stage_name, script_path, _ in stages:
        contracts = _matching_family_contracts(stage_name)
        if not contracts:
            issues.append(f"unknown stage family for stage '{stage_name}'")
            continue
        contract = contracts[0]
        resolved_script = _resolve_existing_script_path(script_path, project_root)
        if resolved_script is None:
            issues.append(f"stage '{stage_name}' script path '{script_path}' does not exist")
            continue
        rel_script = _to_rel_posix(resolved_script, project_root)
        if not any(fnmatch(rel_script, pat) for pat in contract.script_patterns):
            issues.append(f"stage '{stage_name}' script '{rel_script}' violated allowed patterns")
    return issues


def assert_stage_registry_contract(stages: Sequence[StageSpec], project_root: Path) -> None:
    issues = validate_stage_plan_contract(stages, project_root)
    issues.extend(validate_stage_dataflow_dag(stages))
    if issues:
        raise ValueError(f"Stage registry violations: {issues}")


def resolve_stage_artifacts(
    stage_name: str,
    timeframes_str: str = DEFAULT_TIMEFRAME,
) -> ResolvedStageArtifactContract:
    contract, _ = resolve_stage_artifact_contract(stage_name, ["--timeframes", timeframes_str])
    if contract is None:
        return ResolvedStageArtifactContract(
            inputs=(),
            optional_inputs=(),
            outputs=(),
            external_inputs=(),
        )
    return contract


def build_stage_timeframe_artifact_mappings() -> list[StageTimeframeArtifactMapping]:
    mappings: list[StageTimeframeArtifactMapping] = []
    for tf in parse_timeframes("1m,5m,15m"):
        mappings.append(
            StageTimeframeArtifactMapping(
                stage_name=f"ingest_binance_um_ohlcv_{tf}",
                script_path="pipelines/ingest/ingest_binance_um_ohlcv.py",
                timeframe=tf,
                outputs=(make_ohlcv_artifact_token(tf),),
            )
        )
    return mappings
