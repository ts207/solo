# _COMPAT_ADAPTER: Legacy discovery service layer superseded by project.research.phase2_search_engine.
# Active pipeline stages (run_all.py) do NOT call this service — they invoke phase2_search_engine.py
# directly as a subprocess stage. This service is retained for:
#   - reliability/smoke_data.py (smoke test fixtures)
#   - research/cli/candidate_discovery_cli.py (standalone research tool)
# Do not add new pipeline callers here.
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC
from pathlib import Path
from typing import Any

import pandas as pd

from project import PROJECT_ROOT
from project.core.config import (
    get_data_root,  # noqa: F401 - retained for test/plugin monkeypatch compatibility
)
from project.core.execution_costs import resolve_execution_costs
from project.domain.compiled_registry import get_domain_registry
from project.events.event_specs import EVENT_REGISTRY_SPECS
from project.io.utils import ensure_dir
from project.research import discovery
from project.research.CANONICAL_PIPELINE import persist_canonical_pipeline_artifact
from project.research.cost_calibration import CandidateCostEstimate, ToBRegimeCostCalibrator
from project.research.decision_trace_artifacts import (
    write_discovery_trace,
    write_merged_research_trace,
)
from project.research.gating import build_event_return_frame
from project.research.hypothesis_registry import Hypothesis, HypothesisRegistry
from project.research.phase2 import load_features, prepare_events_dataframe
from project.research.regime_routing import annotate_regime_metadata
from project.research.services import candidate_discovery_scoring as candidate_scoring
from project.research.services.candidate_discovery_diagnostics import (
    apply_sample_quality_gates,
    build_false_discovery_diagnostics,
)
from project.research.services.candidate_discovery_scoring import (
    apply_validation_multiple_testing,
)
from project.research.services.pathing import phase2_event_out_dir
from project.research.services.phase2_diagnostics import (
    build_prepare_events_diagnostics,
    get_prepare_events_diagnostics,
)
from project.research.services.phase2_diagnostics import (
    split_counts as phase2_split_counts,
)
from project.research.services.phase2_support import bar_duration_minutes_from_timeframe
from project.research.services.regime_effectiveness_service import (
    write_regime_effectiveness_reports,
)
from project.research.services.reporting_service import write_candidate_reports
from project.research.validation import estimate_effect_from_frame
from project.specs.manifest import finalize_manifest, start_manifest

ResolvedCandidateCostEstimate = CandidateCostEstimate
DEFAULT_SAMPLE_QUALITY_POLICY: dict[str, dict[str, int]] = {
    "standard": {
        # Raised from 2 to 10 (TICKET-005): two events provide near-zero statistical power.
        "min_validation_n_obs": 10,
        "min_test_n_obs": 10,
        "min_total_n_obs": 30,
    },
    "synthetic": {
        "min_validation_n_obs": 1,
        "min_test_n_obs": 1,
        "min_total_n_obs": 4,
    },
}


@dataclass(frozen=True)
class CandidateDiscoveryConfig:
    run_id: str
    symbols: tuple[str, ...]
    config_paths: tuple[str, ...]
    data_root: Path
    event_type: str
    timeframe: str
    horizon_bars: int
    out_dir: Path | None
    run_mode: str
    split_scheme_id: str
    embargo_bars: int
    purge_bars: int
    train_only_lambda_used: float
    discovery_profile: str
    candidate_generation_method: str
    concept_file: str | None
    entry_lag_bars: int
    shift_labels_k: int
    fees_bps: float | None
    slippage_bps: float | None
    cost_bps: float | None
    cost_calibration_mode: str
    cost_min_tob_coverage: float
    cost_tob_tolerance_minutes: int
    candidate_origin_run_id: str | None
    frozen_spec_hash: str | None
    templates: tuple[str, ...] | None = None
    horizons: tuple[str, ...] | None = None
    directions: tuple[str, ...] | None = None
    entry_lags: tuple[int, ...] | None = None
    program_id: str | None = None
    search_budget: int | None = None
    experiment_config: str | None = None
    registry_root: Path | None = None
    min_validation_n_obs: int | None = None
    min_test_n_obs: int | None = None
    min_total_n_obs: int | None = None
    gate_profile: str | None = None

    def resolved_out_dir(self) -> Path:
        if self.out_dir is not None:
            return self.out_dir
        return phase2_event_out_dir(
            data_root=self.data_root,
            run_id=self.run_id,
            event_type=self.event_type,
            timeframe=self.timeframe,
        )

    def manifest_params(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "symbols": ",".join(self.symbols),
            "config": list(self.config_paths),
            "data_root": str(self.data_root),
            "event_type": self.event_type,
            "templates": list(self.templates) if self.templates else None,
            "horizons": list(self.horizons) if self.horizons else None,
            "directions": list(self.directions) if self.directions else None,
            "entry_lags": list(self.entry_lags) if self.entry_lags else None,
            "program_id": self.program_id,
            "search_budget": self.search_budget,
            "timeframe": self.timeframe,
            "horizon_bars": self.horizon_bars,
            "out_dir": str(self.out_dir) if self.out_dir is not None else None,
            "run_mode": self.run_mode,
            "split_scheme_id": self.split_scheme_id,
            "embargo_bars": self.embargo_bars,
            "purge_bars": self.purge_bars,
            "train_only_lambda_used": self.train_only_lambda_used,
            "discovery_profile": self.discovery_profile,
            "candidate_generation_method": self.candidate_generation_method,
            "concept_file": self.concept_file,
            "entry_lag_bars": self.entry_lag_bars,
            "shift_labels_k": self.shift_labels_k,
            "fees_bps": self.fees_bps,
            "slippage_bps": self.slippage_bps,
            "cost_bps": self.cost_bps,
            "cost_calibration_mode": self.cost_calibration_mode,
            "cost_min_tob_coverage": self.cost_min_tob_coverage,
            "cost_tob_tolerance_minutes": self.cost_tob_tolerance_minutes,
            "candidate_origin_run_id": self.candidate_origin_run_id,
            "frozen_spec_hash": self.frozen_spec_hash,
            "experiment_config": self.experiment_config,
            "registry_root": str(self.registry_root) if self.registry_root is not None else None,
            "min_validation_n_obs": None
            if self.min_validation_n_obs is None
            else int(self.min_validation_n_obs),
            "min_test_n_obs": None if self.min_test_n_obs is None else int(self.min_test_n_obs),
            "min_total_n_obs": None if self.min_total_n_obs is None else int(self.min_total_n_obs),
        }


@dataclass
class CandidateDiscoveryResult:
    exit_code: int
    output_dir: Path
    combined_candidates: pd.DataFrame = field(default_factory=pd.DataFrame)
    symbol_candidates: dict[str, pd.DataFrame] = field(default_factory=dict)
    manifest: dict[str, Any] = field(default_factory=dict)


def _resolve_sample_quality_policy(config: CandidateDiscoveryConfig) -> dict[str, Any]:
    profile = str(config.discovery_profile or "standard").strip().lower()
    defaults = DEFAULT_SAMPLE_QUALITY_POLICY.get(profile, DEFAULT_SAMPLE_QUALITY_POLICY["standard"])
    resolved = {
        "profile": profile,
        "min_validation_n_obs": int(config.min_validation_n_obs)
        if config.min_validation_n_obs is not None
        else int(defaults["min_validation_n_obs"]),
        "min_test_n_obs": int(config.min_test_n_obs)
        if config.min_test_n_obs is not None
        else int(defaults["min_test_n_obs"]),
        "min_total_n_obs": int(config.min_total_n_obs)
        if config.min_total_n_obs is not None
        else int(defaults["min_total_n_obs"]),
        "explicit_overrides": {
            "min_validation_n_obs": config.min_validation_n_obs is not None,
            "min_test_n_obs": config.min_test_n_obs is not None,
            "min_total_n_obs": config.min_total_n_obs is not None,
        },
    }
    return resolved


_build_false_discovery_diagnostics = build_false_discovery_diagnostics
_apply_sample_quality_gates = apply_sample_quality_gates


def _split_and_score_candidates(*args: Any, **kwargs: Any) -> pd.DataFrame:
    return candidate_scoring.split_and_score_candidates(
        *args,
        **kwargs,
        build_event_return_frame_fn=build_event_return_frame,
        estimate_effect_from_frame_fn=estimate_effect_from_frame,
    )


_apply_validation_multiple_testing = apply_validation_multiple_testing
_apply_historical_frontier_multiple_testing = candidate_scoring.apply_historical_frontier_multiple_testing
_apply_ledger_multiplicity_correction = candidate_scoring.apply_ledger_multiplicity_correction


def _write_concept_ledger_records(
    candidates: pd.DataFrame,
    *,
    data_root: Path,
    run_id: str,
    program_id: str = "",
) -> None:
    """Append tested concept records to the global concept ledger.

    Called unconditionally after discovery scoring — history accumulates
    regardless of whether ledger-adjusted scoring is enabled.  Ledger write
    failure aborts discovery because missing records corrupt future
    multiplicity accounting.
    """
    try:
        from project.research.knowledge.concept_ledger import (
            append_concept_ledger,
            build_ledger_records,
            default_ledger_path,
        )

        records = build_ledger_records(
            candidates,
            run_id=run_id,
            program_id=program_id,
        )
        if not records.empty:
            ledger_path = default_ledger_path(data_root)
            append_concept_ledger(records, ledger_path, raise_on_error=True)
    except Exception as exc:
        logging.getLogger(__name__).critical(
            "Concept ledger write failed for run %s: %s", run_id, exc
        )
        raise RuntimeError("Concept ledger write failed") from exc


def execute_candidate_discovery(config: CandidateDiscoveryConfig) -> CandidateDiscoveryResult:
    if config.entry_lag_bars < 1:
        return CandidateDiscoveryResult(exit_code=1, output_dir=config.resolved_out_dir())

    out_dir = config.resolved_out_dir()
    ensure_dir(out_dir)
    manifest = start_manifest(
        "phase2_search_engine", config.run_id, config.manifest_params(), [], []
    )
    hyp_registry = HypothesisRegistry()
    symbol_candidates: dict[str, pd.DataFrame] = {}
    combined = pd.DataFrame()
    symbol_diagnostics: dict[str, dict[str, Any]] = {}
    sample_quality_policy = _resolve_sample_quality_policy(config)

    try:
        try:
            resolved_costs = resolve_execution_costs(
                project_root=PROJECT_ROOT.parent,
                config_paths=config.config_paths,
                fees_bps=config.fees_bps,
                slippage_bps=config.slippage_bps,
                cost_bps=config.cost_bps,
            )
        except FileNotFoundError:
            fee_bps = float(config.fees_bps) if config.fees_bps is not None else 4.0
            slippage_bps = float(config.slippage_bps) if config.slippage_bps is not None else 2.0
            total_cost_bps = (
                float(config.cost_bps) if config.cost_bps is not None else fee_bps + slippage_bps
            )
            resolved_costs = type(
                "ResolvedCostsFallback",
                (),
                {
                    "config_digest": "fallback:no_config",
                    "cost_bps": float(total_cost_bps),
                    "fee_bps_per_side": float(fee_bps),
                    "slippage_bps_per_fill": float(slippage_bps),
                    "round_trip_cost_bps": float(2.0 * total_cost_bps),
                },
            )()
        cost_calibrator = ToBRegimeCostCalibrator(
            run_id=config.run_id,
            data_root=config.data_root,
            base_fee_bps=resolved_costs.fee_bps_per_side,
            base_slippage_bps=resolved_costs.slippage_bps_per_fill,
            static_cost_bps=resolved_costs.cost_bps,
            mode=str(config.cost_calibration_mode or "auto"),
            min_tob_coverage=float(config.cost_min_tob_coverage),
            tob_tolerance_minutes=int(config.cost_tob_tolerance_minutes),
        )
        event_frames: list[pd.DataFrame] = []
        experiment_plan = None
        required_experiment_events: set[str] = set()
        if config.experiment_config:
            import importlib

            experiment_engine = importlib.import_module("project.research.experiment_engine")
            experiment_plan = experiment_engine.build_experiment_plan(
                Path(config.experiment_config),
                config.registry_root or Path("project/configs/registries"),
            )
            for hypothesis in experiment_plan.hypotheses:
                trigger = hypothesis.trigger
                if trigger.trigger_type == "event" and trigger.event_id:
                    required_experiment_events.add(trigger.event_id)
                elif trigger.trigger_type == "sequence" and trigger.events:
                    required_experiment_events.update(trigger.events)
                elif trigger.trigger_type == "interaction":
                    if trigger.left:
                        required_experiment_events.add(trigger.left)
                    if trigger.right:
                        required_experiment_events.add(trigger.right)
            if required_experiment_events:
                required_experiment_events.add(config.event_type)

        for symbol in config.symbols:
            # If experiment is active, we might need multiple event types for sequences/interactions
            load_event_type: str | list[str] = config.event_type
            if required_experiment_events:
                load_event_type = sorted(required_experiment_events)

            events_df = prepare_events_dataframe(
                data_root=config.data_root,
                run_id=config.run_id,
                event_type=load_event_type,
                symbols=[symbol],
                event_registry_specs=EVENT_REGISTRY_SPECS,
                horizons=[discovery.bars_to_timeframe(config.horizon_bars)],
                entry_lag_bars=config.entry_lag_bars,
                fam_config={},
                logger=logging.getLogger(__name__),
                run_mode=config.run_mode,
                timeframe=config.timeframe,
            )
            prepare_diag = get_prepare_events_diagnostics(events_df)
            if not prepare_diag:
                prepare_diag = build_prepare_events_diagnostics(
                    run_id=config.run_id,
                    event_type=config.event_type,
                    symbols_requested=[symbol],
                    raw_event_count=len(events_df),
                    canonical_episode_count=len(events_df),
                    split_counts_payload=phase2_split_counts(events_df),
                    loaded_from_fallback_file=False,
                    holdout_integrity_failed=False,
                    resplit_attempted=False,
                    returned_empty_due_to_holdout=False,
                    min_validation_events=1,
                    min_test_events=1,
                    returned_rows=len(events_df),
                )
            symbol_diag: dict[str, Any] = {
                "symbol": symbol,
                "event_type": config.event_type,
                "generated_candidate_rows": 0,
                "post_split_candidate_rows": 0,
                "multiplicity_survivors": 0,
                "rejected_by_min_sample": 0,
                "prepare_events": prepare_diag,
                "loss_attribution": {
                    "raw_anchored_episodes": int(prepare_diag.get("raw_event_count", 0)),
                    "post_filter_episodes": int(prepare_diag.get("returned_rows", 0)),
                    "candidate_hypotheses_synthesized": 0,
                    "surviving_phase_gates": 0,
                    "candidates_entering_validation": 0,
                    "rejected_by_scoring": 0,
                    "rejected_by_multiplicity": 0,
                    "rejected_by_sample_quality": 0,
                }
            }
            if events_df.empty:
                symbol_diagnostics[symbol] = symbol_diag
                continue

            features_df = load_features(
                data_root=config.data_root,
                run_id=config.run_id,
                symbol=symbol,
                timeframe=config.timeframe,
            )
            cost_estimate = cost_calibrator.estimate(symbol=symbol, events_df=events_df)
            symbol_diag["cost_estimate"] = {
                "cost_bps": float(cost_estimate.cost_bps),
                "fee_bps_per_side": float(cost_estimate.fee_bps_per_side),
                "slippage_bps_per_fill": float(cost_estimate.slippage_bps_per_fill),
                "round_trip_cost_bps": float(
                    getattr(cost_estimate, "round_trip_cost_bps", 2.0 * float(cost_estimate.cost_bps))
                ),
                "avg_dynamic_cost_bps": float(cost_estimate.avg_dynamic_cost_bps),
                "cost_input_coverage": float(cost_estimate.cost_input_coverage),
                "cost_model_valid": bool(cost_estimate.cost_model_valid),
                "cost_model_source": str(cost_estimate.cost_model_source),
                "regime_multiplier": float(cost_estimate.regime_multiplier),
            }

            if config.experiment_config:
                candidates = discovery._synthesize_experiment_hypotheses(
                    run_id=config.run_id,
                    symbol=symbol,
                    events_df=events_df,
                    features_df=features_df,
                    experiment_config=config.experiment_config,
                    event_type=config.event_type,
                    registry_root=config.registry_root or Path("project/configs/registries"),
                    experiment_plan=experiment_plan,
                )
            elif config.concept_file:
                candidates = discovery._synthesize_concept_candidates(
                    run_id=config.run_id,
                    symbol=symbol,
                    events_df=events_df,
                    features_df=features_df,
                    entry_lag_bars=config.entry_lag_bars,
                    concept_file=config.concept_file,
                )
            else:
                direction_policy = discovery.resolve_registry_direction_policy(
                    events_df,
                    event_type=config.event_type,
                    default=0.0,
                )
                symbol_diag["direction_policy"] = {
                    "policy": str(direction_policy["policy"]),
                    "source": str(direction_policy["source"]),
                    "resolved": bool(direction_policy["resolved"]),
                    "direction_sign": float(direction_policy["direction_sign"]),
                }
                candidates = discovery._synthesize_registry_candidates(
                    run_id=config.run_id,
                    symbol=symbol,
                    event_type=config.event_type,
                    events_df=events_df,
                    horizon_bars=config.horizon_bars,
                    entry_lag_bars=config.entry_lag_bars,
                    templates=config.templates,
                    horizons=config.horizons,
                    directions=config.directions,
                    entry_lags=config.entry_lags,
                    search_budget=config.search_budget,
                )
                if candidates.empty and not bool(direction_policy["resolved"]):
                    symbol_diag["direction_policy"][
                        "skipped_non_directional_registry_generation"
                    ] = True
            symbol_diag["generated_candidate_rows"] = len(candidates)
            symbol_diag["loss_attribution"]["candidate_hypotheses_synthesized"] = len(candidates)
            if candidates.empty:
                symbol_diagnostics[symbol] = symbol_diag
                continue

            candidates["run_id"] = config.run_id
            candidates["run_mode"] = config.run_mode
            candidates["discovery_batch"] = config.run_id
            candidates["candidate_generation_method"] = config.candidate_generation_method
            candidates["split_scheme_id"] = config.split_scheme_id
            bar_duration_minutes = bar_duration_minutes_from_timeframe(config.timeframe)
            candidates = _split_and_score_candidates(
                candidates,
                events_df,
                horizon_bars=config.horizon_bars,
                split_scheme_id=config.split_scheme_id,
                purge_bars=config.purge_bars,
                embargo_bars=config.embargo_bars,
                bar_duration_minutes=bar_duration_minutes,
                features_df=features_df,
                entry_lag_bars=config.entry_lag_bars,
                shift_labels_k=config.shift_labels_k,
                cost_estimate=cost_estimate,
                cost_coordinate={
                    "config_digest": str(getattr(resolved_costs, "config_digest", "") or ""),
                    "execution_model": dict(getattr(resolved_costs, "execution_model", {}) or {}),
                    "after_cost_includes_funding_carry": False,
                    "fee_bps_per_side": float(getattr(resolved_costs, "fee_bps_per_side", 0.0) or 0.0),
                    "slippage_bps_per_fill": float(getattr(resolved_costs, "slippage_bps_per_fill", 0.0) or 0.0),
                    "cost_bps": float(getattr(resolved_costs, "cost_bps", 0.0) or 0.0),
                    "round_trip_cost_bps": float(
                        getattr(
                            resolved_costs,
                            "round_trip_cost_bps",
                            2.0 * float(getattr(resolved_costs, "cost_bps", 0.0) or 0.0),
                        )
                        or 0.0
                    ),
                },
            )
            symbol_diag["post_split_candidate_rows"] = len(candidates)
            if "validation_n_obs" in candidates.columns or "test_n_obs" in candidates.columns:
                symbol_diag["rejected_by_min_sample"] = int(
                    (
                        pd.to_numeric(
                            candidates.get("validation_n_obs", 0), errors="coerce"
                        ).fillna(0)
                        <= 0
                    ).sum()
                    + (
                        pd.to_numeric(candidates.get("test_n_obs", 0), errors="coerce").fillna(0)
                        <= 0
                    ).sum()
                )
            for idx, row in candidates.iterrows():
                event_type = str(row.get("canonical_event_type", row.get("event_type", ""))).strip()
                event_spec = get_domain_registry().get_event(event_type)
                hyp = Hypothesis(
                    event_family=(
                        event_spec.canonical_regime
                        if event_spec is not None and event_spec.canonical_regime
                        else event_type
                    ),
                    event_type=str(row.get("event_type", "")),
                    symbol_scope=symbol,
                    side=discovery.action_name_from_direction(row.get("direction", 0.0)),
                    horizon=str(config.horizon_bars),
                    condition_template="standard_v1",
                    state_filter="none",
                    parameterization_id="v1",
                    family_id=str(row.get("family_id", "default")),
                    cluster_id=f"{symbol}_cluster_day",
                )
                candidates.at[idx, "hypothesis_id"] = hyp_registry.register(hyp)
            symbol_candidates[symbol] = candidates
            symbol_diagnostics[symbol] = symbol_diag
            event_frames.append(candidates)

        if event_frames:
            combined = pd.concat(event_frames, ignore_index=True)
            combined = apply_validation_multiple_testing(combined)
            combined = _apply_historical_frontier_multiple_testing(
                combined,
                data_root=config.data_root,
                current_run_id=config.run_id,
            )
            combined = apply_sample_quality_gates(
                combined,
                min_validation_n_obs=int(sample_quality_policy["min_validation_n_obs"]),
                min_test_n_obs=int(sample_quality_policy["min_test_n_obs"]),
                min_total_n_obs=int(sample_quality_policy["min_total_n_obs"]),
            )
            combined = annotate_regime_metadata(combined)
            # Phase 3: ledger-adjusted multiplicity correction (additive; flag-gated)
            combined = _apply_ledger_multiplicity_correction(
                combined,
                data_root=config.data_root,
                current_run_id=config.run_id,
            )
            # Phase 3: write concept ledger records (unconditional — always accumulates)
            _write_concept_ledger_records(
                combined,
                data_root=config.data_root,
                run_id=config.run_id,
                program_id=str(config.program_id or ""),
            )
            symbol_candidates = {
                str(symbol): sym_df.copy() for symbol, sym_df in combined.groupby("symbol")
            }
            for symbol, sym_df in symbol_candidates.items():
                symbol_diagnostics.setdefault(symbol, {"symbol": symbol})
                pre_gate_survivors = int(
                    pd.to_numeric(
                        sym_df.get("is_discovery_pre_sample_quality", False), errors="coerce"
                    )
                    .fillna(0)
                    .astype(bool)
                    .sum()
                )
                entering_val = int(
                    pd.to_numeric(sym_df.get("is_discovery", False), errors="coerce")
                    .fillna(0)
                    .astype(bool)
                    .sum()
                )
                symbol_diagnostics[symbol]["multiplicity_survivors"] = entering_val
                symbol_diagnostics[symbol]["rejected_by_sample_quality_gate"] = int(
                    pd.to_numeric(sym_df.get("rejected_by_sample_quality", False), errors="coerce")
                    .fillna(0)
                    .astype(bool)
                    .sum()
                )
                symbol_diagnostics[symbol]["survivors_before_sample_quality_gate"] = (
                    pre_gate_survivors
                )
                if "loss_attribution" in symbol_diagnostics[symbol]:
                    loss = symbol_diagnostics[symbol]["loss_attribution"]
                    loss["surviving_phase_gates"] = pre_gate_survivors
                    loss["candidates_entering_validation"] = entering_val
                    loss["rejected_by_sample_quality"] = symbol_diagnostics[symbol]["rejected_by_sample_quality_gate"]
                    syn = loss["candidate_hypotheses_synthesized"]
                    loss["rejected_by_scoring"] = max(0, syn - pre_gate_survivors)
                    loss["rejected_by_multiplicity"] = max(0, pre_gate_survivors - entering_val - loss["rejected_by_sample_quality"])


        write_candidate_reports(
            out_dir=out_dir,
            combined_candidates=combined,
            symbol_candidates=symbol_candidates,
            diagnostics={
                "run_id": config.run_id,
                "event_type": config.event_type,
                "timeframe": config.timeframe,
                "cost_coordinate": {
                    "config_digest": resolved_costs.config_digest,
                    "cost_bps": float(resolved_costs.cost_bps),
                    "fee_bps_per_side": float(resolved_costs.fee_bps_per_side),
                    "slippage_bps_per_fill": float(resolved_costs.slippage_bps_per_fill),
                    "round_trip_cost_bps": float(
                        getattr(
                            resolved_costs,
                            "round_trip_cost_bps",
                            2.0 * float(resolved_costs.cost_bps),
                        )
                    ),
                    "execution_model": dict(getattr(resolved_costs, "execution_model", {}) or {}),
                    "after_cost_includes_funding_carry": False,
                },
                "symbols_requested": list(config.symbols),
                "symbols_with_candidates": sorted(symbol_candidates),
                "combined_candidate_rows": len(combined),
                "sample_quality_gate_thresholds": {
                    "min_validation_n_obs": int(sample_quality_policy["min_validation_n_obs"]),
                    "min_test_n_obs": int(sample_quality_policy["min_test_n_obs"]),
                    "min_total_n_obs": int(sample_quality_policy["min_total_n_obs"]),
                },
                "sample_quality_gate_policy": sample_quality_policy,
                "false_discovery_diagnostics": build_false_discovery_diagnostics(combined),
                "symbol_diagnostics": [symbol_diagnostics[s] for s in sorted(symbol_diagnostics)],
            },
        )
        regime_artifacts = write_regime_effectiveness_reports(
            run_id=config.run_id,
            data_root=config.data_root,
            episodes=combined,
        )
        manifest["regime_effectiveness_output_dir"] = str(regime_artifacts.output_dir)
        manifest["regime_effectiveness_summary"] = dict(regime_artifacts.summary)

        reg_hash = hyp_registry.write_artifacts(out_dir)
        manifest["hypothesis_registry_hash"] = reg_hash
        canonical_path_path = persist_canonical_pipeline_artifact(
            out_dir,
            run_id=config.run_id,
            stage="discover",
            used_module="project.research.services.candidate_discovery_service",
            extra={
                "discovery_stage": "phase2_search_engine",
                "experiment_config": str(config.experiment_config or ""),
                "discovery_profile": str(config.discovery_profile or ""),
                "gate_profile": str(config.gate_profile or ""),
            },
        )
        trace_artifact = write_discovery_trace(combined, out_dir=out_dir, run_id=config.run_id)
        merged_trace_path = write_merged_research_trace(
            out_dir=out_dir,
            data_root=config.data_root,
            run_id=config.run_id,
            discovery_trace=trace_artifact["frame"],
        )
        manifest["canonical_research_path_path"] = str(canonical_path_path)
        manifest["discovery_decision_trace_path"] = str(trace_artifact["path"])
        if merged_trace_path is not None:
            manifest["research_decision_trace_path"] = str(merged_trace_path)

        # Sprint 7: Artifact manifest
        try:
            from datetime import datetime

            from project.research.validation.manifest import RunArtifactManifest

            artifact_manifest = RunArtifactManifest(
                run_id=config.run_id,
                stage="discover",
                created_at=datetime.now(UTC).isoformat(),
                upstream_run_ids=[],
                artifacts={
                    "phase2_candidates": "phase2_candidates.parquet",
                    "phase2_diagnostics": "phase2_diagnostics.json",
                    "canonical_research_path": "canonical_research_path.json",
                    "discovery_decision_trace": "discovery_decision_trace.parquet",
                    "research_decision_trace": "research_decision_trace.parquet",
                }
            )
            artifact_manifest.persist(out_dir)
        except Exception as exc:
            logging.warning("Failed to persist artifact manifest: %s", exc)

        finalize_manifest(manifest, "success")
        return CandidateDiscoveryResult(0, out_dir, combined, symbol_candidates, manifest)
    except Exception as exc:
        logging.exception("Discovery failed")
        finalize_manifest(manifest, "failed", error=str(exc))
        return CandidateDiscoveryResult(1, out_dir, combined, symbol_candidates, manifest)
