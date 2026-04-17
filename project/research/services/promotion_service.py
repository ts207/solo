from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import pandas as pd

from project import PROJECT_ROOT
from project.contracts.schemas import normalize_dataframe_for_schema
from project.core.config import get_data_root
from project.core.exceptions import (
    ArtifactPersistenceError,
    CompatibilityRequiredError,
    DataIntegrityError,
    IncompleteLineageError,
    MissingArtifactError,
    SchemaMismatchError,
)
from project.io.utils import (
    atomic_write_json,
    ensure_dir,
)
from project.research.audit_historical_artifacts import build_run_historical_trust_summary
from project.research.promotion import (
    build_promotion_statistical_audit,
    promote_candidates,
    stabilize_promoted_output_schema,
)
from project.research.services.reporting_service import write_promotion_reports
from project.research.regime_routing import annotate_regime_metadata
from project.research.validation.evidence_bundle import (
    bundle_to_flat_record,
    serialize_evidence_bundles,
    validate_evidence_bundle,
)
from project.research.services import promotion_artifacts as _promotion_artifacts
from project.research.services import promotion_diagnostics as _promotion_diagnostics
from project.research.services import promotion_inputs as _promotion_inputs
from project.research.services import promotion_policy as _promotion_policy
from project.specs.gates import load_gates_spec as _load_gates_spec
from project.specs.manifest import finalize_manifest, load_run_manifest, start_manifest
from project.specs.objective import resolve_objective_profile_contract
from project.specs.ontology import ontology_spec_hash


@dataclass(frozen=True)
class PromotionConfig:
    run_id: str
    symbols: str
    out_dir: Optional[Path]
    max_q_value: float
    min_events: int
    min_stability_score: float
    min_sign_consistency: float
    min_cost_survival_ratio: float
    max_negative_control_pass_rate: float
    min_tob_coverage: float
    require_hypothesis_audit: bool
    allow_missing_negative_controls: bool
    require_multiplicity_diagnostics: bool
    min_dsr: float
    max_overlap_ratio: float
    max_profile_correlation: float
    allow_discovery_promotion: bool
    program_id: str
    retail_profile: str
    objective_name: str
    objective_spec: Optional[str]
    retail_profiles_spec: Optional[str]
    promotion_profile: str = "auto"

    def resolved_out_dir(self) -> Path:
        data_root = get_data_root()
        return (
            self.out_dir
            if self.out_dir is not None
            else data_root / "reports" / "promotions" / self.run_id
        )

    def manifest_params(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "symbols": self.symbols,
            "out_dir": str(self.out_dir) if self.out_dir is not None else None,
            "max_q_value": self.max_q_value,
            "min_events": self.min_events,
            "min_stability_score": self.min_stability_score,
            "min_sign_consistency": self.min_sign_consistency,
            "min_cost_survival_ratio": self.min_cost_survival_ratio,
            "max_negative_control_pass_rate": self.max_negative_control_pass_rate,
            "min_tob_coverage": self.min_tob_coverage,
            "require_hypothesis_audit": int(self.require_hypothesis_audit),
            "allow_missing_negative_controls": int(self.allow_missing_negative_controls),
            "require_multiplicity_diagnostics": int(self.require_multiplicity_diagnostics),
            "min_dsr": self.min_dsr,
            "max_overlap_ratio": self.max_overlap_ratio,
            "max_profile_correlation": self.max_profile_correlation,
            "allow_discovery_promotion": int(self.allow_discovery_promotion),
            "program_id": self.program_id,
            "retail_profile": self.retail_profile,
            "objective_name": self.objective_name,
            "objective_spec": self.objective_spec,
            "retail_profiles_spec": self.retail_profiles_spec,
            "promotion_profile": self.promotion_profile,
        }


PROMOTION_CONFIG_DEFAULTS: Dict[str, Any] = {
    "max_q_value": 0.10,
    "min_events": 0,
    "min_stability_score": 0.05,
    "min_sign_consistency": 0.67,
    "min_cost_survival_ratio": 0.75,
    "max_negative_control_pass_rate": 0.01,
    "min_tob_coverage": 0.60,
    "require_hypothesis_audit": True,
    "allow_missing_negative_controls": True,
    "require_multiplicity_diagnostics": False,
    "min_dsr": 0.5,
    "max_overlap_ratio": 0.80,
    "max_profile_correlation": 0.90,
    "allow_discovery_promotion": False,
    "program_id": "default_program",
    "retail_profile": "capital_constrained",
    "objective_name": "",
    "objective_spec": None,
    "retail_profiles_spec": None,
    "promotion_profile": "auto",
}


def build_promotion_config(
    *,
    run_id: str,
    symbols: str = "",
    out_dir: Optional[Path] = None,
    **overrides: Any,
) -> PromotionConfig:
    values = dict(PROMOTION_CONFIG_DEFAULTS)
    values.update(overrides)
    values.update(
        {
            "run_id": str(run_id),
            "symbols": str(symbols),
            "out_dir": out_dir,
        }
    )
    return PromotionConfig(**values)


@dataclass(frozen=True)
class ResolvedPromotionPolicy:
    promotion_profile: str
    base_min_events: int
    dynamic_min_events: Dict[str, int]
    min_net_expectancy_bps: float
    max_fee_plus_slippage_bps: Optional[float]
    max_daily_turnover_multiple: Optional[float]
    require_retail_viability: bool
    require_low_capital_viability: bool
    enforce_baseline_beats_complexity: bool
    enforce_placebo_controls: bool
    enforce_timeframe_consensus: bool
    multiplicity_scope_mode: str = "campaign_lineage"
    require_scope_level_multiplicity: bool = True
    allow_multiplicity_scope_degraded: bool = True
    use_effective_q_value: bool = True


PROMOTION_CLASSES: tuple[str, ...] = ("paper_promoted", "production_promoted")
DEFAULT_DEPLOYMENT_STATE_BY_PROMOTION_CLASS: dict[str, str] = {
    "paper_promoted": "paper_only",
    "production_promoted": "live_enabled",
}


def normalize_promotion_class(value: str | None, *, default: str = "paper_promoted") -> str:
    token = str(value or "").strip().lower()
    if token in PROMOTION_CLASSES:
        return token
    return default


def default_deployment_state_for_promotion_class(
    value: str | None, *, default: str = "paper_only"
) -> str:
    token = normalize_promotion_class(value, default="paper_promoted")
    return DEFAULT_DEPLOYMENT_STATE_BY_PROMOTION_CLASS.get(token, default)


@dataclass
class PromotionServiceResult:
    exit_code: int
    output_dir: Path
    audit_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    promoted_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    diagnostics: Dict[str, Any] = field(default_factory=dict)


def _record_degraded_state(
    diagnostics: Dict[str, Any],
    *,
    code: str,
    message: str,
    details: Dict[str, Any] | None = None,
) -> None:
    states = diagnostics.setdefault("degraded_states", [])
    if not isinstance(states, list):
        states = []
        diagnostics["degraded_states"] = states
    payload: Dict[str, Any] = {
        "code": str(code).strip(),
        "status": "degraded",
        "message": str(message).strip(),
    }
    if details:
        payload["details"] = dict(details)
    states.append(payload)


_empty_artifact_frame = _promotion_artifacts._empty_artifact_frame
_EMPTY_PROMOTION_AUDIT_COLUMNS = _promotion_artifacts._EMPTY_PROMOTION_AUDIT_COLUMNS
_EMPTY_BUNDLE_SUMMARY_COLUMNS = _promotion_artifacts._EMPTY_BUNDLE_SUMMARY_COLUMNS
_EMPTY_PROMOTION_DECISION_COLUMNS = _promotion_artifacts._EMPTY_PROMOTION_DECISION_COLUMNS


def _is_promoted_audit_row(row: Dict[str, Any]) -> bool:
    for key in ("promotion_status", "promotion_decision"):
        if str(row.get(key, "")).strip().lower() == "promoted":
            return True
    return bool(row.get("eligible", False))


def _parse_valid_evidence_bundle(raw: Any) -> Dict[str, Any] | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        validate_evidence_bundle(raw)
        return dict(raw)
    text = str(raw).strip()
    if not text:
        return None
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("evidence_bundle_json must decode to an object")
    validate_evidence_bundle(payload)
    return payload


_annotate_promotion_audit_decisions = _promotion_diagnostics._annotate_promotion_audit_decisions
_build_promotion_decision_diagnostics = _promotion_diagnostics._build_promotion_decision_diagnostics
_apply_artifact_audit_stamp = _promotion_artifacts._apply_artifact_audit_stamp
_trace_payload = _promotion_diagnostics._trace_payload
_failed_stages_from_trace = _promotion_diagnostics._failed_stages_from_trace
_primary_reject_reason = _promotion_diagnostics._primary_reject_reason
_classify_rejection = _promotion_diagnostics._classify_rejection
_recommended_next_action_for_rejection = (
    _promotion_diagnostics._recommended_next_action_for_rejection
)


_read_csv_or_parquet = _promotion_inputs._read_csv_or_parquet
_load_bridge_metrics = _promotion_inputs._load_bridge_metrics
_merge_bridge_metrics = _promotion_inputs._merge_bridge_metrics
_parse_run_symbols = _promotion_inputs._parse_run_symbols
_hydrate_edge_candidates_from_phase2 = _promotion_inputs._hydrate_edge_candidates_from_phase2
_load_negative_control_summary = _promotion_inputs._load_negative_control_summary
_canonicalize_candidate_audit_keys = _promotion_inputs._canonicalize_candidate_audit_keys


def _load_hypothesis_index(
    *,
    run_id: str,
    data_root: Path,
    diagnostics: Dict[str, Any] | None = None,
) -> Dict[str, Dict[str, Any]]:
    return _promotion_inputs._load_hypothesis_index(
        run_id=run_id,
        data_root=data_root,
        diagnostics=diagnostics,
        read_csv_or_parquet_fn=_read_csv_or_parquet,
        record_degraded_state_fn=_record_degraded_state,
    )


_load_dynamic_min_events_by_event = _promotion_policy._load_dynamic_min_events_by_event
_resolve_promotion_profile = _promotion_policy._resolve_promotion_profile


def _resolve_promotion_policy(
    *,
    config: PromotionConfig,
    contract: Any,
    source_run_mode: str,
    project_root: Path,
) -> ResolvedPromotionPolicy:
    return _promotion_policy._resolve_promotion_policy(
        config=config,
        contract=contract,
        source_run_mode=source_run_mode,
        project_root=project_root,
        load_dynamic_min_events_by_event_fn=_load_dynamic_min_events_by_event,
        resolved_policy_cls=ResolvedPromotionPolicy,
    )


_write_promotion_lineage_audit = _promotion_artifacts._write_promotion_lineage_audit
_write_multiplicity_scope_diagnostics = _promotion_artifacts._write_multiplicity_scope_diagnostics
REQUIRED_PROMOTION_FIELDS = _promotion_inputs.REQUIRED_PROMOTION_FIELDS
_missing_or_blank_mask = _promotion_inputs._missing_or_blank_mask
_coalesce_column = _promotion_inputs._coalesce_column
_fill_numeric_column_from_scaled_sources = _promotion_inputs._fill_numeric_column_from_scaled_sources
_derive_cost_survival_ratio_from_bridge_flags = (
    _promotion_inputs._derive_cost_survival_ratio_from_bridge_flags
)
_hydrate_canonical_promotion_aliases = _promotion_inputs._hydrate_canonical_promotion_aliases
_diagnose_missing_fields = _promotion_inputs._diagnose_missing_fields


def execute_promotion(config: PromotionConfig) -> PromotionServiceResult:
    data_root = get_data_root()
    out_dir = config.resolved_out_dir()
    ensure_dir(out_dir)
    manifest = start_manifest("promote_candidates", config.run_id, config.manifest_params(), [], [])
    audit_df = pd.DataFrame()
    promoted_df = pd.DataFrame()
    diagnostics: Dict[str, Any] = {}
    promotion_input_mode = "canonical"

    try:
        # Require canonical validation before promotion.
        from project.research.validation.result_writer import load_validation_bundle
        from project.research.services.evaluation_service import (
            ValidationService,
            select_stage_candidate_table,
        )
        from project.research.validation.contracts import PromotionReasonCodes

        diagnostics["compat_mode_used"] = False
        diagnostics["compat_reason"] = ""
        diagnostics["compat_source_artifacts"] = []
        diagnostics["canonical_contract_bypassed"] = False
        diagnostics["degraded_states"] = []

        val_bundle = load_validation_bundle(
            config.run_id,
            strict=True,
            compatibility_mode=False,
        )
        if val_bundle is None:
            raise MissingArtifactError(
                f"Promotion rejected for {config.run_id}: "
                f"missing validation bundle. {PromotionReasonCodes.NOT_VALIDATED}. "
                "Canonical validation is mandatory."
            )

        canonical_candidate_path = (
            out_dir.parent.parent
            / "validation"
            / config.run_id
            / "promotion_ready_candidates.parquet"
        )
        canonical_candidate_csv_path = canonical_candidate_path.with_suffix(".csv")
        if not canonical_candidate_path.exists() and not canonical_candidate_csv_path.exists():
            raise MissingArtifactError(
                f"Canonical promotion-ready candidates not found at {canonical_candidate_path}. "
                "Run canonical validation before promotion."
            )

        if not val_bundle.validated_candidates:
            promotion_input_mode = "canonical_empty"
            audit_df = _empty_artifact_frame(*_EMPTY_PROMOTION_AUDIT_COLUMNS)
            promoted_df = normalize_dataframe_for_schema(pd.DataFrame(), "promoted_candidates")
            evidence_bundle_summary = _empty_artifact_frame(*_EMPTY_BUNDLE_SUMMARY_COLUMNS)
            promotion_decisions = _empty_artifact_frame(*_EMPTY_PROMOTION_DECISION_COLUMNS)
            serialize_evidence_bundles([], out_dir / "evidence_bundles.jsonl")
            diagnostics["promotion_input_mode"] = promotion_input_mode
            diagnostics["decision_summary"] = _build_promotion_decision_diagnostics(audit_df)
            diagnostics["live_thesis_export"] = {
                "output_path": "",
                "thesis_count": 0,
                "active_count": 0,
                "pending_count": 0,
                "contract_json_path": "",
                "contract_md_path": "",
            }
            diagnostics["promotion_lineage_audit"] = {}
            diagnostics["no_promotable_candidates"] = True
            diagnostics["no_promotable_reason"] = "validation produced zero validated candidates"
            write_promotion_reports(
                out_dir=out_dir,
                audit_df=audit_df,
                promoted_df=promoted_df,
                evidence_bundle_summary=evidence_bundle_summary,
                promotion_decisions=promotion_decisions,
                diagnostics=diagnostics,
                promotion_summary=pd.DataFrame(
                    columns=[
                        "candidate_id",
                        "event_type",
                        "stage",
                        "statistic",
                        "threshold",
                        "pass_fail",
                    ]
                ),
            )
            finalize_manifest(manifest, "success", stats=diagnostics)
            return PromotionServiceResult(0, out_dir, audit_df, promoted_df, diagnostics)

        source_manifest = load_run_manifest(config.run_id)
        source_run_mode = str(source_manifest.get("run_mode", "")).strip().lower()
        source_profile = str(source_manifest.get("discovery_profile", "")).strip().lower()
        confirmatory_rerun_run_id = str(
            source_manifest.get("confirmatory_rerun_run_id", "")
        ).strip()
        candidate_origin_run_id = str(source_manifest.get("candidate_origin_run_id", "")).strip()
        program_id = str(source_manifest.get("program_id", config.program_id)).strip()
        if source_run_mode == "discovery":
            source_run_mode = "exploratory"
        is_exploratory = source_run_mode == "exploratory"
        is_confirmatory = source_run_mode in {
            "confirmatory",
            "production",
            "certification",
            "promotion",
            "deploy",
        }
        if is_exploratory and not config.allow_discovery_promotion:
            raise ValueError(
                f"Promotion blocked for {config.run_id}: source run_mode={source_run_mode}. "
                "Promotion requires a confirmatory run."
            )

        contract = resolve_objective_profile_contract(
            project_root=PROJECT_ROOT,
            data_root=data_root,
            run_id=config.run_id,
            objective_name=config.objective_name.strip() or None,
            objective_spec_path=config.objective_spec,
            retail_profile_name=config.retail_profile.strip() or None,
            retail_profiles_spec_path=config.retail_profiles_spec,
            required=True,
        )

        # Prefer canonical promotion-ready candidates artifact over ambient table loading
        candidates_df = pd.DataFrame()

        if canonical_candidate_path.exists() or canonical_candidate_csv_path.exists():
            promotion_input_mode = "canonical"
            validation_meta_df = _read_csv_or_parquet(canonical_candidate_path)

            validation_svc = ValidationService(data_root=data_root)
            source_tables = validation_svc.load_candidate_tables(config.run_id)
            source_candidates_df = select_stage_candidate_table(source_tables)

            if source_candidates_df.empty:
                raise IncompleteLineageError(
                    f"Canonical promotion input for run {config.run_id} had validation metadata "
                    "but no source candidate tables."
                )
            elif validation_meta_df.empty:
                raise SchemaMismatchError(
                    f"Canonical promotion-ready candidates file is empty for run {config.run_id}."
                )
            else:
                validated_ids = set(validation_meta_df["candidate_id"].dropna().astype(str))
                candidates_df = source_candidates_df[
                    source_candidates_df["candidate_id"].astype(str).isin(validated_ids)
                ].copy()

                for col in validation_meta_df.columns:
                    if col not in candidates_df.columns and col != "candidate_id":
                        candidates_df[col] = (
                            validation_meta_df.set_index("candidate_id")
                            .reindex(candidates_df["candidate_id"].astype(str))[col]
                            .values
                        )

                if candidates_df.empty and not validation_meta_df.empty:
                    raise IncompleteLineageError(
                        "No matching candidates found between validation metadata and source "
                        f"tables for run {config.run_id}."
                    )

            if not candidates_df.empty:
                missing_before_hydration = _diagnose_missing_fields(candidates_df)
                candidates_df = _hydrate_canonical_promotion_aliases(candidates_df)
                missing_fields = _diagnose_missing_fields(candidates_df)
                if missing_before_hydration:
                    diagnostics["canonical_missing_fields_before_alias_hydration"] = (
                        missing_before_hydration
                    )
                    diagnostics["canonical_alias_hydrated_fields"] = [
                        field for field in missing_before_hydration if field not in missing_fields
                    ]
                if missing_fields:
                    diagnostics["canonical_missing_fields"] = missing_fields
                    raise SchemaMismatchError(
                        "Canonical promotion input missing required fields: "
                        + ", ".join(missing_fields)
                    )
                logging.info(
                    "Loaded %d candidates via canonical path for run %s",
                    len(candidates_df),
                    config.run_id,
                )
        else:
            raise CompatibilityRequiredError(
                f"Canonical promotion-ready candidates not found at {canonical_candidate_path}. "
                "Run canonical validation before promotion."
            )

        diagnostics["promotion_input_mode"] = promotion_input_mode

        # Workstream B: Load search-burden summary if present
        from project.research.contracts.search_burden import (
            default_search_burden_dict,
            load_search_burden_summary,
            merge_search_burden_columns,
        )

        search_burden = None
        search_burden_paths = [
            data_root / "reports" / "phase2" / config.run_id,
            data_root / "runs" / config.run_id,
        ]
        for search_burden_dir in search_burden_paths:
            search_burden = load_search_burden_summary(search_burden_dir)
            if search_burden is not None:
                diagnostics["search_burden_summary_path"] = str(
                    search_burden_dir / "search_burden_summary.json"
                )
                break
        if search_burden is None:
            logging.warning(
                "No search-burden summary found for run %s; using defaults (estimated mode)",
                config.run_id,
            )
            search_burden = default_search_burden_dict(estimated=True)

        candidates_df = merge_search_burden_columns(candidates_df, defaults=search_burden)

        candidates_df = _canonicalize_candidate_audit_keys(candidates_df)

        if is_confirmatory and not candidates_df.empty:
            if "confirmatory_locked" in candidates_df.columns:
                locked_candidates = candidates_df["confirmatory_locked"].fillna(False).astype(bool)
            else:
                locked_candidates = pd.Series(False, index=candidates_df.index, dtype=bool)
            if not bool(locked_candidates.all()):
                raise ValueError("Confirmatory run contains unlocked candidates.")
            curr_ontology_hash = ontology_spec_hash(PROJECT_ROOT.parent)
            frozen_hashes = candidates_df["frozen_spec_hash"].unique()
            if len(frozen_hashes) > 1 or frozen_hashes[0] != curr_ontology_hash:
                raise ValueError(
                    f"Spec hash mismatch in confirmatory run: {frozen_hashes} vs {curr_ontology_hash}"
                )

        ontology_hash = ontology_spec_hash(PROJECT_ROOT.parent)
        gate_spec = _load_gates_spec(PROJECT_ROOT.parent)
        promotion_confirmatory_gates = gate_spec.get("promotion_confirmatory_gates", {})
        hypothesis_index = _load_hypothesis_index(
            run_id=config.run_id,
            data_root=data_root,
            diagnostics=diagnostics,
        )
        promotion_spec = {
            "run_id": config.run_id,
            "ontology_spec_hash": ontology_hash,
            "source_run_mode": source_run_mode,
            "source_profile": source_profile,
            "confirmatory_rerun_run_id": confirmatory_rerun_run_id,
            "candidate_origin_run_id": candidate_origin_run_id,
            "program_id": program_id,
            "promotion_basis": "confirmatory_only" if is_confirmatory else "direct",
            "is_reduced_evidence": bool(is_exploratory),
            "promotion_confirmatory_gates": promotion_confirmatory_gates,
        }
        negative_control_summary = _load_negative_control_summary(config.run_id)
        resolved_policy = _resolve_promotion_policy(
            config=config,
            contract=contract,
            source_run_mode=source_run_mode,
            project_root=PROJECT_ROOT.parent,
        )
        audit_df, promoted_df, diagnostics = promote_candidates(
            candidates_df=candidates_df,
            promotion_spec=promotion_spec,
            hypothesis_index=hypothesis_index,
            negative_control_summary=negative_control_summary,
            contract=contract,
            dynamic_min_events=resolved_policy.dynamic_min_events,
            base_min_events=resolved_policy.base_min_events,
            max_q_value=config.max_q_value,
            min_stability_score=config.min_stability_score,
            min_sign_consistency=config.min_sign_consistency,
            min_cost_survival_ratio=config.min_cost_survival_ratio,
            max_negative_control_pass_rate=config.max_negative_control_pass_rate,
            min_tob_coverage=config.min_tob_coverage,
            require_hypothesis_audit=config.require_hypothesis_audit,
            allow_missing_negative_controls=config.allow_missing_negative_controls,
            require_multiplicity_diagnostics=config.require_multiplicity_diagnostics,
            min_dsr=config.min_dsr,
            max_overlap_ratio=config.max_overlap_ratio,
            max_profile_correlation=config.max_profile_correlation,
            promotion_profile=resolved_policy.promotion_profile,
            min_net_expectancy_bps=resolved_policy.min_net_expectancy_bps,
            max_fee_plus_slippage_bps=resolved_policy.max_fee_plus_slippage_bps,
            max_daily_turnover_multiple=resolved_policy.max_daily_turnover_multiple,
            require_retail_viability=resolved_policy.require_retail_viability,
            require_low_capital_viability=resolved_policy.require_low_capital_viability,
            enforce_baseline_beats_complexity=resolved_policy.enforce_baseline_beats_complexity,
            enforce_placebo_controls=resolved_policy.enforce_placebo_controls,
            enforce_timeframe_consensus=resolved_policy.enforce_timeframe_consensus,
            multiplicity_scope_mode=getattr(
                resolved_policy, "multiplicity_scope_mode", "campaign_lineage"
            ),
            require_scope_level_multiplicity=getattr(
                resolved_policy, "require_scope_level_multiplicity", True
            ),
            allow_multiplicity_scope_degraded=getattr(
                resolved_policy, "allow_multiplicity_scope_degraded", True
            ),
            use_effective_q_value=getattr(resolved_policy, "use_effective_q_value", True),
        )
        diagnostics["promotion_profile"] = resolved_policy.promotion_profile

        # Write multiplicity scope diagnostics
        multiplicity_scope_diag = diagnostics.get("multiplicity_scope_diagnostics", {})
        if multiplicity_scope_diag:
            _write_multiplicity_scope_diagnostics(out_dir, multiplicity_scope_diag)
        audit_statistical_df = build_promotion_statistical_audit(
            audit_df=audit_df,
            max_q_value=config.max_q_value,
            min_stability_score=config.min_stability_score,
            min_sign_consistency=config.min_sign_consistency,
            min_cost_survival_ratio=config.min_cost_survival_ratio,
            max_negative_control_pass_rate=config.max_negative_control_pass_rate,
            min_tob_coverage=config.min_tob_coverage,
            min_net_expectancy_bps=resolved_policy.min_net_expectancy_bps,
            max_fee_plus_slippage_bps=resolved_policy.max_fee_plus_slippage_bps,
            max_daily_turnover_multiple=resolved_policy.max_daily_turnover_multiple,
            require_hypothesis_audit=config.require_hypothesis_audit,
            allow_missing_negative_controls=config.allow_missing_negative_controls,
            require_retail_viability=bool(resolved_policy.require_retail_viability),
            require_low_capital_viability=bool(resolved_policy.require_low_capital_viability),
        )
        audit_statistical_df["source_run_mode"] = source_run_mode
        audit_statistical_df["source_profile"] = source_profile
        audit_statistical_df["promotion_profile"] = resolved_policy.promotion_profile
        audit_statistical_df["confirmatory_rerun_run_id"] = confirmatory_rerun_run_id
        audit_df = _annotate_promotion_audit_decisions(audit_statistical_df.copy())
        audit_df = _apply_artifact_audit_stamp(audit_df)
        audit_df = annotate_regime_metadata(audit_df)
        diagnostics["decision_summary"] = _build_promotion_decision_diagnostics(audit_df)
        promoted_df = stabilize_promoted_output_schema(
            promoted_df=promoted_df,
            audit_df=audit_df,
        ).copy()
        promoted_df = annotate_regime_metadata(promoted_df)

        evidence_bundles = []
        invalid_promoted_rows: list[str] = []
        if not audit_df.empty:
            for row in audit_df.to_dict(orient="records"):
                candidate_id = str(row.get("candidate_id", "")).strip()
                if not candidate_id:
                    candidate_id = str(row.get("hypothesis_id", "")).strip()
                raw = row.get("evidence_bundle_json")
                try:
                    bundle = _parse_valid_evidence_bundle(raw)
                except (json.JSONDecodeError, ValueError) as exc:
                    if _is_promoted_audit_row(row):
                        invalid_promoted_rows.append(f"{candidate_id or '<unknown>'}: {exc}")
                    continue
                if bundle is None:
                    if _is_promoted_audit_row(row):
                        invalid_promoted_rows.append(
                            f"{candidate_id or '<unknown>'}: missing evidence bundle"
                        )
                    continue
                evidence_bundles.append(bundle)
        if invalid_promoted_rows:
            raise ValueError(
                "Promoted rows are missing valid evidence bundles: "
                + "; ".join(invalid_promoted_rows)
            )
        serialize_evidence_bundles(evidence_bundles, out_dir / "evidence_bundles.jsonl")
        evidence_bundle_summary = pd.DataFrame(
            [bundle_to_flat_record(bundle) for bundle in evidence_bundles]
        )
        evidence_bundle_summary = annotate_regime_metadata(evidence_bundle_summary)
        evidence_bundle_summary = normalize_dataframe_for_schema(
            evidence_bundle_summary,
            "evidence_bundle_summary",
        )
        decision_cols = [
            column
            for column in [
                "candidate_id",
                "event_type",
                "hypothesis_id",
                "promotion_decision",
                "promotion_track",
                "rank_score",
                "rejection_reasons",
                "policy_version",
                "bundle_version",
                "is_reduced_evidence",
                "canonical_regime",
                "subtype",
                "phase",
                "evidence_mode",
                "recommended_bucket",
                "regime_bucket",
                "routing_profile_id",
            ]
            if column in evidence_bundle_summary.columns
        ]
        promotion_decisions = normalize_dataframe_for_schema(
            evidence_bundle_summary.reindex(columns=decision_cols),
            "promotion_decisions",
        )

        summary_rows = pd.DataFrame(
            columns=["candidate_id", "event_type", "stage", "statistic", "threshold", "pass_fail"]
        )
        if not audit_df.empty:
            stage_rows = []
            for row in audit_df.to_dict(orient="records"):
                candidate_id = str(row.get("candidate_id", "")).strip()
                event_type = str(row.get("event_type", "")).strip()
                trace = row.get("promotion_metrics_trace", "{}")
                try:
                    trace_payload = (
                        json.loads(trace) if isinstance(trace, str) else dict(trace or {})
                    )
                except (json.JSONDecodeError, TypeError, ValueError):
                    trace_payload = {}
                for stage, meta in sorted(trace_payload.items()):
                    observed = meta.get("observed", {}) if isinstance(meta, dict) else {}
                    thresholds = meta.get("thresholds", {}) if isinstance(meta, dict) else {}
                    stage_rows.append(
                        {
                            "candidate_id": candidate_id,
                            "event_type": event_type,
                            "stage": stage,
                            "statistic": json.dumps(observed, sort_keys=True),
                            "threshold": json.dumps(thresholds, sort_keys=True),
                            "pass_fail": bool(meta.get("passed", False))
                            if isinstance(meta, dict)
                            else False,
                        }
                    )
            summary_rows = pd.DataFrame(stage_rows)
        diagnostics["evidence_bundle_count"] = int(len(evidence_bundles))
        diagnostics["evidence_bundle_summary_rows"] = int(len(evidence_bundle_summary))
        write_promotion_reports(
            out_dir=out_dir,
            audit_df=audit_df,
            promoted_df=promoted_df,
            evidence_bundle_summary=evidence_bundle_summary,
            promotion_decisions=promotion_decisions,
            diagnostics=diagnostics,
            promotion_summary=summary_rows,
        )
        from project.research.live_export import export_promoted_theses_for_run

        thesis_export = export_promoted_theses_for_run(
            config.run_id,
            data_root=get_data_root(),
            bundles=[] if promoted_df.empty else evidence_bundles,
            promoted_df=promoted_df,
            allow_bundle_only_export=bool(promoted_df.empty),
            compatibility_mode=bool(diagnostics.get("compat_mode_used", False)),
        )
        diagnostics["live_thesis_export"] = {
            "output_path": str(thesis_export.output_path),
            "thesis_count": int(thesis_export.thesis_count),
            "active_count": int(thesis_export.active_count),
            "pending_count": int(thesis_export.pending_count),
            "contract_json_path": str(thesis_export.contract_json_path)
            if thesis_export.contract_json_path
            else "",
            "contract_md_path": str(thesis_export.contract_md_path)
            if thesis_export.contract_md_path
            else "",
        }
        diagnostics["promotion_lineage_audit"] = _write_promotion_lineage_audit(
            out_dir=out_dir,
            run_id=config.run_id,
            evidence_bundles=evidence_bundles,
            promoted_df=promoted_df,
            live_export_diagnostics=diagnostics.get("live_thesis_export"),
        )
        diagnostics["historical_trust"] = build_run_historical_trust_summary(
            run_id=config.run_id,
            data_root=get_data_root(),
        )
        atomic_write_json(out_dir / "promotion_diagnostics.json", diagnostics)
        diagnostics["promotion_lineage_audit"] = _write_promotion_lineage_audit(
            out_dir=out_dir,
            run_id=config.run_id,
            evidence_bundles=evidence_bundles,
            promoted_df=promoted_df,
            live_export_diagnostics=diagnostics.get("live_thesis_export"),
            historical_trust=diagnostics.get("historical_trust"),
        )

        # Sprint 7: Artifact manifest
        try:
            from project.research.validation.manifest import RunArtifactManifest
            from datetime import datetime, timezone

            artifact_manifest = RunArtifactManifest(
                run_id=config.run_id,
                stage="promote",
                created_at=datetime.now(timezone.utc).isoformat(),
                upstream_run_ids=[config.run_id],
                artifacts={
                    "promotion_audit": "promotion_audit.parquet",
                    "promoted_candidates": "promoted_candidates.parquet",
                    "promotion_summary": "promotion_summary.csv",
                    "promotion_diagnostics": "promotion_diagnostics.json",
                },
            )
            artifact_manifest.persist(out_dir)
        except (OSError, TypeError, ValueError) as exc:
            wrapped = ArtifactPersistenceError(
                f"Failed to persist artifact manifest for promotion run {config.run_id}: {exc}"
            )
            logging.warning("%s", wrapped)
            _record_degraded_state(
                diagnostics,
                code="artifact_manifest_persist_failed",
                message=str(wrapped),
                details={"run_id": config.run_id, "output_dir": str(out_dir)},
            )

        finalize_manifest(manifest, "success", stats=diagnostics)
        return PromotionServiceResult(0, out_dir, audit_df, promoted_df, diagnostics)
    except MissingArtifactError as exc:
        err_msg = str(exc)
        logging.warning("Promotion skipped: %s", err_msg)
        _record_degraded_state(
            diagnostics,
            code="promotion_inputs_missing",
            message=err_msg,
            details={"run_id": config.run_id},
        )
        finalize_manifest(manifest, "warning", error=err_msg)
        diagnostics["error"] = err_msg
        return PromotionServiceResult(1, out_dir, audit_df, promoted_df, diagnostics)
    except FileNotFoundError:
        raise
    except (
        CompatibilityRequiredError,
        DataIntegrityError,
        IncompleteLineageError,
        RuntimeError,
        SchemaMismatchError,
        ValueError,
    ) as exc:
        err_msg = str(exc)
        logging.exception("Promotion failed: %s", exc)
        _record_degraded_state(
            diagnostics,
            code="promotion_stage_failed",
            message=err_msg,
            details={"run_id": config.run_id, "exception_type": type(exc).__name__},
        )
        finalize_manifest(manifest, "failed", error=err_msg)
        diagnostics["error"] = err_msg
        return PromotionServiceResult(1, out_dir, audit_df, promoted_df, diagnostics)
