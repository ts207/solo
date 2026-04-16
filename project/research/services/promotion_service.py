from __future__ import annotations

import importlib
import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

import pandas as pd

from project import PROJECT_ROOT
from project.contracts.schemas import normalize_dataframe_for_schema
from project.core.config import get_data_root
from project.core.coercion import as_bool, safe_int
from project.core.exceptions import (
    ArtifactPersistenceError,
    ArtifactReadError,
    CompatibilityRequiredError,
    DataIntegrityError,
    IncompleteLineageError,
    MissingArtifactError,
    SchemaMismatchError,
)
from project.io.parquet_compat import read_parquet_compat
from project.io.utils import atomic_write_json, atomic_write_text, ensure_dir, read_parquet, write_parquet
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
from project.research.contracts.stat_regime import (
    STAT_REGIME_POST_AUDIT,
    AUDIT_STATUS_CURRENT,
    AUDIT_STATUS_DEGRADED,
    ARTIFACT_AUDIT_VERSION_PHASE1_V1,
    default_audit_stamp,
)
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
    values.update({
        "run_id": str(run_id),
        "symbols": str(symbols),
        "out_dir": out_dir,
    })
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


def default_deployment_state_for_promotion_class(value: str | None, *, default: str = "paper_only") -> str:
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


def _empty_artifact_frame(*columns: str) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


_EMPTY_PROMOTION_AUDIT_COLUMNS = (
    "candidate_id",
    "event_type",
    "promotion_decision",
    "promotion_track",
    "policy_version",
    "bundle_version",
    "is_reduced_evidence",
    "gate_promo_statistical",
    "gate_promo_stability",
    "gate_promo_cost_survival",
    "gate_promo_negative_control",
)
_EMPTY_BUNDLE_SUMMARY_COLUMNS = (
    "candidate_id",
    "event_type",
    "promotion_decision",
    "promotion_track",
    "policy_version",
    "bundle_version",
    "is_reduced_evidence",
)
_EMPTY_PROMOTION_DECISION_COLUMNS = _EMPTY_BUNDLE_SUMMARY_COLUMNS


def _trace_payload(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}
    return {}


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


def _failed_stages_from_trace(raw: Any) -> List[str]:
    payload = _trace_payload(raw)
    failed: List[str] = []
    for stage, meta in payload.items():
        if not isinstance(meta, dict):
            continue
        if meta.get("passed") is False:
            failed.append(str(stage))
    return failed


def _primary_reject_reason(row: Dict[str, Any]) -> str:
    primary = str(row.get("promotion_fail_reason_primary", "")).strip()
    if primary:
        return primary
    reject_reason = str(row.get("reject_reason", "")).strip()
    if not reject_reason:
        return ""
    return next((token for token in reject_reason.split("|") if token.strip()), "")


def _classify_rejection(row: Dict[str, Any], failed_stages: List[str]) -> str:
    primary_gate = str(row.get("promotion_fail_gate_primary", "")).strip().lower()
    primary_reason = _primary_reject_reason(row).strip().lower()
    reject_reason = str(row.get("reject_reason", "")).strip().lower()
    weakest_fail_stage = str(row.get("weakest_fail_stage", "")).strip().lower()
    combined = " ".join(
        [primary_gate, primary_reason, reject_reason, weakest_fail_stage, " ".join(failed_stages)]
    )

    if any(
        token in combined
        for token in [
            "spec hash mismatch",
            "bridge_evaluation_failed",
            "unlocked candidates",
            "schema",
            "contract",
        ]
    ):
        return "contract_failure"
    if any(
        token in combined
        for token in [
            "negative_control_missing",
            "failed_placebo_controls",
            "hypothesis_audit",
            "missing_realized_oos_path",
            "oos_insufficient_samples",
            "oos_validation",
            "confirmatory",
            "validation",
            "test_support",
            "multiplicity_strict",
        ]
    ):
        return "weak_holdout_support"
    if any(
        token in combined
        for token in [
            "expectancy",
            "after_cost",
            "turnover",
            "retail",
            "low_capital",
            "dsr",
            "economic",
            "tradable",
        ]
    ):
        return "weak_economics"
    if any(
        token in combined
        for token in [
            "baseline",
            "complexity",
            "placebo",
            "timeframe_consensus",
            "overlap",
            "profile_correlation",
            "regime_unstable",
            "scope",
        ]
    ):
        return "scope_mismatch"
    if failed_stages:
        return "scope_mismatch"
    return "unclassified"


def _recommended_next_action_for_rejection(classification: str) -> str:
    mapping = {
        "contract_failure": "repair_pipeline",
        "weak_holdout_support": "run_confirmatory",
        "weak_economics": "stop_or_reframe",
        "scope_mismatch": "narrow_scope",
        "unclassified": "review_manually",
    }
    return mapping.get(str(classification).strip().lower(), "review_manually")


def _annotate_promotion_audit_decisions(audit_df: pd.DataFrame) -> pd.DataFrame:
    if audit_df.empty:
        out = audit_df.copy()
        out["primary_reject_reason"] = pd.Series(dtype="object")
        out["failed_gate_count"] = pd.Series(dtype="int64")
        out["failed_gate_list"] = pd.Series(dtype="object")
        out["weakest_fail_stage"] = pd.Series(dtype="object")
        return out

    rows: List[Dict[str, Any]] = []
    for row in audit_df.to_dict(orient="records"):
        failed_stages = _failed_stages_from_trace(row.get("promotion_metrics_trace", {}))
        primary_gate = str(row.get("promotion_fail_gate_primary", "")).strip()
        weakest_fail_stage = failed_stages[0] if failed_stages else primary_gate
        rows.append(
            {
                **row,
                "primary_reject_reason": _primary_reject_reason(row),
                "failed_gate_count": int(len(failed_stages)),
                "failed_gate_list": "|".join(failed_stages),
                "weakest_fail_stage": weakest_fail_stage,
                "rejection_classification": _classify_rejection(row, failed_stages),
                "recommended_next_action": _recommended_next_action_for_rejection(
                    _classify_rejection(row, failed_stages)
                ),
            }
        )
    return pd.DataFrame(rows)


def _apply_artifact_audit_stamp(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        out = df.copy()
        out["stat_regime"] = pd.Series(dtype="object")
        out["audit_status"] = pd.Series(dtype="object")
        out["artifact_audit_version"] = pd.Series(dtype="object")
        return out
    out = df.copy()
    multiplicity_degraded = out.get("multiplicity_scope_degraded", pd.Series(False, index=out.index))
    if not isinstance(multiplicity_degraded, pd.Series):
        multiplicity_degraded = pd.Series(multiplicity_degraded, index=out.index)
    audit_status = multiplicity_degraded.astype(bool).apply(
        lambda x: AUDIT_STATUS_DEGRADED if x else AUDIT_STATUS_CURRENT
    )
    out["stat_regime"] = STAT_REGIME_POST_AUDIT
    out["audit_status"] = audit_status
    out["artifact_audit_version"] = ARTIFACT_AUDIT_VERSION_PHASE1_V1
    return out


def _build_promotion_decision_diagnostics(audit_df: pd.DataFrame) -> Dict[str, Any]:
    if audit_df.empty:
        return {
            "candidates_total": 0,
            "promoted_count": 0,
            "rejected_count": 0,
            "primary_fail_gate_counts": {},
            "primary_reject_reason_counts": {},
            "failed_stage_counts": {},
            "rejection_classification_counts": {},
            "recommended_next_action_counts": {},
            "mean_failed_gate_count_rejected": 0.0,
            "confirmatory_field_availability": {},
        }

    decision_counts = (
        audit_df.get("promotion_decision", pd.Series(dtype="object"))
        .astype(str)
        .value_counts()
        .to_dict()
    )
    rejected = audit_df[
        audit_df.get("promotion_decision", pd.Series(dtype="object")).astype(str) == "rejected"
    ].copy()
    fail_gates = (
        rejected.get("promotion_fail_gate_primary", pd.Series(dtype="object"))
        .astype(str)
        .str.strip()
    )
    fail_reasons = (
        rejected.get("primary_reject_reason", pd.Series(dtype="object")).astype(str).str.strip()
    )
    rejection_classes = (
        rejected.get("rejection_classification", pd.Series(dtype="object")).astype(str).str.strip()
    )
    next_actions = (
        rejected.get("recommended_next_action", pd.Series(dtype="object")).astype(str).str.strip()
    )
    stage_counter: Counter[str] = Counter()
    for value in rejected.get("failed_gate_list", pd.Series(dtype="object")).astype(str):
        for token in value.split("|"):
            token = token.strip()
            if token:
                stage_counter[token] += 1

    availability: Dict[str, Dict[str, int]] = {}
    field_names = [
        "plan_row_id",
        "has_realized_oos_path",
        "bridge_certified",
        "q_value_by",
        "q_value_cluster",
        "repeated_fold_consistency",
        "structural_robustness_score",
        "robustness_panel_complete",
        "gate_regime_stability",
        "gate_structural_break",
        "num_regimes_supported",
    ]
    for field in field_names:
        if field not in audit_df.columns:
            availability[field] = {"present": 0, "missing": int(len(audit_df))}
            continue
        series = audit_df[field]
        if series.dtype == bool:
            present_mask = pd.Series(True, index=series.index)
        elif pd.api.types.is_numeric_dtype(series):
            present_mask = pd.to_numeric(series, errors="coerce").notna()
        else:
            normalized = series.astype(str).str.strip().str.lower()
            present_mask = ~(series.isna() | normalized.isin({"", "nan", "none", "null"}))
        availability[field] = {
            "present": int(present_mask.sum()),
            "missing": int((~present_mask).sum()),
        }

    return {
        "candidates_total": int(len(audit_df)),
        "promoted_count": int(decision_counts.get("promoted", 0)),
        "rejected_count": int(decision_counts.get("rejected", 0)),
        "primary_fail_gate_counts": {
            str(k): int(v) for k, v in fail_gates[fail_gates != ""].value_counts().to_dict().items()
        },
        "primary_reject_reason_counts": {
            str(k): int(v)
            for k, v in fail_reasons[fail_reasons != ""].value_counts().to_dict().items()
        },
        "failed_stage_counts": dict(sorted(stage_counter.items())),
        "rejection_classification_counts": {
            str(k): int(v)
            for k, v in rejection_classes[rejection_classes != ""].value_counts().to_dict().items()
        },
        "recommended_next_action_counts": {
            str(k): int(v)
            for k, v in next_actions[next_actions != ""].value_counts().to_dict().items()
        },
        "mean_failed_gate_count_rejected": 0.0
        if rejected.empty
        else float(
            pd.to_numeric(rejected.get("failed_gate_count", 0), errors="coerce").fillna(0).mean()
        ),
        "confirmatory_field_availability": availability,
    }


def _read_csv_or_parquet(path: Path) -> pd.DataFrame:
    if path.suffix.lower() != ".parquet":
        return pd.read_csv(path)
    try:
        return pd.read_parquet(path)
    except RuntimeError:
        raise
    except (ImportError, OSError, ValueError):
        csv_fallback = path.with_suffix(".csv")
        if csv_fallback.exists():
            return pd.read_csv(csv_fallback)
        return read_parquet_compat(path)


def _read_bridge_table(path: Path) -> pd.DataFrame:
    return read_parquet(path)


def _normalize_statuses(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
        return [token.strip() for token in raw.split(",") if token.strip()]
    return []


def _canonicalize_candidate_audit_keys(candidates_df: pd.DataFrame) -> pd.DataFrame:
    if candidates_df.empty:
        return candidates_df.copy()
    out = candidates_df.copy()
    if "plan_row_id" not in out.columns:
        out["plan_row_id"] = ""
    if "hypothesis_id" not in out.columns:
        out["hypothesis_id"] = ""

    plan_row_ids = out["plan_row_id"].astype(str).str.strip()
    hypothesis_ids = out["hypothesis_id"].astype(str).str.strip()
    out["plan_row_id"] = plan_row_ids.where(plan_row_ids != "", hypothesis_ids)
    return out


def _load_hypothesis_index(
    *,
    run_id: str,
    data_root: Path,
    diagnostics: Dict[str, Any] | None = None,
) -> Dict[str, Dict[str, Any]]:
    phase2_root = data_root / "reports" / "phase2" / run_id
    if not phase2_root.exists():
        return {}

    candidate_paths: List[Path] = []
    for direct_name in ("hypothesis_registry.parquet", "hypothesis_registry.csv"):
        direct_path = phase2_root / direct_name
        if direct_path.exists():
            candidate_paths.append(direct_path)
    for pattern in ("*/*/hypothesis_registry.parquet", "*/*/hypothesis_registry.csv"):
        candidate_paths.extend(sorted(phase2_root.glob(pattern)))

    index: Dict[str, Dict[str, Any]] = {}
    seen_paths: set[Path] = set()
    for registry_path in candidate_paths:
        if registry_path in seen_paths or not registry_path.exists():
            continue
        seen_paths.add(registry_path)
        try:
            registry_df = _read_csv_or_parquet(registry_path)
        except (
            ArtifactReadError,
            ImportError,
            OSError,
            UnicodeDecodeError,
            ValueError,
            pd.errors.ParserError,
        ) as exc:
            wrapped = (
                exc
                if isinstance(exc, ArtifactReadError)
                else ArtifactReadError(f"Failed loading hypothesis registry {registry_path}: {exc}")
            )
            logging.warning("%s", wrapped)
            if diagnostics is not None:
                _record_degraded_state(
                    diagnostics,
                    code="hypothesis_registry_unreadable",
                    message=str(wrapped),
                    details={"path": str(registry_path)},
                )
            continue
        if registry_df.empty:
            continue

        for _, row in registry_df.iterrows():
            record = row.to_dict()
            hypothesis_id = str(record.get("hypothesis_id", "")).strip()
            if not hypothesis_id:
                continue
            plan_row_id = str(record.get("plan_row_id", "")).strip() or hypothesis_id
            statuses = _normalize_statuses(record.get("statuses"))
            normalized = dict(record)
            normalized["hypothesis_id"] = hypothesis_id
            normalized["plan_row_id"] = plan_row_id
            normalized["statuses"] = statuses or ["candidate_discovery"]
            normalized["executed"] = bool(record.get("executed", True))
            index.setdefault(hypothesis_id, normalized)
            index.setdefault(plan_row_id, normalized)
    return index


def _load_bridge_metrics(bridge_root: Path, symbol: str | None = None) -> pd.DataFrame:
    del symbol
    versioned_files = list(bridge_root.rglob("*_v1.csv"))
    parquet_files = list(bridge_root.rglob("bridge_evaluation.parquet"))
    fallback_csv_files = [
        path for path in bridge_root.rglob("*.csv") if path not in versioned_files
    ]
    ordered_files = [*versioned_files, *parquet_files, *fallback_csv_files]
    if not ordered_files:
        return pd.DataFrame()
    frames = [_read_bridge_table(path) for path in ordered_files]
    out = pd.concat(frames, ignore_index=True)
    dedupe_cols = [col for col in ("candidate_id", "event_type", "symbol") if col in out.columns]
    if dedupe_cols:
        out = out.drop_duplicates(subset=dedupe_cols, keep="first").reset_index(drop=True)
    return out


def _merge_bridge_metrics(phase2_df: pd.DataFrame, bridge_df: pd.DataFrame) -> pd.DataFrame:
    if bridge_df.empty:
        return phase2_df
    out = pd.merge(
        phase2_df,
        bridge_df[
            [
                "candidate_id",
                "event_type",
                "gate_bridge_tradable",
                "bridge_validation_after_cost_bps",
            ]
        ],
        on=["candidate_id", "event_type"],
        how="left",
        suffixes=("", "_bridge"),
    )
    if "gate_bridge_tradable_bridge" in out.columns:
        out["gate_bridge_tradable"] = out["gate_bridge_tradable_bridge"].combine_first(
            out["gate_bridge_tradable"]
        )
        out = out.drop(columns=["gate_bridge_tradable_bridge"])
    return out


def _parse_run_symbols(raw_symbols: Any) -> List[str]:
    if isinstance(raw_symbols, (list, tuple, set)):
        values = raw_symbols
    else:
        values = str(raw_symbols or "").split(",")
    ordered: List[str] = []
    seen: set[str] = set()
    for value in values:
        symbol = str(value).strip().upper()
        if not symbol or symbol in seen:
            continue
        ordered.append(symbol)
        seen.add(symbol)
    return ordered


def _hydrate_edge_candidates_from_phase2(
    *,
    run_id: str,
    run_symbols: List[str],
    source_run_mode: str,
    data_root: Path,
) -> pd.DataFrame:
    if not run_symbols:
        return pd.DataFrame()
    export_module = importlib.import_module("project.research.export_edge_candidates")
    rows = export_module._collect_phase2_candidates(run_id, run_symbols=run_symbols)
    candidates_df = pd.DataFrame(rows)
    if candidates_df.empty:
        return candidates_df

    from project.research.helpers.shrinkage import _apply_hierarchical_shrinkage

    candidates_df = _apply_hierarchical_shrinkage(
        candidates_df,
        train_only_lambda=True,
        split_col="split_label",
        run_mode=source_run_mode,
    )
    is_confirmatory = bool(export_module._is_confirmatory_run_mode(source_run_mode))
    current_spec_hash = ontology_spec_hash(PROJECT_ROOT.parent)
    candidates_df = export_module._normalize_edge_candidates_df(
        candidates_df,
        run_mode=source_run_mode,
        is_confirmatory=is_confirmatory,
        current_spec_hash=current_spec_hash,
    )

    out_dir = data_root / "reports" / "edge_candidates" / run_id
    ensure_dir(out_dir)
    write_parquet(candidates_df, out_dir / "edge_candidates_normalized.parquet")
    (out_dir / "edge_candidates_normalized.json").write_text(
        candidates_df.to_json(orient="records", indent=2),
        encoding="utf-8",
    )
    return candidates_df


def _load_negative_control_summary(run_id: str) -> Dict[str, Any]:
    data_root = get_data_root()
    path = data_root / "reports" / "negative_control" / run_id / "negative_control_summary.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}


def _load_dynamic_min_events_by_event(spec_root: str | Path) -> Dict[str, int]:
    path = Path(spec_root) / "spec" / "states" / "state_registry.yaml"
    if not path.exists():
        return {}
    try:
        import yaml

        with path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle) or {}
    except (ImportError, OSError, UnicodeDecodeError):
        logging.warning("Failed loading state_registry")
        return {}
    except yaml.YAMLError:
        logging.warning("Failed loading state_registry")
        return {}

    out: Dict[str, int] = {}
    default_min = data.get("defaults", {}).get("min_events", 0)
    for state_row in data.get("states", []):
        event_type = state_row.get("source_event_type")
        if event_type:
            out[event_type] = max(
                out.get(event_type, default_min), state_row.get("min_events", default_min)
            )
    return out


def _resolve_promotion_profile(configured_profile: str, source_run_mode: str) -> str:
    profile = str(configured_profile or "auto").strip().lower()
    if profile in {"research", "deploy"}:
        return profile
    if source_run_mode in {"confirmatory", "production", "certification", "promotion", "deploy"}:
        return "deploy"
    return "research"


def _resolve_promotion_policy(
    *,
    config: PromotionConfig,
    contract: Any,
    source_run_mode: str,
    project_root: Path,
) -> ResolvedPromotionPolicy:
    profile = _resolve_promotion_profile(config.promotion_profile, source_run_mode)
    base_min_events = int(config.min_events)
    dynamic_min_events: Dict[str, int] = {}

    min_net_expectancy_bps = float(
        max(0.0, float(getattr(contract, "min_net_expectancy_bps", 0.0) or 0.0))
    )
    max_fee_plus_slippage_bps = getattr(contract, "max_fee_plus_slippage_bps", None)
    max_daily_turnover_multiple = getattr(contract, "max_daily_turnover_multiple", None)
    require_retail_viability = bool(getattr(contract, "require_retail_viability", False))
    require_low_capital_viability = bool(getattr(contract, "require_low_capital_contract", False))
    enforce_baseline_beats_complexity = True
    enforce_placebo_controls = True
    enforce_timeframe_consensus = True

    if profile == "deploy":
        base_min_events = max(
            base_min_events,
            int(getattr(contract, "min_trade_count", base_min_events) or base_min_events),
        )
        dynamic_min_events = _load_dynamic_min_events_by_event(project_root)
    else:
        min_net_expectancy_bps = min(min_net_expectancy_bps, 1.5)
        require_retail_viability = False
        require_low_capital_viability = False
        enforce_baseline_beats_complexity = False
        enforce_placebo_controls = False
        enforce_timeframe_consensus = False

    use_effective_q_value = profile == "deploy"

    return ResolvedPromotionPolicy(
        promotion_profile=profile,
        base_min_events=base_min_events,
        dynamic_min_events=dynamic_min_events,
        min_net_expectancy_bps=min_net_expectancy_bps,
        max_fee_plus_slippage_bps=max_fee_plus_slippage_bps,
        max_daily_turnover_multiple=max_daily_turnover_multiple,
        require_retail_viability=require_retail_viability,
        require_low_capital_viability=require_low_capital_viability,
        enforce_baseline_beats_complexity=enforce_baseline_beats_complexity,
        enforce_placebo_controls=enforce_placebo_controls,
        enforce_timeframe_consensus=enforce_timeframe_consensus,
        use_effective_q_value=use_effective_q_value,
    )




def _write_promotion_lineage_audit(
    *,
    out_dir: Path,
    run_id: str,
    evidence_bundles: list[dict[str, Any]],
    promoted_df: pd.DataFrame,
    live_export_diagnostics: Mapping[str, Any] | None = None,
    historical_trust: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    rows: list[dict[str, Any]] = []
    promoted_ids = {
        str(row.get("candidate_id", "")).strip()
        for row in promoted_df.to_dict(orient="records")
        if str(row.get("candidate_id", "")).strip()
    }
    for bundle in evidence_bundles:
        candidate_id = str(bundle.get("candidate_id", "")).strip()
        decision = bundle.get("promotion_decision", {}) if isinstance(bundle.get("promotion_decision", {}), dict) else {}
        metadata = bundle.get("metadata", {}) if isinstance(bundle.get("metadata", {}), dict) else {}
        search_burden = bundle.get("search_burden", {}) if isinstance(bundle.get("search_burden", {}), dict) else {}
        rows.append({
            "run_id": run_id,
            "candidate_id": candidate_id,
            "event_type": str(bundle.get("event_type", "")).strip(),
            "promotion_status": str(decision.get("promotion_status", "")).strip(),
            "promotion_track": str(decision.get("promotion_track", "")).strip(),
            "bundle_version": str(bundle.get("bundle_version", "")).strip(),
            "policy_version": str(bundle.get("policy_version", "")).strip(),
            "hypothesis_id": str(metadata.get("hypothesis_id", "")).strip(),
            "plan_row_id": str(metadata.get("plan_row_id", "")).strip(),
            "program_id": str(metadata.get("program_id", "")).strip(),
            "campaign_id": str(metadata.get("campaign_id", "")).strip(),
            "live_exported": candidate_id in promoted_ids,
            "search_candidates_generated": safe_int(search_burden.get("search_candidates_generated", 0), 0),
            "search_candidates_eligible": safe_int(search_burden.get("search_candidates_eligible", 0), 0),
            "search_mutations_attempted": safe_int(search_burden.get("search_mutations_attempted", 0), 0),
            "search_family_count": safe_int(search_burden.get("search_family_count", 0), 0),
            "search_lineage_count": safe_int(search_burden.get("search_lineage_count", 0), 0),
            "search_burden_estimated": bool(as_bool(search_burden.get("search_burden_estimated", False))),
            "search_scope_version": str(search_burden.get("search_scope_version", "phase1_v1")),
        })
    json_path = out_dir / "promotion_lineage_audit.json"
    md_path = out_dir / "promotion_lineage_audit.md"
    payload = {
        "schema_version": "promotion_lineage_audit_v1",
        "run_id": run_id,
        "rows": rows,
        "live_export": dict(live_export_diagnostics or {}),
        "historical_trust": dict(historical_trust or {}),
    }
    atomic_write_json(json_path, payload)
    md_lines = [
        "# Promotion lineage audit",
        "",
        f"- run_id: `{run_id}`",
        f"- evidence_bundle_count: `{len(evidence_bundles)}`",
        f"- live_exported_count: `{sum(1 for row in rows if row['live_exported'])}`",
        f"- live_thesis_store: `{str((live_export_diagnostics or {}).get('output_path', ''))}`",
        f"- live_contract_json: `{str((live_export_diagnostics or {}).get('contract_json_path', ''))}`",
        f"- live_contract_md: `{str((live_export_diagnostics or {}).get('contract_md_path', ''))}`",
        f"- historical_trust_status: `{str((historical_trust or {}).get('historical_trust_status', ''))}`",
        f"- canonical_reuse_allowed: `{bool((historical_trust or {}).get('canonical_reuse_allowed', False))}`",
        f"- compat_reuse_allowed: `{bool((historical_trust or {}).get('compat_reuse_allowed', False))}`",
        "",
        "| candidate_id | event_type | promotion_status | promotion_track | program_id | campaign_id | live_exported |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        md_lines.append(
            "| {candidate_id} | {event_type} | {promotion_status} | {promotion_track} | {program_id} | {campaign_id} | {live_exported} |".format(**row)
        )
    atomic_write_text(md_path, "\n".join(md_lines) + "\n")
    return {"json_path": str(json_path), "md_path": str(md_path)}


def _write_multiplicity_scope_diagnostics(out_dir: Path, diag: Dict[str, Any]) -> Dict[str, str]:
    """Write multiplicity scope diagnostics as JSON and Markdown."""
    json_path = out_dir / "multiplicity_scope_diagnostics.json"
    md_path = out_dir / "multiplicity_scope_diagnostics.md"

    atomic_write_json(json_path, diag)

    md_lines = [
        "# Multiplicity Scope Diagnostics",
        "",
        f"- scope_mode: `{diag.get('scope_mode', 'unknown')}`",
        f"- scope_version: `{diag.get('scope_version', 'unknown')}`",
        f"- program_id: `{diag.get('program_id', 'unknown')}`",
        f"- campaign_id: `{diag.get('campaign_id', 'none')}`",
        "",
        "## Candidate Counts",
        f"- current_candidates_total: `{diag.get('current_candidates_total', 0)}`",
        f"- historical_candidates_total: `{diag.get('historical_candidates_total', 0)}`",
        f"- combined_candidates_total: `{diag.get('combined_candidates_total', 0)}`",
        f"- scope_keys_unique: `{diag.get('scope_keys_unique', 0)}`",
        "",
        "## Multiplicity Statistics",
        f"- num_tests_scope_avg: `{diag.get('num_tests_scope_avg', 0.0):.2f}`",
        f"- effective_q_value_avg: `{diag.get('effective_q_value_avg', 0.0):.4f}`",
        "",
        "## Degradation Status",
        f"- scope_degraded_count: `{diag.get('scope_degraded_count', 0)}`",
    ]

    # Add scope context counts if available
    context_counts = diag.get('scope_context_counts', {})
    if context_counts:
        md_lines.extend(["", "## Context Breakdown"])
        for context, count in sorted(context_counts.items()):
            md_lines.append(f"- {context}: `{count}`")

    # Add degraded reason counts if available
    degraded_reasons = diag.get('scope_degraded_reason_counts', {})
    if degraded_reasons:
        md_lines.extend(["", "## Degraded Reasons"])
        for reason, count in sorted(degraded_reasons.items()):
            md_lines.append(f"- {reason}: `{count}`")

    md_lines.append("")
    atomic_write_text(md_path, "\n".join(md_lines) + "\n")

    return {"json_path": str(json_path), "md_path": str(md_path)}


REQUIRED_PROMOTION_FIELDS = frozenset({
        "candidate_id",
        "family",
        "event_type",
        "net_expectancy_bps",
        "stability_score",
        "sign_consistency",
        "cost_survival_ratio",
        "q_value",
        "n_events",
    })


def _missing_or_blank_mask(series: pd.Series) -> pd.Series:
    mask = series.isna()
    if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
        normalized = series.astype(str).str.strip().str.lower()
        mask = mask | normalized.isin({"", "nan", "none", "null", "<na>"})
    return mask


def _coalesce_column(out: pd.DataFrame, target: str, sources: list[str]) -> None:
    if target not in out.columns:
        out[target] = pd.NA
    target_missing = _missing_or_blank_mask(out[target])
    for source in sources:
        if source not in out.columns:
            continue
        source_values = out[source]
        source_present = ~_missing_or_blank_mask(source_values)
        fill_mask = target_missing & source_present
        if bool(fill_mask.any()):
            out.loc[fill_mask, target] = source_values.loc[fill_mask]
            target_missing = _missing_or_blank_mask(out[target])
        if not bool(target_missing.any()):
            break


def _fill_numeric_column_from_scaled_sources(
    out: pd.DataFrame,
    target: str,
    sources: list[tuple[str, float]],
) -> None:
    if target not in out.columns:
        out[target] = pd.NA
    target_numeric = pd.to_numeric(out[target], errors="coerce")
    target_missing = target_numeric.isna()
    for source, scale in sources:
        if source not in out.columns:
            continue
        source_numeric = pd.to_numeric(out[source], errors="coerce") * float(scale)
        fill_mask = target_missing & source_numeric.notna()
        if bool(fill_mask.any()):
            out.loc[fill_mask, target] = source_numeric.loc[fill_mask]
            target_numeric = pd.to_numeric(out[target], errors="coerce")
            target_missing = target_numeric.isna()
        if not bool(target_missing.any()):
            break


def _derive_cost_survival_ratio_from_bridge_flags(out: pd.DataFrame) -> pd.Series:
    scenario_keys = [
        "gate_after_cost_positive",
        "gate_after_cost_stressed_positive",
        "gate_bridge_after_cost_positive_validation",
        "gate_bridge_after_cost_stressed_positive_validation",
    ]
    present = pd.Series(0, index=out.index, dtype="int64")
    passed = pd.Series(0, index=out.index, dtype="int64")
    for key in scenario_keys:
        if key not in out.columns:
            continue
        values = out[key]
        key_present = ~_missing_or_blank_mask(values)
        normalized = values.astype(str).str.strip().str.lower()
        key_passed = key_present & (
            values.eq(True) | normalized.isin({"pass", "true", "1", "passed"})
        )
        present = present + key_present.astype("int64")
        passed = passed + key_passed.astype("int64")
    return passed.where(present > 0).astype("float64") / present.where(present > 0)


def _hydrate_canonical_promotion_aliases(candidates_df: pd.DataFrame) -> pd.DataFrame:
    """Map current validated edge-candidate columns onto canonical promotion inputs."""
    if candidates_df.empty:
        return candidates_df.copy()

    out = candidates_df.copy()
    _coalesce_column(
        out,
        "family",
        ["family_id", "event_family", "research_family", "canonical_family"],
    )
    _fill_numeric_column_from_scaled_sources(
        out,
        "net_expectancy_bps",
        [
            ("bridge_validation_stressed_after_cost_bps", 1.0),
            ("bridge_validation_after_cost_bps", 1.0),
            ("stressed_after_cost_expectancy_bps", 1.0),
            ("after_cost_expectancy_bps", 1.0),
            ("stressed_after_cost_expectancy_per_trade", 10_000.0),
            ("after_cost_expectancy_per_trade", 10_000.0),
        ],
    )

    if "cost_survival_ratio" not in out.columns:
        out["cost_survival_ratio"] = pd.NA
    cost_missing = pd.to_numeric(out["cost_survival_ratio"], errors="coerce").isna()
    if bool(cost_missing.any()):
        derived = _derive_cost_survival_ratio_from_bridge_flags(out)
        fill_mask = cost_missing & derived.notna()
        if bool(fill_mask.any()):
            out.loc[fill_mask, "cost_survival_ratio"] = derived.loc[fill_mask]
    return out


def _diagnose_missing_fields(df: pd.DataFrame) -> list[str]:
    """Return list of missing required fields for promotion."""
    if df.empty:
        return []
    missing = []
    for field in REQUIRED_PROMOTION_FIELDS:
        if field not in df.columns:
            missing.append(field)
        elif df[field].isna().all():
            missing.append(f"{field} (all null)")
    return missing


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
        from project.research.services.evaluation_service import ValidationService
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
            out_dir.parent.parent / "validation" / config.run_id / "promotion_ready_candidates.parquet"
        )
        canonical_candidate_csv_path = canonical_candidate_path.with_suffix(".csv")
        if (
            not canonical_candidate_path.exists()
            and not canonical_candidate_csv_path.exists()
        ):
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
        run_symbols = _parse_run_symbols(config.symbols or source_manifest.get("symbols"))
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
            source_candidates_df = pd.DataFrame()
            for source in ("edge_candidates", "promotion_audit", "phase2_candidates"):
                if not source_tables[source].empty:
                    source_candidates_df = source_tables[source]
                    break
            
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
                        candidates_df[col] = validation_meta_df.set_index("candidate_id").reindex(
                            candidates_df["candidate_id"].astype(str)
                        )[col].values
                
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
                        field
                        for field in missing_before_hydration
                        if field not in missing_fields
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
                    config.run_id
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
            multiplicity_scope_mode=getattr(resolved_policy, 'multiplicity_scope_mode', 'campaign_lineage'),
            require_scope_level_multiplicity=getattr(resolved_policy, 'require_scope_level_multiplicity', True),
            allow_multiplicity_scope_degraded=getattr(resolved_policy, 'allow_multiplicity_scope_degraded', True),
            use_effective_q_value=getattr(resolved_policy, 'use_effective_q_value', True),
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
                        invalid_promoted_rows.append(
                            f"{candidate_id or '<unknown>'}: {exc}"
                        )
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
            "contract_json_path": str(thesis_export.contract_json_path) if thesis_export.contract_json_path else "",
            "contract_md_path": str(thesis_export.contract_md_path) if thesis_export.contract_md_path else "",
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
                }
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
