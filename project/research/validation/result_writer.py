from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from project.contracts.schemas import (
    normalize_dataframe_for_schema,
    validate_dataframe_for_schema,
    validate_payload_for_schema,
    validate_schema_at_producer,
)
from project.core.config import get_data_root
from project.core.exceptions import (
    CompatibilityRequiredError,
    DataIntegrityError,
    MalformedArtifactError,
    MissingArtifactError,
    SchemaMismatchError,
)
from project.io.utils import atomic_write_json, write_parquet
from project.research.contracts.historical_trust import (
    HISTORICAL_TRUST_LEGACY,
    HISTORICAL_TRUST_REQUIRES_REVALIDATION,
)
from project.research.historical_trust import inspect_artifact_trust
from project.research.validation.contracts import (
    ValidatedCandidateRecord,
    ValidationArtifactRef,
    ValidationBundle,
    ValidationDecision,
    ValidationMetrics,
)

PROMOTION_READY_COLUMNS = (
    "candidate_id",
    "anchor_summary",
    "template_id",
    "direction",
    "horizon_bars",
    "validation_stage_version",
    "validation_status",
    "validation_run_id",
    "validation_program_id",
    "validation_reason_codes",
    "metric_sample_count",
    "metric_effective_sample_size",
    "metric_expectancy",
    "metric_net_expectancy",
    "metric_hit_rate",
    "metric_p_value",
    "metric_q_value",
    "metric_stability_score",
    "metric_cost_sensitivity",
    "metric_turnover",
    "metric_regime_support_score",
    "metric_time_slice_support_score",
    "metric_negative_control_score",
    "metric_max_drawdown",
    "source_event_name",
    "source_event_version",
    "source_detector_class",
    "source_evidence_mode",
    "source_threshold_version",
    "source_calibration_artifact",
)


def _validate_mapping_payload(payload: Any, *, artifact_name: str) -> None:
    if not isinstance(payload, dict):
        raise SchemaMismatchError(f"{artifact_name} must be a JSON object payload")


def _validate_validation_bundle_payload(payload: Any) -> None:
    validate_payload_for_schema(payload, "validation_bundle")
    _validate_mapping_payload(payload, artifact_name="validation_bundle.json")
    required = {
        "run_id": str,
        "created_at": str,
        "validated_candidates": list,
        "rejected_candidates": list,
        "inconclusive_candidates": list,
        "summary_stats": dict,
        "effect_stability_report": dict,
    }
    for field_name, field_type in required.items():
        if field_name not in payload:
            raise SchemaMismatchError(
                f"validation_bundle.json missing required field {field_name!r}"
            )
        if not isinstance(payload[field_name], field_type):
            raise SchemaMismatchError(
                f"validation_bundle.json field {field_name!r} must be {field_type.__name__}"
            )
    if not str(payload.get("run_id", "")).strip():
        raise SchemaMismatchError("validation_bundle.json run_id must be non-empty")
    if not str(payload.get("created_at", "")).strip():
        raise SchemaMismatchError("validation_bundle.json created_at must be non-empty")


def write_validation_bundle(bundle: ValidationBundle, base_dir: Optional[Path] = None) -> Path:
    if base_dir is None:
        base_dir = get_data_root() / "reports" / "validation" / bundle.run_id

    base_dir.mkdir(parents=True, exist_ok=True)

    bundle_path = base_dir / "validation_bundle.json"
    atomic_write_json(
        bundle_path,
        bundle.to_dict(),
        validator=_validate_validation_bundle_payload,
    )

    # Canonical: validation_report.json
    summary_path = base_dir / "validation_report.json"
    atomic_write_json(
        summary_path,
        bundle.summary_stats,
        validator=lambda payload: _validate_mapping_payload(
            payload, artifact_name="validation_report.json"
        ),
    )

    # Canonical: effect_stability_report.json
    stability_path = base_dir / "effect_stability_report.json"
    atomic_write_json(
        stability_path,
        bundle.effect_stability_report,
        validator=lambda payload: _validate_mapping_payload(
            payload, artifact_name="effect_stability_report.json"
        ),
    )

    return bundle_path


def write_validated_candidate_tables(bundle: ValidationBundle, base_dir: Optional[Path] = None) -> Dict[str, Path]:
    if base_dir is None:
        base_dir = get_data_root() / "reports" / "validation" / bundle.run_id

    base_dir.mkdir(parents=True, exist_ok=True)

    paths = {}

    # Canonical Groups
    # 1. validated_candidates.parquet
    # 2. rejection_reasons.parquet (rejected + inconclusive)

    groups = {
        "validated_candidates": bundle.validated_candidates,
        "rejection_reasons": bundle.rejected_candidates + bundle.inconclusive_candidates,
    }

    for name, candidates in groups.items():
        if not candidates:
            # Still write an empty file if it's the canonical name?
            # Usually better to have the file exist.
            flat_df = pd.DataFrame()
        else:
            flat_data = []
            for c in candidates:
                row = {
                    "candidate_id": c.candidate_id,
                    "anchor_summary": c.anchor_summary,
                    "template_id": c.template_id,
                    "direction": c.direction,
                    "horizon_bars": c.horizon_bars,
                    "validation_stage_version": c.validation_stage_version,
                    "status": c.decision.status,
                    "run_id": c.decision.run_id,
                    "program_id": c.decision.program_id,
                    "reason_codes": "|".join(c.decision.reason_codes),
                }
                # Add metrics
                metrics_dict = c.metrics.to_dict()
                for k, v in metrics_dict.items():
                    row[f"metric_{k}"] = v
                for k, v in c.detector_lineage.items():
                    row[k] = v
                flat_data.append(row)
            flat_df = pd.DataFrame(flat_data)

        path = base_dir / f"{name}.parquet"
        write_parquet(flat_df, path)
        paths[name] = path

    # Keep the promotion handoff canonical whenever validation tables are written.
    paths["promotion_ready_candidates"] = write_promotion_ready_candidates(
        bundle,
        base_dir=base_dir,
    )

    return paths


def write_promotion_ready_candidates(bundle: ValidationBundle, base_dir: Optional[Path] = None) -> Optional[Path]:
    if base_dir is None:
        base_dir = get_data_root() / "reports" / "validation" / bundle.run_id

    base_dir.mkdir(parents=True, exist_ok=True)

    flat_data = []
    for c in bundle.validated_candidates:
        row = {
            "candidate_id": c.candidate_id,
            "anchor_summary": c.anchor_summary,
            "template_id": c.template_id,
            "direction": c.direction,
            "horizon_bars": c.horizon_bars,
            "validation_stage_version": c.validation_stage_version,
            "validation_status": c.decision.status,
            "validation_run_id": c.decision.run_id,
            "validation_program_id": c.decision.program_id,
            "validation_reason_codes": "|".join(c.decision.reason_codes),
        }
        metrics_dict = c.metrics.to_dict()
        for k, v in metrics_dict.items():
            row[f"metric_{k}"] = v
        for k, v in c.detector_lineage.items():
            row[k] = v
        flat_data.append(row)

    flat_df = pd.DataFrame(flat_data, columns=PROMOTION_READY_COLUMNS)
    flat_df = normalize_dataframe_for_schema(flat_df, "promotion_ready_candidates")
    if not flat_df.empty:
        if "validation_program_id" in flat_df.columns:
            flat_df["validation_program_id"] = (
                flat_df["validation_program_id"].fillna("").astype(str)
            )
        if "metric_net_expectancy" in flat_df.columns:
            fallback = pd.to_numeric(
                flat_df.get("metric_expectancy", pd.Series(0.0, index=flat_df.index)),
                errors="coerce",
            ).fillna(0.0)
            metric_net_expectancy = pd.to_numeric(
                flat_df["metric_net_expectancy"],
                errors="coerce",
            )
            flat_df["metric_net_expectancy"] = metric_net_expectancy.where(
                ~metric_net_expectancy.isna(),
                fallback,
            ).fillna(0.0)
        if "metric_q_value" in flat_df.columns:
            flat_df["metric_q_value"] = pd.to_numeric(
                flat_df["metric_q_value"],
                errors="coerce",
            ).fillna(1.0)
        if "metric_stability_score" in flat_df.columns:
            flat_df["metric_stability_score"] = pd.to_numeric(
                flat_df["metric_stability_score"],
                errors="coerce",
            ).fillna(0.0)
    validate_schema_at_producer(
        flat_df,
        "promotion_ready_candidates",
        context="write_promotion_ready_candidates",
    )
    flat_df = validate_dataframe_for_schema(
        flat_df,
        "promotion_ready_candidates",
        allow_empty=True,
    )
    path = base_dir / "promotion_ready_candidates.parquet"
    write_parquet(flat_df, path)
    return path


def load_validation_bundle(
    run_id: str,
    base_dir: Optional[Path] = None,
    *,
    strict: bool = False,
    compatibility_mode: bool = False,
) -> Optional[ValidationBundle]:
    if base_dir is None:
        resolved_dir = get_data_root() / "reports" / "validation" / run_id
    else:
        resolved_dir = Path(base_dir)
        if not (resolved_dir / "validation_bundle.json").exists():
            candidate_dir = resolved_dir / run_id
            if (candidate_dir / "validation_bundle.json").exists():
                resolved_dir = candidate_dir

    bundle_path = resolved_dir / "validation_bundle.json"
    if not bundle_path.exists():
        if strict and not compatibility_mode:
            raise MissingArtifactError(
                f"Missing required validation artifact {bundle_path}. "
                "Run canonical validation before reusing this run."
            )
        return None

    try:
        with bundle_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MalformedArtifactError(
            f"Failed to read validation bundle {bundle_path}: {exc}"
        ) from exc
    _validate_validation_bundle_payload(data)
    if strict and not compatibility_mode:
        trust = inspect_artifact_trust("validation_bundle", bundle_path)
        if trust.historical_trust_status == HISTORICAL_TRUST_LEGACY:
            raise CompatibilityRequiredError(
                f"Validation artifact {bundle_path} is legacy_but_interpretable and cannot be reused on the canonical path"
            )
        if trust.historical_trust_status == HISTORICAL_TRUST_REQUIRES_REVALIDATION:
            raise DataIntegrityError(
                f"Validation artifact {bundle_path} requires revalidation before reuse on the canonical path"
            )

    def _parse_candidate(c_data: Dict[str, Any]) -> ValidatedCandidateRecord:
        decision = ValidationDecision(**c_data["decision"])
        metrics = ValidationMetrics(**c_data["metrics"])
        artifacts = [ValidationArtifactRef(**a) for a in c_data.get("artifact_refs", [])]

        return ValidatedCandidateRecord(
            candidate_id=c_data["candidate_id"],
            decision=decision,
            metrics=metrics,
            anchor_summary=c_data.get("anchor_summary", ""),
            template_id=c_data.get("template_id", ""),
            direction=c_data.get("direction", ""),
            horizon_bars=c_data.get("horizon_bars", 0),
            artifact_refs=artifacts,
            validation_stage_version=c_data.get("validation_stage_version", "v1"),
        )

    # Load effect_stability_report with backward-compat fallback
    effect_stability_report = data.get("effect_stability_report", {})
    if not effect_stability_report and compatibility_mode:
        stability_path = resolved_dir / "effect_stability_report.json"
        if stability_path.exists():
            try:
                with stability_path.open("r", encoding="utf-8") as f:
                    effect_stability_report = json.load(f)
            except Exception:
                # Legacy fallback failed, keep empty but note for maintenance
                effect_stability_report = {}

    return ValidationBundle(
        run_id=data["run_id"],
        created_at=data["created_at"],
        program_id=data.get("program_id"),
        validated_candidates=[_parse_candidate(c) for c in data.get("validated_candidates", [])],
        rejected_candidates=[_parse_candidate(c) for c in data.get("rejected_candidates", [])],
        inconclusive_candidates=[_parse_candidate(c) for c in data.get("inconclusive_candidates", [])],
        summary_stats=data.get("summary_stats", {}),
        effect_stability_report=effect_stability_report,
    )
