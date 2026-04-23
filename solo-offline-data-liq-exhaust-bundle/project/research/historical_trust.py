from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from project.contracts.schemas import validate_dataframe_for_schema
from project.core.exceptions import DataIntegrityError
from project.io.utils import read_parquet
from project.live.contracts import PromotedThesis
from project.research.contracts.historical_trust import (
    HistoricalTrustStamp,
    legacy_but_interpretable,
    requires_revalidation,
    trusted_under_current_rules,
)


CURRENT_ARTIFACT_FILENAMES: dict[str, set[str]] = {
    "run_manifest": {"run_manifest.json"},
    "validation_bundle": {"validation_bundle.json"},
    "promotion_ready_candidates": {"promotion_ready_candidates.parquet", "promotion_ready_candidates.csv"},
    "promotion_audit": {"promotion_audit.parquet", "promotion_audit.csv"},
    "promoted_candidates": {"promoted_candidates.parquet", "promoted_candidates.csv"},
    "evidence_bundle_summary": {"evidence_bundle_summary.parquet", "evidence_bundle_summary.csv"},
    "promotion_lineage_audit": {"promotion_lineage_audit.json"},
    "promoted_theses": {"promoted_theses.json"},
    "live_thesis_index": {"index.json"},
}

LEGACY_ARTIFACT_FILENAMES: dict[str, set[str]] = {
    "run_manifest": {"manifest.json"},
    "promotion_audit": {"promotion_statistical_audit.parquet", "promotion_statistical_audit.csv"},
}

_PROMOTION_SCHEMA_BY_TYPE = {
    "promotion_audit": "promotion_audit",
    "promoted_candidates": "promoted_candidates",
    "evidence_bundle_summary": "evidence_bundle_summary",
}

_PROMOTION_READY_REQUIRED_COLUMNS = {
    "candidate_id",
    "validation_status",
    "validation_run_id",
    "validation_program_id",
    "metric_sample_count",
    "metric_q_value",
    "metric_stability_score",
    "metric_net_expectancy",
}


def artifact_uses_legacy_filename(artifact_type: str, artifact_path: Path) -> bool:
    name = str(Path(artifact_path).name).strip().lower()
    return name in {value.lower() for value in LEGACY_ARTIFACT_FILENAMES.get(str(artifact_type), set())}


def inspect_artifact_trust(artifact_type: str, artifact_path: Path) -> HistoricalTrustStamp:
    path = Path(artifact_path)
    if not path.exists():
        return requires_revalidation("artifact_missing")
    inspectors = {
        "run_manifest": _inspect_run_manifest,
        "validation_bundle": _inspect_validation_bundle,
        "promotion_ready_candidates": _inspect_promotion_ready_candidates,
        "promotion_audit": _inspect_promotion_dataframe,
        "promoted_candidates": _inspect_promotion_dataframe,
        "evidence_bundle_summary": _inspect_promotion_dataframe,
        "promotion_lineage_audit": _inspect_promotion_lineage_audit,
        "promoted_theses": _inspect_promoted_theses,
        "live_thesis_index": _inspect_live_thesis_index,
    }
    inspector = inspectors.get(str(artifact_type))
    if inspector is None:
        return requires_revalidation(f"unsupported_artifact_type:{artifact_type}")
    return inspector(path, artifact_type=str(artifact_type))


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise DataIntegrityError(f"{path} did not contain a JSON object payload")
    return payload


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    raise DataIntegrityError(f"Unsupported tabular artifact suffix for {path}")


def _is_json_parse_failure(exc: Exception) -> bool:
    return isinstance(exc, (json.JSONDecodeError, OSError, UnicodeDecodeError, TypeError, ValueError))


def _inspect_run_manifest(path: Path, *, artifact_type: str) -> HistoricalTrustStamp:
    try:
        payload = _read_json_object(path)
    except Exception as exc:
        if _is_json_parse_failure(exc):
            return requires_revalidation("run_manifest_malformed_json")
        return requires_revalidation("run_manifest_unreadable")
    if artifact_uses_legacy_filename(artifact_type, path):
        return legacy_but_interpretable("legacy_manifest_filename")
    if str(payload.get("run_id", "")).strip():
        return trusted_under_current_rules("current_run_manifest")
    return legacy_but_interpretable("run_manifest_missing_run_id")


def _inspect_validation_bundle(path: Path, *, artifact_type: str) -> HistoricalTrustStamp:
    try:
        payload = _read_json_object(path)
    except Exception as exc:
        if _is_json_parse_failure(exc):
            return requires_revalidation("validation_bundle_malformed_json")
        return requires_revalidation("validation_bundle_unreadable")
    required_fields = {
        "run_id",
        "created_at",
        "validated_candidates",
        "rejected_candidates",
        "inconclusive_candidates",
        "summary_stats",
    }
    if not required_fields.issubset(payload.keys()):
        return legacy_but_interpretable("validation_bundle_missing_current_fields")
    if not isinstance(payload.get("validated_candidates"), list):
        return requires_revalidation("validation_bundle_validated_candidates_malformed")
    companion = _promotion_ready_candidate_path(path.parent)
    if companion is None:
        return legacy_but_interpretable("missing_promotion_ready_candidates")
    companion_trust = inspect_artifact_trust("promotion_ready_candidates", companion)
    if companion_trust.historical_trust_status == "requires_revalidation":
        return companion_trust
    if companion_trust.historical_trust_status != "trusted_under_current_rules":
        return legacy_but_interpretable("promotion_ready_candidates_not_current_contract")
    return trusted_under_current_rules("validation_bundle_and_companion_current")


def _promotion_ready_candidate_path(base_dir: Path) -> Path | None:
    for candidate in (
        base_dir / "promotion_ready_candidates.parquet",
        base_dir / "promotion_ready_candidates.csv",
    ):
        if candidate.exists():
            return candidate
    return None


def _inspect_promotion_ready_candidates(path: Path, *, artifact_type: str) -> HistoricalTrustStamp:
    try:
        frame = _read_table(path)
    except Exception:
        return requires_revalidation("promotion_ready_candidates_unreadable")
    missing = sorted(_PROMOTION_READY_REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        return legacy_but_interpretable(
            f"promotion_ready_candidates_missing_columns:{','.join(missing)}"
        )
    return trusted_under_current_rules("promotion_ready_candidates_current")


def _inspect_promotion_dataframe(path: Path, *, artifact_type: str) -> HistoricalTrustStamp:
    try:
        frame = _read_table(path)
    except Exception:
        return requires_revalidation(f"{artifact_type}_unreadable")
    schema_name = _PROMOTION_SCHEMA_BY_TYPE.get(str(artifact_type))
    if schema_name is None:
        return requires_revalidation(f"{artifact_type}_unsupported_schema")
    try:
        validate_dataframe_for_schema(frame, schema_name, allow_empty=True)
    except Exception:
        if artifact_uses_legacy_filename(artifact_type, path):
            return legacy_but_interpretable(f"{artifact_type}_legacy_alias")
        return legacy_but_interpretable(f"{artifact_type}_schema_not_current")
    if artifact_uses_legacy_filename(artifact_type, path):
        return legacy_but_interpretable(f"{artifact_type}_legacy_alias")
    return trusted_under_current_rules(f"{artifact_type}_current")


def _inspect_promotion_lineage_audit(path: Path, *, artifact_type: str) -> HistoricalTrustStamp:
    try:
        payload = _read_json_object(path)
    except Exception as exc:
        if _is_json_parse_failure(exc):
            return requires_revalidation("promotion_lineage_audit_malformed_json")
        return requires_revalidation("promotion_lineage_audit_unreadable")
    if payload.get("schema_version") != "promotion_lineage_audit_v1":
        return legacy_but_interpretable("promotion_lineage_audit_schema_not_current")
    if not isinstance(payload.get("rows", []), list):
        return requires_revalidation("promotion_lineage_audit_rows_malformed")
    if not isinstance(payload.get("live_export", {}), dict):
        return requires_revalidation("promotion_lineage_audit_live_export_malformed")
    return trusted_under_current_rules("promotion_lineage_audit_current")


def _inspect_promoted_theses(path: Path, *, artifact_type: str) -> HistoricalTrustStamp:
    try:
        payload = _read_json_object(path)
    except Exception as exc:
        if _is_json_parse_failure(exc):
            return requires_revalidation("promoted_theses_malformed_json")
        return requires_revalidation("promoted_theses_unreadable")
    required = {
        "schema_version",
        "run_id",
        "generated_at_utc",
        "thesis_count",
        "active_thesis_count",
        "pending_thesis_count",
        "theses",
    }
    if not required.issubset(payload.keys()):
        return legacy_but_interpretable("promoted_theses_missing_current_fields")
    theses = payload.get("theses", [])
    if not isinstance(theses, list):
        return requires_revalidation("promoted_theses_rows_malformed")
    if payload.get("schema_version") != "promoted_theses_v1":
        return legacy_but_interpretable("promoted_theses_schema_not_current")
    if int(payload.get("thesis_count", 0) or 0) != len(theses):
        return requires_revalidation("promoted_theses_count_mismatch")
    active_count = sum(
        1 for thesis in theses if isinstance(thesis, dict) and thesis.get("status") == "active"
    )
    pending_count = sum(
        1
        for thesis in theses
        if isinstance(thesis, dict) and thesis.get("status") == "pending_blueprint"
    )
    if int(payload.get("active_thesis_count", 0) or 0) != active_count:
        return requires_revalidation("promoted_theses_active_count_mismatch")
    if int(payload.get("pending_thesis_count", 0) or 0) != pending_count:
        return requires_revalidation("promoted_theses_pending_count_mismatch")
    try:
        for thesis in theses:
            PromotedThesis.model_validate(thesis)
    except Exception:
        return legacy_but_interpretable("promoted_theses_not_current_runtime_contract")
    return trusted_under_current_rules("promoted_theses_current")


def _inspect_live_thesis_index(path: Path, *, artifact_type: str) -> HistoricalTrustStamp:
    try:
        payload = _read_json_object(path)
    except Exception as exc:
        if _is_json_parse_failure(exc):
            return requires_revalidation("live_thesis_index_malformed_json")
        return requires_revalidation("live_thesis_index_unreadable")
    required = {"schema_version", "latest_run_id", "default_resolution_disabled", "runs"}
    if not required.issubset(payload.keys()):
        return legacy_but_interpretable("live_thesis_index_missing_current_fields")
    if payload.get("schema_version") != "promoted_thesis_index_v1":
        return legacy_but_interpretable("live_thesis_index_schema_not_current")
    runs = payload.get("runs")
    if not isinstance(runs, dict):
        return requires_revalidation("live_thesis_index_runs_malformed")
    latest_run_id = str(payload.get("latest_run_id", "")).strip()
    if latest_run_id and latest_run_id not in runs:
        return requires_revalidation("live_thesis_index_latest_missing")
    if latest_run_id:
        referenced = runs.get(latest_run_id, {})
        if not isinstance(referenced, dict):
            return requires_revalidation("live_thesis_index_latest_metadata_malformed")
        output_path = str(referenced.get("output_path", "")).strip()
        if output_path and not Path(output_path).exists():
            return requires_revalidation("live_thesis_index_referenced_store_missing")
    return trusted_under_current_rules("live_thesis_index_current")


__all__ = [
    "CURRENT_ARTIFACT_FILENAMES",
    "LEGACY_ARTIFACT_FILENAMES",
    "artifact_uses_legacy_filename",
    "inspect_artifact_trust",
]
