"""
Statistical regime contract for artifact auditing.

Defines canonical values for statistical-regime stamps, audit status,
and inference helpers for determining whether an artifact is pre-audit,
post-audit, or unknown provenance.

Phase 1 invariant:
    All promotion-related artifacts must carry an explicit stat_regime
    and audit_status stamp. Historical artifacts must be audited and
    stamped retrospectively.

See:
    - docs/92_assurance_and_benchmarks.md for status
    - project/research/audit_historical_artifacts.py for scanning
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

STAT_REGIME_PRE_AUDIT = "pre_audit_stat_regime"
STAT_REGIME_POST_AUDIT = "post_audit_stat_regime"
STAT_REGIME_UNKNOWN = "unknown_stat_regime"

CANONICAL_STAT_REGIMES = {
    STAT_REGIME_PRE_AUDIT,
    STAT_REGIME_POST_AUDIT,
    STAT_REGIME_UNKNOWN,
}

AUDIT_STATUS_CURRENT = "current"
AUDIT_STATUS_DEGRADED = "degraded"
AUDIT_STATUS_MANUAL_REVIEW_REQUIRED = "manual_review_required"
AUDIT_STATUS_LEGACY = "legacy"
AUDIT_STATUS_UNKNOWN = "unknown"

CANONICAL_AUDIT_STATUSES = {
    AUDIT_STATUS_CURRENT,
    AUDIT_STATUS_DEGRADED,
    AUDIT_STATUS_MANUAL_REVIEW_REQUIRED,
    AUDIT_STATUS_LEGACY,
    AUDIT_STATUS_UNKNOWN,
}

ARTIFACT_AUDIT_VERSION_PHASE1_V1 = "phase1_v1"

CANONICAL_ARTIFACT_AUDIT_VERSIONS = {
    ARTIFACT_AUDIT_VERSION_PHASE1_V1,
}

PHASE1_AUDIT_BOUNDARY_ISO = "2025-01-01T00:00:00Z"

PHASE1_REQUIRED_FIELDS = {
    "q_value",
    "q_value_scope",
    "effective_q_value",
    "num_tests_scope",
    "multiplicity_scope_mode",
    "multiplicity_scope_version",
}

SEARCH_BURDEN_REQUIRED_FIELDS = {
    "search_candidates_generated",
    "search_candidates_eligible",
    "search_family_count",
    "search_lineage_count",
    "search_burden_estimated",
    "search_scope_version",
}


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def _get_phase1_audit_boundary() -> datetime:
    return _parse_iso_datetime(PHASE1_AUDIT_BOUNDARY_ISO) or datetime(2025, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class ArtifactAuditStamp:
    stat_regime: str
    audit_status: str
    artifact_audit_version: str
    audit_reason: str
    requires_repromotion: bool
    requires_manual_review: bool
    inference_confidence: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stat_regime": self.stat_regime,
            "audit_status": self.audit_status,
            "artifact_audit_version": self.artifact_audit_version,
            "audit_reason": self.audit_reason,
            "requires_repromotion": self.requires_repromotion,
            "requires_manual_review": self.requires_manual_review,
            "inference_confidence": self.inference_confidence,
        }


def infer_stat_regime_from_artifact_metadata(
    row: Dict[str, Any],
    *,
    artifact_timestamp: Optional[datetime] = None,
    audit_boundary: Optional[datetime] = None,
) -> ArtifactAuditStamp:
    explicit_regime = str(row.get("stat_regime", "")).strip()
    if explicit_regime in CANONICAL_STAT_REGIMES:
        audit_status = str(row.get("audit_status", "")).strip()
        if audit_status not in CANONICAL_AUDIT_STATUSES:
            audit_status = AUDIT_STATUS_UNKNOWN
        version = str(row.get("artifact_audit_version", "")).strip()
        if not version:
            version = ARTIFACT_AUDIT_VERSION_PHASE1_V1
        return ArtifactAuditStamp(
            stat_regime=explicit_regime,
            audit_status=audit_status,
            artifact_audit_version=version,
            audit_reason="explicit_stamp",
            requires_repromotion=explicit_regime == STAT_REGIME_PRE_AUDIT,
            requires_manual_review=audit_status == AUDIT_STATUS_MANUAL_REVIEW_REQUIRED,
            inference_confidence="high",
        )

    version = str(row.get("artifact_audit_version", "")).strip()
    if version in CANONICAL_ARTIFACT_AUDIT_VERSIONS:
        return ArtifactAuditStamp(
            stat_regime=STAT_REGIME_POST_AUDIT,
            audit_status=AUDIT_STATUS_CURRENT,
            artifact_audit_version=version,
            audit_reason="recognized_audit_version",
            requires_repromotion=False,
            requires_manual_review=False,
            inference_confidence="high",
        )

    phase1_fields_present = sum(1 for f in PHASE1_REQUIRED_FIELDS if _has_valid_value(row, f))
    total_phase1_fields = len(PHASE1_REQUIRED_FIELDS)
    has_phase1_fields = phase1_fields_present >= total_phase1_fields * 0.8

    search_burden_present = sum(1 for f in SEARCH_BURDEN_REQUIRED_FIELDS if _has_valid_value(row, f))
    has_search_burden = search_burden_present >= len(SEARCH_BURDEN_REQUIRED_FIELDS) * 0.5

    policy_version = str(row.get("policy_version", "")).strip()
    bundle_version = str(row.get("bundle_version", "")).strip()
    recognized_policy = policy_version.startswith("phase") and "_" in policy_version
    recognized_bundle = bundle_version.startswith("phase") and "_" in bundle_version

    boundary = audit_boundary or _get_phase1_audit_boundary()
    artifact_dt = artifact_timestamp or _parse_iso_datetime(row.get("created_at") or row.get("timestamp"))

    if has_phase1_fields and has_search_burden and (recognized_policy or recognized_bundle):
        return ArtifactAuditStamp(
            stat_regime=STAT_REGIME_POST_AUDIT,
            audit_status=AUDIT_STATUS_CURRENT,
            artifact_audit_version=ARTIFACT_AUDIT_VERSION_PHASE1_V1,
            audit_reason="phase1_fields_present",
            requires_repromotion=False,
            requires_manual_review=False,
            inference_confidence="high",
        )

    if has_phase1_fields and (artifact_dt and artifact_dt >= boundary):
        return ArtifactAuditStamp(
            stat_regime=STAT_REGIME_POST_AUDIT,
            audit_status=AUDIT_STATUS_CURRENT,
            artifact_audit_version=ARTIFACT_AUDIT_VERSION_PHASE1_V1,
            audit_reason="phase1_fields_after_boundary",
            requires_repromotion=False,
            requires_manual_review=False,
            inference_confidence="medium",
        )

    if artifact_dt and artifact_dt < boundary:
        missing_phase1 = [f for f in PHASE1_REQUIRED_FIELDS if not _has_valid_value(row, f)]
        return ArtifactAuditStamp(
            stat_regime=STAT_REGIME_PRE_AUDIT,
            audit_status=AUDIT_STATUS_LEGACY,
            artifact_audit_version="",
            audit_reason=f"predates_boundary_missing_{len(missing_phase1)}_phase1_fields",
            requires_repromotion=True,
            requires_manual_review=False,
            inference_confidence="medium",
        )

    if has_phase1_fields and (recognized_policy or recognized_bundle or (artifact_dt and artifact_dt >= boundary)):
        return ArtifactAuditStamp(
            stat_regime=STAT_REGIME_POST_AUDIT,
            audit_status=AUDIT_STATUS_CURRENT,
            artifact_audit_version=ARTIFACT_AUDIT_VERSION_PHASE1_V1,
            audit_reason="phase1_fields_partial_with_provenance",
            requires_repromotion=False,
            requires_manual_review=False,
            inference_confidence="medium",
        )

    return ArtifactAuditStamp(
        stat_regime=STAT_REGIME_UNKNOWN,
        audit_status=AUDIT_STATUS_UNKNOWN,
        artifact_audit_version="",
        audit_reason="insufficient_provenance",
        requires_repromotion=False,
        requires_manual_review=True,
        inference_confidence="low",
    )


def _has_valid_value(row: Dict[str, Any], field: str) -> bool:
    value = row.get(field)
    if value is None:
        return False
    if isinstance(value, float):
        import math
        return math.isfinite(value)
    if isinstance(value, (int, bool)):
        return True
    text = str(value).strip().lower()
    return text not in {"", "nan", "none", "null", "na", "inf", "-inf"}


def default_audit_stamp(
    *,
    stat_regime: str = STAT_REGIME_POST_AUDIT,
    audit_status: str = AUDIT_STATUS_CURRENT,
    audit_reason: str = "default_for_new_artifact",
) -> ArtifactAuditStamp:
    return ArtifactAuditStamp(
        stat_regime=stat_regime,
        audit_status=audit_status,
        artifact_audit_version=ARTIFACT_AUDIT_VERSION_PHASE1_V1,
        audit_reason=audit_reason,
        requires_repromotion=stat_regime == STAT_REGIME_PRE_AUDIT,
        requires_manual_review=audit_status == AUDIT_STATUS_MANUAL_REVIEW_REQUIRED,
        inference_confidence="high",
    )


def is_post_audit_artifact(row: Dict[str, Any]) -> bool:
    stamp = infer_stat_regime_from_artifact_metadata(row)
    return stamp.stat_regime == STAT_REGIME_POST_AUDIT


def requires_repromotion_from_stamp(stamp: ArtifactAuditStamp) -> bool:
    return stamp.stat_regime == STAT_REGIME_PRE_AUDIT or stamp.requires_repromotion


def stamp_row(row: Dict[str, Any], *, stamp: Optional[ArtifactAuditStamp] = None) -> Dict[str, Any]:
    effective_stamp = stamp or default_audit_stamp()
    out = dict(row)
    out["stat_regime"] = effective_stamp.stat_regime
    out["audit_status"] = effective_stamp.audit_status
    out["artifact_audit_version"] = effective_stamp.artifact_audit_version
    return out


__all__ = [
    "STAT_REGIME_PRE_AUDIT",
    "STAT_REGIME_POST_AUDIT",
    "STAT_REGIME_UNKNOWN",
    "CANONICAL_STAT_REGIMES",
    "AUDIT_STATUS_CURRENT",
    "AUDIT_STATUS_DEGRADED",
    "AUDIT_STATUS_MANUAL_REVIEW_REQUIRED",
    "AUDIT_STATUS_LEGACY",
    "AUDIT_STATUS_UNKNOWN",
    "CANONICAL_AUDIT_STATUSES",
    "ARTIFACT_AUDIT_VERSION_PHASE1_V1",
    "CANONICAL_ARTIFACT_AUDIT_VERSIONS",
    "ArtifactAuditStamp",
    "infer_stat_regime_from_artifact_metadata",
    "default_audit_stamp",
    "is_post_audit_artifact",
    "requires_repromotion_from_stamp",
    "stamp_row",
]
