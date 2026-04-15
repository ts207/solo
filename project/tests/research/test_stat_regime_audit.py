"""Tests for stat regime audit contract and inference helpers."""
from __future__ import annotations

import pytest

from project.research.contracts.stat_regime import (
    ARTIFACT_AUDIT_VERSION_PHASE1_V1,
    AUDIT_STATUS_CURRENT,
    AUDIT_STATUS_DEGRADED,
    AUDIT_STATUS_LEGACY,
    AUDIT_STATUS_MANUAL_REVIEW_REQUIRED,
    AUDIT_STATUS_UNKNOWN,
    STAT_REGIME_POST_AUDIT,
    STAT_REGIME_PRE_AUDIT,
    STAT_REGIME_UNKNOWN,
    ArtifactAuditStamp,
    default_audit_stamp,
    infer_stat_regime_from_artifact_metadata,
    is_post_audit_artifact,
    requires_repromotion_from_stamp,
    stamp_row,
)


class TestStatRegimeConstans:
    def test_canonical_values_are_defined(self):
        assert STAT_REGIME_PRE_AUDIT == "pre_audit_stat_regime"
        assert STAT_REGIME_POST_AUDIT == "post_audit_stat_regime"
        assert STAT_REGIME_UNKNOWN == "unknown_stat_regime"
        assert AUDIT_STATUS_CURRENT == "current"
        assert AUDIT_STATUS_DEGRADED == "degraded"
        assert AUDIT_STATUS_MANUAL_REVIEW_REQUIRED == "manual_review_required"
        assert AUDIT_STATUS_LEGACY == "legacy"
        assert AUDIT_STATUS_UNKNOWN == "unknown"
        assert ARTIFACT_AUDIT_VERSION_PHASE1_V1 == "phase1_v1"


class TestInferStatRegime:
    def test_explicit_stat_regime_is_preserved(self):
        row = {
            "stat_regime": STAT_REGIME_POST_AUDIT,
            "audit_status": AUDIT_STATUS_CURRENT,
            "artifact_audit_version": ARTIFACT_AUDIT_VERSION_PHASE1_V1,
        }
        stamp = infer_stat_regime_from_artifact_metadata(row)
        assert stamp.stat_regime == STAT_REGIME_POST_AUDIT
        assert stamp.audit_status == AUDIT_STATUS_CURRENT
        assert stamp.inference_confidence == "high"

    def test_pre_audit_explicit_stamp(self):
        row = {
            "stat_regime": STAT_REGIME_PRE_AUDIT,
        }
        stamp = infer_stat_regime_from_artifact_metadata(row)
        assert stamp.stat_regime == STAT_REGIME_PRE_AUDIT
        assert stamp.requires_repromotion is True

    def test_unknown_explicit_stamp(self):
        row = {
            "stat_regime": STAT_REGIME_UNKNOWN,
        }
        stamp = infer_stat_regime_from_artifact_metadata(row)
        assert stamp.stat_regime == STAT_REGIME_UNKNOWN
        assert stamp.audit_status == AUDIT_STATUS_UNKNOWN

    def test_recognized_audit_version_implies_post_audit(self):
        row = {
            "artifact_audit_version": "phase1_v1",
            "q_value": 0.03,
        }
        stamp = infer_stat_regime_from_artifact_metadata(row)
        assert stamp.stat_regime == STAT_REGIME_POST_AUDIT
        assert stamp.audit_status == AUDIT_STATUS_CURRENT
        assert stamp.audit_reason == "recognized_audit_version"

    def test_phase1_fields_present_implies_post_audit(self):
        row = {
            "q_value": 0.03,
            "q_value_scope": 0.04,
            "effective_q_value": 0.04,
            "num_tests_scope": 100,
            "multiplicity_scope_mode": "campaign_lineage",
            "multiplicity_scope_version": "phase1_v1",
            "search_candidates_generated": 500,
            "search_candidates_eligible": 200,
            "search_family_count": 15,
            "search_lineage_count": 3,
            "search_burden_estimated": False,
            "search_scope_version": "phase1_v1",
            "policy_version": "phase4_pr5_v1",
            "bundle_version": "phase4_bundle_v1",
        }
        stamp = infer_stat_regime_from_artifact_metadata(row)
        assert stamp.stat_regime == STAT_REGIME_POST_AUDIT
        assert stamp.audit_status == AUDIT_STATUS_CURRENT
        assert stamp.inference_confidence == "high"

    def test_missing_phase1_fields_implies_unknown(self):
        row = {
            "q_value": 0.03,
        }
        stamp = infer_stat_regime_from_artifact_metadata(row)
        assert stamp.stat_regime == STAT_REGIME_UNKNOWN
        assert stamp.audit_status == AUDIT_STATUS_UNKNOWN
        assert stamp.requires_manual_review is True

    def test_conflicting_fields_implies_unknown(self):
        row = {
            "q_value": 0.03,
            "q_value_scope": None,
            "effective_q_value": None,
            "num_tests_scope": None,
            "multiplicity_scope_mode": "",
        }
        stamp = infer_stat_regime_from_artifact_metadata(row)
        assert stamp.stat_regime == STAT_REGIME_UNKNOWN

    def test_partial_fields_with_policy_version_is_post_audit(self):
        row = {
            "q_value": 0.03,
            "q_value_scope": 0.04,
            "effective_q_value": 0.04,
            "num_tests_scope": 100,
            "multiplicity_scope_mode": "campaign_lineage",
            "policy_version": "phase4_pr5_v1",
        }
        stamp = infer_stat_regime_from_artifact_metadata(row)
        assert stamp.stat_regime == STAT_REGIME_POST_AUDIT
        assert stamp.audit_status == AUDIT_STATUS_CURRENT
        assert stamp.audit_reason == "phase1_fields_partial_with_provenance"

    def test_partial_fields_without_provenance_is_unknown(self):
        row = {
            "q_value": 0.03,
            "q_value_scope": 0.04,
            "effective_q_value": 0.04,
        }
        stamp = infer_stat_regime_from_artifact_metadata(row)
        assert stamp.stat_regime == STAT_REGIME_UNKNOWN
        assert stamp.audit_status == AUDIT_STATUS_UNKNOWN


class TestDefaultAuditStamp:
    def test_default_stamp_is_post_audit_current(self):
        stamp = default_audit_stamp()
        assert stamp.stat_regime == STAT_REGIME_POST_AUDIT
        assert stamp.audit_status == AUDIT_STATUS_CURRENT
        assert stamp.artifact_audit_version == ARTIFACT_AUDIT_VERSION_PHASE1_V1
        assert stamp.requires_repromotion is False
        assert stamp.requires_manual_review is False

    def test_degraded_stamp(self):
        stamp = default_audit_stamp(audit_status=AUDIT_STATUS_DEGRADED)
        assert stamp.audit_status == AUDIT_STATUS_DEGRADED


class TestIsPostAuditArtifact:
    def test_post_audit_artifact_returns_true(self):
        row = {
            "stat_regime": STAT_REGIME_POST_AUDIT,
        }
        assert is_post_audit_artifact(row) is True

    def test_pre_audit_artifact_returns_false(self):
        row = {
            "stat_regime": STAT_REGIME_PRE_AUDIT,
        }
        assert is_post_audit_artifact(row) is False

    def test_unknown_artifact_returns_false(self):
        row = {}
        assert is_post_audit_artifact(row) is False


class TestRequiresRepromotionFromStamp:
    def test_pre_audit_requires_repromotion(self):
        stamp = ArtifactAuditStamp(
            stat_regime=STAT_REGIME_PRE_AUDIT,
            audit_status=AUDIT_STATUS_LEGACY,
            artifact_audit_version="",
            audit_reason="predates_boundary",
            requires_repromotion=True,
            requires_manual_review=False,
            inference_confidence="medium",
        )
        assert requires_repromotion_from_stamp(stamp) is True

    def test_post_audit_does_not_require_repromotion(self):
        stamp = default_audit_stamp()
        assert requires_repromotion_from_stamp(stamp) is False


class TestStampRow:
    def test_stamp_row_adds_audit_fields(self):
        row = {"candidate_id": "abc123", "q_value": 0.03}
        stamped = stamp_row(row)
        assert "stat_regime" in stamped
        assert "audit_status" in stamped
        assert "artifact_audit_version" in stamped
        assert stamped["stat_regime"] == STAT_REGIME_POST_AUDIT
        assert stamped["audit_status"] == AUDIT_STATUS_CURRENT

    def test_stamp_row_preserves_existing_fields(self):
        row = {"candidate_id": "abc123", "event_type": "VOL_SHOCK"}
        stamped = stamp_row(row)
        assert stamped["candidate_id"] == "abc123"
        assert stamped["event_type"] == "VOL_SHOCK"

    def test_stamp_row_with_custom_stamp(self):
        custom = ArtifactAuditStamp(
            stat_regime=STAT_REGIME_PRE_AUDIT,
            audit_status=AUDIT_STATUS_LEGACY,
            artifact_audit_version="",
            audit_reason="test",
            requires_repromotion=True,
            requires_manual_review=False,
            inference_confidence="high",
        )
        row = {"candidate_id": "test"}
        stamped = stamp_row(row, stamp=custom)
        assert stamped["stat_regime"] == STAT_REGIME_PRE_AUDIT
        assert stamped["audit_status"] == AUDIT_STATUS_LEGACY