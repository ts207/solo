"""Integration tests for historical artifact audit scanner."""
from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from project.research.audit_historical_artifacts import (
    ArtifactInventoryRow,
    AuditInventoryResult,
    build_run_historical_trust_summary,
    scan_historical_artifacts,
    write_artifact_audit_stamp_sidecar,
    write_audit_inventory,
    rewrite_audit_stamp_sidecars,
)
from project.research.contracts.historical_trust import (
    HISTORICAL_TRUST_LEGACY,
    HISTORICAL_TRUST_REQUIRES_REVALIDATION,
    HISTORICAL_TRUST_TRUSTED,
    trusted_under_current_rules,
)
from project.research.contracts.stat_regime import (
    AUDIT_STATUS_CURRENT,
    AUDIT_STATUS_LEGACY,
    AUDIT_STATUS_MANUAL_REVIEW_REQUIRED,
    AUDIT_STATUS_UNKNOWN,
    STAT_REGIME_POST_AUDIT,
    STAT_REGIME_PRE_AUDIT,
    STAT_REGIME_UNKNOWN,
)


@pytest.fixture
def temp_data_root():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


def _write_parquet_artifact(data_root: Path, run_id: str, filename: str, records: list[dict]) -> Path:
    import pandas as pd
    promo_dir = data_root / "reports" / "promotions" / run_id
    promo_dir.mkdir(parents=True, exist_ok=True)
    path = promo_dir / filename
    df = pd.DataFrame(records)
    df.to_parquet(path, index=False)
    return path


def _write_validation_bundle_artifact(data_root: Path, run_id: str) -> None:
    validation_dir = data_root / "reports" / "validation" / run_id
    validation_dir.mkdir(parents=True, exist_ok=True)
    (validation_dir / "validation_bundle.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "created_at": "2026-04-12T00:00:00Z",
                "validated_candidates": [],
                "rejected_candidates": [],
                "inconclusive_candidates": [],
                "summary_stats": {},
                "effect_stability_report": {},
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "validation_status": "validated",
                "validation_run_id": run_id,
                "validation_program_id": "prog_1",
                "metric_sample_count": 100,
                "metric_q_value": 0.01,
                "metric_stability_score": 0.9,
                "metric_net_expectancy": 5.0,
            }
        ]
    ).to_parquet(validation_dir / "promotion_ready_candidates.parquet", index=False)


class TestScanHistoricalArtifacts:
    def test_scan_empty_data_root(self, temp_data_root):
        result = scan_historical_artifacts(data_root=temp_data_root)
        assert len(result.rows) == 0
        assert len(result.scanned_artifact_paths) == 0
        assert len(result.errors) == 0

    def test_scan_single_artifact(self, temp_data_root):
        records = [
            {
                "run_id": "run_001",
                "candidate_id": "cand_001",
                "hypothesis_id": "hyp_001",
                "campaign_id": "camp_001",
                "program_id": "prog_001",
                "q_value": 0.03,
                "q_value_scope": 0.04,
                "effective_q_value": 0.04,
                "num_tests_scope": 100,
                "multiplicity_scope_mode": "campaign_lineage",
                "multiplicity_scope_version": "phase1_v1",
                "policy_version": "phase4_pr5_v1",
                "bundle_version": "phase4_bundle_v1",
            }
        ]
        path = _write_parquet_artifact(temp_data_root, "run_001", "promotion_audit.parquet", records)
        result = scan_historical_artifacts(data_root=temp_data_root)
        assert len(result.rows) == 1
        assert len(result.scanned_artifact_paths) == 1
        assert result.rows[0].stat_regime == STAT_REGIME_POST_AUDIT
        assert result.rows[0].audit_status == AUDIT_STATUS_CURRENT
        assert result.rows[0].historical_trust_status == HISTORICAL_TRUST_LEGACY

    def test_scan_filters_by_run_id(self, temp_data_root):
        _write_parquet_artifact(
            temp_data_root,
            "run_001",
            "promotion_audit.parquet",
            [{"run_id": "run_001", "candidate_id": "c1", "q_value": 0.03}],
        )
        _write_parquet_artifact(
            temp_data_root,
            "run_002",
            "promotion_audit.parquet",
            [{"run_id": "run_002", "candidate_id": "c2", "q_value": 0.05}],
        )
        result = scan_historical_artifacts(data_root=temp_data_root, run_id="run_001")
        assert len(result.rows) == 1
        assert result.rows[0].run_id == "run_001"

    def test_scan_detects_pre_audit_artifact(self, temp_data_root):
        records = [
            {
                "run_id": "legacy_run",
                "candidate_id": "cand_legacy",
                "hypothesis_id": "hyp_legacy",
                "q_value": 0.03,
            }
        ]
        path = _write_parquet_artifact(temp_data_root, "legacy_run", "promotion_audit.parquet", records)
        result = scan_historical_artifacts(data_root=temp_data_root)
        assert len(result.rows) == 1
        assert result.rows[0].stat_regime == STAT_REGIME_UNKNOWN
        assert result.rows[0].audit_status == AUDIT_STATUS_UNKNOWN
        assert result.rows[0].requires_manual_review is True
        assert result.rows[0].historical_trust_status == HISTORICAL_TRUST_LEGACY

    def test_scan_validation_bundle_classifies_current_contract_trust(self, temp_data_root):
        _write_validation_bundle_artifact(temp_data_root, "run_001")
        result = scan_historical_artifacts(data_root=temp_data_root, run_id="run_001")
        trust_rows = [row for row in result.rows if row.artifact_type == "validation_bundle"]
        assert trust_rows
        assert trust_rows[0].historical_trust_status == HISTORICAL_TRUST_TRUSTED

    def test_scan_promoted_theses_malformed_requires_revalidation(self, temp_data_root):
        thesis_dir = temp_data_root / "live" / "theses" / "run_bad"
        thesis_dir.mkdir(parents=True, exist_ok=True)
        (thesis_dir / "promoted_theses.json").write_text("{not-json", encoding="utf-8")
        result = scan_historical_artifacts(data_root=temp_data_root, run_id="run_bad")
        assert result.errors
        summary = build_run_historical_trust_summary(run_id="run_bad", data_root=temp_data_root, result=result)
        assert summary["historical_trust_status"] == HISTORICAL_TRUST_REQUIRES_REVALIDATION


class TestWriteAuditInventory:
    def test_write_inventory_with_rows(self, temp_data_root):
        records = [
            {
                "run_id": "run_001",
                "candidate_id": "cand_001",
                "q_value": 0.03,
                "q_value_scope": 0.04,
                "effective_q_value": 0.04,
                "num_tests_scope": 100,
                "multiplicity_scope_mode": "campaign_lineage",
                "multiplicity_scope_version": "phase1_v1",
                "policy_version": "phase4_pr5_v1",
            }
        ]
        _write_parquet_artifact(temp_data_root, "run_001", "promotion_audit.parquet", records)
        result = scan_historical_artifacts(data_root=temp_data_root)
        output_dir = temp_data_root / "reports" / "audit"
        paths = write_audit_inventory(result, output_dir)
        assert paths["parquet_path"].exists()
        assert paths["json_path"].exists()
        assert paths["md_path"].exists()

    def test_write_inventory_empty_scan(self, temp_data_root):
        result = AuditInventoryResult(
            rows=[],
            run_id_counts={},
            stat_regime_counts={},
            audit_status_counts={},
            requires_repromotion_count=0,
            requires_manual_review_count=0,
            scanned_artifact_paths=[],
            errors=[],
        )
        output_dir = temp_data_root / "reports" / "audit"
        paths = write_audit_inventory(result, output_dir)
        assert paths["parquet_path"].exists()
        assert paths["json_path"].exists()
        assert paths["md_path"].exists()


class TestWriteArtifactAuditStampSidecar:
    def test_write_sidecar(self, temp_data_root):
        from project.research.contracts.stat_regime import ArtifactAuditStamp, ARTIFACT_AUDIT_VERSION_PHASE1_V1
        artifact_path = temp_data_root / "test_artifact.parquet"
        artifact_path.write_bytes(b"")
        stamp = ArtifactAuditStamp(
            stat_regime=STAT_REGIME_POST_AUDIT,
            audit_status=AUDIT_STATUS_CURRENT,
            artifact_audit_version=ARTIFACT_AUDIT_VERSION_PHASE1_V1,
            audit_reason="test",
            requires_repromotion=False,
            requires_manual_review=False,
            inference_confidence="high",
        )
        sidecar_path = write_artifact_audit_stamp_sidecar(
            artifact_path,
            stamp,
            trusted_under_current_rules("test_current"),
        )
        assert sidecar_path.exists()
        payload = json.loads(sidecar_path.read_text())
        assert payload["stat_regime"] == STAT_REGIME_POST_AUDIT
        assert payload["audit_status"] == AUDIT_STATUS_CURRENT
        assert payload["historical_trust_status"] == HISTORICAL_TRUST_TRUSTED


class TestRewriteAuditStampSidecars:
    def test_rewrite_aggregates_multiple_rows(self, temp_data_root):
        records = [
            {
                "run_id": "run_001",
                "candidate_id": "c1",
                "q_value": 0.03,
                "q_value_scope": 0.04,
                "effective_q_value": 0.04,
                "num_tests_scope": 100,
                "multiplicity_scope_mode": "campaign_lineage",
                "multiplicity_scope_version": "phase1_v1",
                "policy_version": "phase4_pr5_v1",
            },
            {
                "run_id": "run_001",
                "candidate_id": "c2",
                "q_value": 0.05,
                "q_value_scope": 0.06,
                "effective_q_value": 0.06,
                "num_tests_scope": 150,
                "multiplicity_scope_mode": "campaign_lineage",
                "multiplicity_scope_version": "phase1_v1",
                "policy_version": "phase4_pr5_v1",
            },
        ]
        artifact_path = _write_parquet_artifact(temp_data_root, "run_001", "promotion_audit.parquet", records)
        result = scan_historical_artifacts(data_root=temp_data_root)
        rewrite_result = rewrite_audit_stamp_sidecars(result)
        assert rewrite_result["sidecars_written"] == 1
        assert rewrite_result["artifacts_processed"] == 1
        sidecar_path = artifact_path.with_suffix(".parquet.audit_stamp.json")
        assert sidecar_path.exists()
        payload = json.loads(sidecar_path.read_text())
        assert payload["stat_regime"] == STAT_REGIME_POST_AUDIT
        assert payload["historical_trust_status"] == HISTORICAL_TRUST_LEGACY

    def test_rewrite_with_manual_review_takes_precedence(self, temp_data_root):
        records = [
            {"run_id": "run_001", "candidate_id": "c1", "q_value": 0.03, "policy_version": "phase4_v1"},
            {"run_id": "run_001", "candidate_id": "c2"},
        ]
        artifact_path = _write_parquet_artifact(temp_data_root, "run_001", "promotion_audit.parquet", records)
        result = scan_historical_artifacts(data_root=temp_data_root)
        for row in result.rows:
            if row.candidate_id == "c2":
                assert row.audit_status == AUDIT_STATUS_UNKNOWN
                assert row.stat_regime == STAT_REGIME_UNKNOWN
                assert row.requires_manual_review is True
        rewrite_result = rewrite_audit_stamp_sidecars(result)
        assert rewrite_result["sidecars_written"] == 1
        sidecar_path = artifact_path.with_suffix(".parquet.audit_stamp.json")
        payload = json.loads(sidecar_path.read_text())
        assert payload["audit_status"] == AUDIT_STATUS_MANUAL_REVIEW_REQUIRED
        assert payload["stat_regime"] == STAT_REGIME_UNKNOWN
        assert payload["historical_trust_status"] == HISTORICAL_TRUST_LEGACY
