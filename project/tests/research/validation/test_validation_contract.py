import json
from datetime import datetime
from pathlib import Path

import pytest

from project.core.exceptions import CompatibilityRequiredError
from project.research.validation.contracts import (
    ValidationBundle,
    ValidatedCandidateRecord,
    ValidationDecision,
    ValidationMetrics,
    ValidationReasonCodes,
)
from project.research.validation.result_writer import (
    write_validation_bundle,
    write_validated_candidate_tables,
    load_validation_bundle,
)


def test_validation_bundle_serialization(tmp_path):
    run_id = "test_run_123"
    created_at = datetime.now().isoformat()
    
    decision = ValidationDecision(
        status="validated",
        candidate_id="cand_1",
        run_id=run_id,
        reason_codes=[],
        summary="Looks good"
    )
    
    metrics = ValidationMetrics(
        sample_count=100,
        expectancy=0.05,
        p_value=0.01
    )
    
    candidate = ValidatedCandidateRecord(
        candidate_id="cand_1",
        decision=decision,
        metrics=metrics,
        anchor_summary="Summary",
        template_id="tpl_1",
        direction="long",
        horizon_bars=12
    )
    
    bundle = ValidationBundle(
        run_id=run_id,
        created_at=created_at,
        validated_candidates=[candidate],
        summary_stats={"total": 1}
    )
    
    # Write to tmp_path
    bundle_path = write_validation_bundle(bundle, base_dir=tmp_path)
    assert bundle_path.exists()
    
    # Load back
    loaded = load_validation_bundle(run_id, base_dir=tmp_path)
    assert loaded is not None
    assert loaded.run_id == run_id
    assert len(loaded.validated_candidates) == 1
    assert loaded.validated_candidates[0].candidate_id == "cand_1"
    assert loaded.validated_candidates[0].decision.status == "validated"
    assert loaded.validated_candidates[0].metrics.expectancy == 0.05


def test_validation_reason_codes():
    assert ValidationReasonCodes.FAILED_OOS_VALIDATION == "failed_oos_validation"
    assert ValidationReasonCodes.FAILED_STABILITY == "failed_stability"
    
    decision = ValidationDecision(
        status="rejected",
        candidate_id="cand_2",
        run_id="run_2",
        reason_codes=[ValidationReasonCodes.FAILED_STABILITY]
    )
    
    assert ValidationReasonCodes.FAILED_STABILITY in decision.reason_codes


def test_invalid_status():
    with pytest.raises(ValueError):
        ValidationDecision(
            status="invalid_status",
            candidate_id="cand_3",
            run_id="run_3"
        )


def test_strict_validation_bundle_rejects_legacy_without_promotion_ready_candidates(tmp_path):
    run_id = "legacy_validation_run"
    bundle = ValidationBundle(
        run_id=run_id,
        created_at=datetime.now().isoformat(),
        validated_candidates=[],
        summary_stats={},
    )
    write_validation_bundle(bundle, base_dir=tmp_path)

    with pytest.raises(CompatibilityRequiredError, match="legacy_but_interpretable"):
        load_validation_bundle(run_id, base_dir=tmp_path, strict=True)


def test_strict_validation_bundle_accepts_current_contract_with_companion(tmp_path):
    run_id = "current_validation_run"
    decision = ValidationDecision(
        status="validated",
        candidate_id="cand_current",
        run_id=run_id,
        reason_codes=[],
    )
    metrics = ValidationMetrics(sample_count=100, expectancy=0.05, q_value=0.01, stability_score=0.9, net_expectancy=0.04)
    candidate = ValidatedCandidateRecord(
        candidate_id="cand_current",
        decision=decision,
        metrics=metrics,
        template_id="tpl_current",
        direction="long",
        horizon_bars=12,
    )
    bundle = ValidationBundle(
        run_id=run_id,
        created_at=datetime.now().isoformat(),
        validated_candidates=[candidate],
        summary_stats={},
    )
    write_validation_bundle(bundle, base_dir=tmp_path)
    write_validated_candidate_tables(bundle, base_dir=tmp_path)

    loaded = load_validation_bundle(run_id, base_dir=tmp_path, strict=True)
    assert loaded.run_id == run_id
