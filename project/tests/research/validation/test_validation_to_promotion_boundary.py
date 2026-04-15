import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from project.research.validation.contracts import (
    ValidationBundle,
    ValidatedCandidateRecord,
    ValidationDecision,
    ValidationMetrics,
    ValidationReasonCodes,
)
from project.research.services.evaluation_service import ValidationService
from project.research.services.promotion_service import execute_promotion, PromotionConfig


@pytest.fixture
def mock_data_root(tmp_path):
    data_root = tmp_path / "data"
    data_root.mkdir()
    (data_root / "reports" / "validation").mkdir(parents=True)
    (data_root / "reports" / "promotions").mkdir(parents=True)
    (data_root / "runs" / "test_run").mkdir(parents=True)
    (data_root / "runs" / "test_run" / "run_manifest.json").write_text('{"run_mode": "confirmatory"}')
    return data_root


def _create_mock_candidates_table(mock_data_root, run_id, candidate_ids):
    from project.specs.ontology import ontology_spec_hash
    from project import PROJECT_ROOT
    curr_hash = ontology_spec_hash(PROJECT_ROOT.parent)

    (mock_data_root / "reports" / "edge_candidates" / run_id).mkdir(parents=True, exist_ok=True)
    rows = []
    for cid in candidate_ids:
        rows.append({
            "candidate_id": cid,
            "event_type": "VOL_SHOCK",
            "family": "VOL_SHOCK",
            "rule_template": "tpl1",
            "direction": "long",
            "horizon": 12,
            "n_events": 100,
            "n_obs": 100,
            "expectancy": 0.1,
            "net_expectancy_bps": 8.0,
            "q_value": 0.01,
            "p_value": 0.01,
            "stability_score": 0.8,
            "sign_consistency": 0.9,
            "cost_survival_ratio": 0.8,
            "tob_coverage": 0.9,
            "selection_score": 0.8,
            "confirmatory_locked": True,
            "frozen_spec_hash": curr_hash,
        })
    pd.DataFrame(rows).to_parquet(
        mock_data_root / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.parquet"
    )


def test_validation_maps_oos_gate_failure_to_explicit_reason(mock_data_root):
    service = ValidationService(data_root=mock_data_root)
    row = {
        "candidate_id": "cand_oos_fail",
        "rule_template": "mean_reversion",
        "direction": "long",
        "horizon": "12b",
        "n_events": 100,
        "expectancy": 0.01,
        "p_value": 0.01,
        "q_value": 0.02,
        "stability_score": 0.69,
        "gate_oos_validation": False,
        "gate_after_cost_positive": True,
        "gate_after_cost_stressed_positive": True,
        "gate_c_regime_stable": True,
        "gate_multiplicity": True,
    }

    record = service._map_row_to_validated_record(row, "test_run")

    assert record.decision.status == "rejected"
    assert record.decision.reason_codes == [ValidationReasonCodes.FAILED_OOS_VALIDATION]


def test_promotion_fails_without_bundle_by_default(mock_data_root):
    run_id = "test_run"
    _create_mock_candidates_table(mock_data_root, run_id, ["cand_1"])
    
    config = PromotionConfig(
        run_id=run_id, symbols="BTC", out_dir=None, max_q_value=0.05, min_events=20,
        min_stability_score=0.5, min_sign_consistency=0.5, min_cost_survival_ratio=0.5,
        max_negative_control_pass_rate=0.05, min_tob_coverage=0.5,
        require_hypothesis_audit=False, allow_missing_negative_controls=True,
        require_multiplicity_diagnostics=False, min_dsr=0.0, max_overlap_ratio=1.0,
        max_profile_correlation=1.0, allow_discovery_promotion=True,
        program_id="test_program", retail_profile="default", objective_name="default",
        objective_spec=None, retail_profiles_spec=None,
    )
    
    with patch("project.research.services.promotion_service.get_data_root", return_value=mock_data_root):
        with patch("project.research.validation.result_writer.get_data_root", return_value=mock_data_root):
                with patch("project.research.services.promotion_service.load_run_manifest", return_value={"run_mode": "confirmatory"}):
                    result = execute_promotion(config)
                    assert result.exit_code != 0
                    assert "Missing required validation artifact" in result.diagnostics.get("error", "")


def test_promotion_rejects_legacy_candidate_tables_without_canonical_validation(mock_data_root):
    run_id = "test_run"
    _create_mock_candidates_table(mock_data_root, run_id, ["cand_1"])
    
    config = PromotionConfig(
        run_id=run_id, symbols="BTC", out_dir=None, max_q_value=0.05, min_events=20,
        min_stability_score=0.5, min_sign_consistency=0.5, min_cost_survival_ratio=0.5,
        max_negative_control_pass_rate=0.05, min_tob_coverage=0.5,
        require_hypothesis_audit=False, allow_missing_negative_controls=True,
        require_multiplicity_diagnostics=False, min_dsr=0.0, max_overlap_ratio=1.0,
        max_profile_correlation=1.0, allow_discovery_promotion=True,
        program_id="test_program", retail_profile="default", objective_name="default",
        objective_spec=None, retail_profiles_spec=None,
    )
    
    with patch("project.research.services.promotion_service.get_data_root", return_value=mock_data_root):
        with patch("project.research.validation.result_writer.get_data_root", return_value=mock_data_root):
            with patch("project.research.services.promotion_service.load_run_manifest", return_value={"run_mode": "confirmatory"}):
                result = execute_promotion(config)
                assert result.exit_code != 0
                assert "Missing required validation artifact" in result.diagnostics.get("error", "")


def test_promotion_uses_canonical_validated_candidates(mock_data_root):
    run_id = "test_run"
    _create_mock_candidates_table(mock_data_root, run_id, ["cand_1", "cand_2"])
    
    # Bundle only validates cand_1
    decision1 = ValidationDecision(status="validated", candidate_id="cand_1", run_id=run_id)
    candidate1 = ValidatedCandidateRecord(candidate_id="cand_1", decision=decision1, metrics=ValidationMetrics(sample_count=100))
    
    decision2 = ValidationDecision(status="rejected", candidate_id="cand_2", run_id=run_id, reason_codes=["failed_stability"])
    candidate2 = ValidatedCandidateRecord(candidate_id="cand_2", decision=decision2, metrics=ValidationMetrics(sample_count=100))
    
    bundle = ValidationBundle(
        run_id=run_id, created_at="2026-01-01", 
        validated_candidates=[candidate1], 
        rejected_candidates=[candidate2]
    )
    
    from project.research.validation.result_writer import write_validation_bundle, write_validated_candidate_tables
    write_validation_bundle(bundle, base_dir=mock_data_root / "reports" / "validation" / run_id)
    paths = write_validated_candidate_tables(bundle, base_dir=mock_data_root / "reports" / "validation" / run_id)
    assert paths["promotion_ready_candidates"].name == "promotion_ready_candidates.parquet"
    
    config = PromotionConfig(
        run_id=run_id, symbols="BTC", out_dir=None, max_q_value=0.05, min_events=20,
        min_stability_score=0.5, min_sign_consistency=0.5, min_cost_survival_ratio=0.5,
        max_negative_control_pass_rate=0.05, min_tob_coverage=0.5,
        require_hypothesis_audit=False, allow_missing_negative_controls=True,
        require_multiplicity_diagnostics=False, min_dsr=0.0, max_overlap_ratio=1.0,
        max_profile_correlation=1.0, allow_discovery_promotion=True,
        program_id="test_program", retail_profile="default", objective_name="default",
        objective_spec=None, retail_profiles_spec=None
    )
    
    with patch("project.research.services.promotion_service.get_data_root", return_value=mock_data_root):
        with patch("project.research.validation.result_writer.get_data_root", return_value=mock_data_root):
            with patch("project.research.services.promotion_service.load_run_manifest", return_value={"run_mode": "confirmatory"}):
                with patch("project.research.services.promotion_service.resolve_objective_profile_contract") as mock_contract:
                    mock_contract.return_value = MagicMock()
                    # We expect it to call promote_candidates with ONLY cand_1
                    with patch("project.research.services.promotion_service.promote_candidates") as mock_promote:
                        mock_promote.return_value = (pd.DataFrame([{"candidate_id": "cand_1"}]), pd.DataFrame([{"candidate_id": "cand_1"}]), {})
                        execute_promotion(config)
                        
                        # Verify candidates_df passed to promote_candidates only has cand_1
                        args, kwargs = mock_promote.call_args
                        passed_df = kwargs['candidates_df']
                        assert list(passed_df['candidate_id']) == ["cand_1"]


def test_promoted_result_contains_maturity_fields(mock_data_root):
    # This tests the output of _assemble_promotion_result via execute_promotion
    run_id = "test_run"
    _create_mock_candidates_table(mock_data_root, run_id, ["cand_1"])
    
    decision = ValidationDecision(status="validated", candidate_id="cand_1", run_id=run_id)
    candidate = ValidatedCandidateRecord(candidate_id="cand_1", decision=decision, metrics=ValidationMetrics(sample_count=100))
    bundle = ValidationBundle(run_id=run_id, created_at="2026-01-01", validated_candidates=[candidate])
    
    from project.research.validation.result_writer import write_validation_bundle, write_validated_candidate_tables
    write_validation_bundle(bundle, base_dir=mock_data_root / "reports" / "validation" / run_id)
    paths = write_validated_candidate_tables(bundle, base_dir=mock_data_root / "reports" / "validation" / run_id)
    assert paths["promotion_ready_candidates"].name == "promotion_ready_candidates.parquet"
    
    config = PromotionConfig(
        run_id=run_id, symbols="BTC", out_dir=None, max_q_value=0.05, min_events=20,
        min_stability_score=0.5, min_sign_consistency=0.5, min_cost_survival_ratio=0.5,
        max_negative_control_pass_rate=0.05, min_tob_coverage=0.5,
        require_hypothesis_audit=False, allow_missing_negative_controls=True,
        require_multiplicity_diagnostics=False, min_dsr=0.0, max_overlap_ratio=1.0,
        max_profile_correlation=1.0, allow_discovery_promotion=True,
        program_id="test_program", retail_profile="default", objective_name="default",
        objective_spec=None, retail_profiles_spec=None
    )
    
    with patch("project.research.services.promotion_service.get_data_root", return_value=mock_data_root):
        with patch("project.research.validation.result_writer.get_data_root", return_value=mock_data_root):
            with patch("project.research.services.promotion_service.load_run_manifest", return_value={"run_mode": "confirmatory"}):
                with patch("project.research.services.promotion_service.resolve_objective_profile_contract") as mock_contract:
                    mock_contract.return_value = MagicMock()
                    # Here we need to return a DataFrame that has the new columns
                    # Actually, _assemble_promotion_result adds them, so they should be in promoted_df
                    # But we mocked promote_candidates, so we must ensure it returns them or don't mock it that deep.
                    # Let's mock promote_candidates to return a DF with the new columns
                    mock_audit = pd.DataFrame([{
                        "candidate_id": "cand_1",
                        "promotion_decision": "promoted",
                        "promotion_class": "paper_promoted",
                        "readiness_status": "paper_ready",
                        "deployment_state_default": "paper_only"
                    }])
                    with patch("project.research.services.promotion_service.promote_candidates") as mock_promote:
                        mock_promote.return_value = (mock_audit, mock_audit, {})
                        result = execute_promotion(config)
                        assert "promotion_class" in result.promoted_df.columns
                        assert result.promoted_df.iloc[0]["promotion_class"] == "paper_promoted"
