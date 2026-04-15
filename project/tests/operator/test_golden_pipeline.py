import json
import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from project.research.validation.contracts import (
    ValidationBundle, ValidatedCandidateRecord, ValidationDecision, ValidationMetrics
)
from project.research.validation.result_writer import write_validation_bundle, write_validated_candidate_tables
from project.research.services.promotion_service import execute_promotion, PromotionConfig
from project.live.runner import LiveEngineRunner
from project.live.contracts import PromotedThesis

@pytest.fixture
def mock_pipeline_data(tmp_path):
    data_root = tmp_path / "data"
    data_root.mkdir()
    (data_root / "reports" / "phase2" / "golden_run").mkdir(parents=True)
    (data_root / "reports" / "validation").mkdir(parents=True)
    (data_root / "reports" / "promotions").mkdir(parents=True)
    (data_root / "reports" / "promoted_theses").mkdir(parents=True)
    (data_root / "runs" / "golden_run").mkdir(parents=True)
    (data_root / "runs" / "golden_run" / "run_manifest.json").write_text('{"run_mode": "confirmatory"}')
    return data_root

def test_golden_pipeline_end_to_end(mock_pipeline_data):
    run_id = "golden_run"
    persist_dir = mock_pipeline_data / "live" / "persist"
    
    # 1. DISCOVER (Mocked)
    candidates_path = mock_pipeline_data / "reports" / "edge_candidates" / run_id / "edge_candidates_normalized.parquet"
    candidates_path.parent.mkdir(parents=True)
    
    from project.specs.ontology import ontology_spec_hash
    from project import PROJECT_ROOT
    curr_hash = ontology_spec_hash(PROJECT_ROOT.parent)
    
    df = pd.DataFrame([{
        "candidate_id": "cand_golden",
        "event_type": "VOL_SHOCK",
        "family": "VOL_SHOCK",
        "rule_template": "tpl1",
        "direction": "long",
        "horizon": 12,
        "n_obs": 100,
        "n_events": 100,
        "expectancy": 0.1,
        "net_expectancy_bps": 5.0,
        "q_value": 0.01,
        "p_value": 0.01,
        "stability_score": 0.8,
        "sign_consistency": 0.9,
        "cost_survival_ratio": 0.8,
        "tob_coverage": 0.9,
        "selection_score": 0.8,
        "confirmatory_locked": True,
        "frozen_spec_hash": curr_hash,
    }])
    df.to_parquet(candidates_path)
    
    # 2. VALIDATE
    decision = ValidationDecision(status="validated", candidate_id="cand_golden", run_id=run_id)
    candidate = ValidatedCandidateRecord(
        candidate_id="cand_golden", 
        decision=decision, 
        metrics=ValidationMetrics(sample_count=100, expectancy=0.1, net_expectancy=0.05),
        template_id="tpl1",
        direction="long",
        horizon_bars=12
    )
    bundle = ValidationBundle(run_id=run_id, created_at="2026-01-01", validated_candidates=[candidate])
    
    write_validation_bundle(bundle, base_dir=mock_pipeline_data / "reports" / "validation" / run_id)
    write_validated_candidate_tables(bundle, base_dir=mock_pipeline_data / "reports" / "validation" / run_id)
    
    # 3. PROMOTE
    config = PromotionConfig(
        run_id=run_id, symbols="BTC", out_dir=mock_pipeline_data / "reports" / "promotions" / run_id,
        max_q_value=0.05, min_events=20, min_stability_score=0.5, min_sign_consistency=0.5,
        min_cost_survival_ratio=0.5, max_negative_control_pass_rate=0.05, min_tob_coverage=0.5,
        require_hypothesis_audit=False, allow_missing_negative_controls=True,
        require_multiplicity_diagnostics=False, min_dsr=0.0, max_overlap_ratio=1.0,
        max_profile_correlation=1.0, allow_discovery_promotion=True,
        program_id="golden_program", retail_profile="default", objective_name="default",
        objective_spec=None, retail_profiles_spec=None
    )
    
    # Mock audit_df with evidence_bundle_json
    mock_bundle = {
        "candidate_id": "cand_golden",
        "event_type": "VOL_SHOCK",
        "event_family": "VOL_SHOCK",
        "sample_definition": {"symbol": "BTCUSDT", "n_events": 100},
        "split_definition": {"bar_duration_minutes": 60},
        "effect_estimates": {"estimate_bps": 10.0, "net_expectancy_bps": 5.0},
        "uncertainty_estimates": {"q_value": 0.01},
        "stability_tests": {"stability_score": 0.8},
        "falsification_results": {"negative_control_pass_rate": 0.0},
        "cost_robustness": {"cost_survival_ratio": 0.8, "net_expectancy_bps": 5.0},
        "multiplicity_adjustment": {"p_value_adj": 0.01},
        "timeframe": "1h",
        "symbol_scope": {"symbols": ["BTCUSDT"]},
        "evidence": {"sample_size": 100, "net_expectancy_bps": 5.0},
        "lineage": {"run_id": run_id, "candidate_id": "cand_golden"},
        "promotion_decision": {"promotion_status": "promoted"}
    }
    
    mock_audit = pd.DataFrame([{
        "candidate_id": "cand_golden",
        "event_type": "VOL_SHOCK",
        "promotion_decision": "promoted",
        "promotion_track": "standard",
        "evidence_bundle_json": json.dumps(mock_bundle),
        "promotion_metrics_trace": "{}",
        "status": "promoted"
    }])
    
    with patch("project.research.services.promotion_service.get_data_root", return_value=mock_pipeline_data):
        with patch("project.research.validation.result_writer.get_data_root", return_value=mock_pipeline_data):
            with patch("project.research.services.promotion_service.load_run_manifest", return_value={"run_mode": "confirmatory"}):
                with patch("project.research.services.promotion_service.resolve_objective_profile_contract") as mock_contract:
                    mock_contract.return_value = MagicMock()
                    with patch("project.research.services.promotion_service.promote_candidates") as mock_promote:
                        mock_promote.return_value = (mock_audit, mock_audit, {})
                        with patch("project.research.live_export.get_data_root", return_value=mock_pipeline_data):
                            result = execute_promotion(config)
                            assert result.exit_code == 0
                            
                            from project.research.live_export import export_promoted_theses_for_run
                            export_res = export_promoted_theses_for_run(run_id, data_root=mock_pipeline_data)
                            assert export_res.thesis_count > 0
    
    # 4. DEPLOY (Admission Control check)
    with patch("project.artifacts.catalog.get_data_root", return_value=mock_pipeline_data):
        runner = LiveEngineRunner(
            symbols=["BTCUSDT"],
            runtime_mode="monitor_only",
            strategy_runtime={
                "implemented": True,
                "thesis_run_id": run_id,
                "auto_submit": False,
                "persist_dir": str(persist_dir),
            }
        )
        
        assert runner._thesis_store is not None
        assert len(runner._thesis_store.all()) == 1
        assert runner.thesis_manager.get_state("thesis::golden_run::cand_golden") is not None
        assert runner.thesis_manager.get_state("thesis::golden_run::cand_golden").state == "eligible"

def test_deploy_rejects_unvalidated(mock_pipeline_data):
    # Try to load a run that exists but has no promoted theses
    run_id = "raw_run"
    (mock_pipeline_data / "reports" / "phase2" / run_id).mkdir(parents=True)
    
    with patch("project.artifacts.catalog.get_data_root", return_value=mock_pipeline_data):
        with pytest.raises(RuntimeError, match="Configured thesis store is unavailable"):
            LiveEngineRunner(
                symbols=["BTCUSDT"],
                runtime_mode="monitor_only",
                strategy_runtime={
                    "implemented": True,
                    "thesis_run_id": run_id
                }
            )
