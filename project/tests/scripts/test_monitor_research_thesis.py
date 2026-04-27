import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from project.scripts.monitor_research_thesis import build_report

def test_monitor_report_schema():
    """Verify the report has the expected schema version and slug."""
    with patch("project.scripts.monitor_research_thesis._load_eval_results") as m_eval:
        m_eval.return_value = None # Empty state
        # Mock other loaders to avoid FileNotFoundError or other issues
        with patch("project.scripts.monitor_research_thesis._load_validation_bundle") as m_val:
            m_val.return_value = {}
            with patch("project.scripts.monitor_research_thesis._load_promotion_trace") as m_promo:
                m_promo.return_value = None
                report = build_report(run_id="test_run", data_root=Path("/tmp"))
                assert report["schema_version"] == "monitor_report_v1"
                assert report["thesis_slug"] == "oasrep_chop_long_48b"

def test_deployment_ready_logic():
    """Verify deployment_ready is true only when all gates pass."""
    with patch("project.scripts.monitor_research_thesis._load_eval_results") as m_eval:
        # Mock other loaders
        with patch("project.scripts.monitor_research_thesis._load_validation_bundle") as m_val:
            m_val.return_value = {}
            with patch("project.scripts.monitor_research_thesis._load_promotion_trace") as m_promo:
                m_promo.return_value = None
                
                # Mocking eval results that pass all gates
                # robustness >= 0.70, t_net >= 2.0, net_bps > 0
                m_eval.side_effect = [
                    {
                        "n": 100,
                        "mean_return_bps": 50.0,
                        "mean_return_net_bps": 30.0,
                        "hit_rate": 0.55,
                        "mae_mean_bps": 10.0,
                        "mfe_mean_bps": 60.0,
                        "t_stat_net": 2.5,
                        "robustness_score": 0.75,
                        "expected_cost_bps_per_trade": 10.0,
                        "sharpe": 1.5,
                        "stress_score": 0.8,
                        "placebo_shift_effect": 0.1,
                        "placebo_random_entry_effect": 0.05
                    },
                    None # ETH cross-symbol
                ]
                
                report = build_report(run_id="test_run", data_root=Path("/tmp"))
                assert report["deployment_ready"] is True
                assert report["gate_progress_to_0_70"] >= 1.0

                # Now mock a failure (robustness < 0.70)
                m_eval.side_effect = [
                    {
                        "n": 100,
                        "mean_return_bps": 50.0,
                        "mean_return_net_bps": 30.0,
                        "hit_rate": 0.55,
                        "mae_mean_bps": 10.0,
                        "mfe_mean_bps": 60.0,
                        "t_stat_net": 2.5,
                        "robustness_score": 0.585,
                        "expected_cost_bps_per_trade": 10.0,
                        "sharpe": 1.5,
                        "stress_score": 0.8,
                        "placebo_shift_effect": 0.1,
                        "placebo_random_entry_effect": 0.05
                    },
                    None # ETH cross-symbol
                ]
                report = build_report(run_id="test_run", data_root=Path("/tmp"))
                assert report["deployment_ready"] is False
                assert report["gate_progress_to_0_70"] < 1.0
                assert "robustness < 0.70" in report["deployment_blocker"]
