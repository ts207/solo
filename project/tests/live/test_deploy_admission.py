import pytest
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from project.live.deploy_admission import assert_deploy_admission

@pytest.fixture
def mock_thesis():
    thesis = MagicMock()
    thesis.thesis_id = "test_thesis"
    thesis.deployment_state = "monitor_only"
    thesis.live_approval.live_approval_status = ""
    thesis.cap_profile.is_configured = False
    thesis.lineage.run_id = "test_run"
    return thesis

@pytest.fixture
def mock_store(mock_thesis):
    store = MagicMock()
    store.all.return_value = [mock_thesis]
    return store

def test_deploy_admission_monitor_only_allowed(mock_store):
    """monitor_only state + runtime_mode=monitor_only -> pass"""
    with patch("project.live.deploy_admission.ThesisStore.from_path", return_value=mock_store):
        assert_deploy_admission(
            thesis_path=Path("dummy.json"),
            runtime_mode="monitor_only"
        )

def test_deploy_admission_simulation_blocked_for_monitor_only(mock_store):
    """monitor_only + simulation -> fail"""
    with patch("project.live.deploy_admission.ThesisStore.from_path", return_value=mock_store):
        with pytest.raises(PermissionError, match="Simulation mode blocked"):
            assert_deploy_admission(
                thesis_path=Path("dummy.json"),
                runtime_mode="simulation"
            )

def test_deploy_admission_trading_blocked_for_monitor_only(mock_store):
    """monitor_only + trading -> fail"""
    with patch("project.live.deploy_admission.ThesisStore.from_path", return_value=mock_store):
        with pytest.raises(PermissionError, match="Trading mode blocked"):
            assert_deploy_admission(
                thesis_path=Path("dummy.json"),
                runtime_mode="trading"
            )

def test_deploy_admission_simulation_allowed_for_promoted_ready(mock_store, mock_thesis):
    """promoted + simulation + deployment_ready=True -> pass"""
    mock_thesis.deployment_state = "promoted"
    with patch("project.live.deploy_admission.ThesisStore.from_path", return_value=mock_store):
        with patch("project.live.deploy_admission.json.loads") as m_json:
            m_json.return_value = {"deployment_ready": True}
            with patch("pathlib.Path.exists", return_value=True):
                with patch("pathlib.Path.read_text", return_value="{}"):
                    assert_deploy_admission(
                        thesis_path=Path("dummy.json"),
                        runtime_mode="simulation",
                        monitor_report_path=Path("report.json")
                    )

@patch("project.live.deploy_admission.evaluate_paper_gate")
def test_deploy_admission_trading_allowed_for_live_enabled_ready(mock_paper_gate, mock_store, mock_thesis, tmp_path):
    """live_enabled + trading + deployment_ready=True + all gates pass -> pass"""
    mock_thesis.deployment_state = "live_enabled"
    mock_thesis.lineage.run_id = "test_run"
    mock_paper_gate.return_value = MagicMock(status="pass")
    
    # Forward confirmation
    fc_dir = tmp_path / "reports" / "validation" / "test_run"
    fc_dir.mkdir(parents=True, exist_ok=True)
    (fc_dir / "forward_confirmation.json").write_text(json.dumps({
        "method": "oos_frozen_thesis_replay_v1",
        "metrics": {"status": "success", "event_count": 1, "mean_return_net_bps": 1.0, "t_stat_net": 1.0}
    }))
    
    monitor_path = tmp_path / "report.json"
    monitor_path.write_text(json.dumps({"deployment_ready": True}))
    
    with patch("project.live.deploy_admission.ThesisStore.from_path", return_value=mock_store):
        assert_deploy_admission(
            thesis_path=Path("dummy.json"),
            runtime_mode="trading",
            monitor_report_path=monitor_path,
            data_root=tmp_path
        )

def test_deploy_admission_trading_blocked_for_live_enabled_not_ready(mock_store, mock_thesis, tmp_path):
    """live_enabled + trading + deployment_ready=False -> fail"""
    mock_thesis.deployment_state = "live_enabled"
    monitor_path = tmp_path / "report.json"
    monitor_path.write_text(json.dumps({"deployment_ready": False}))
    
    with patch("project.live.deploy_admission.ThesisStore.from_path", return_value=mock_store):
        with pytest.raises(PermissionError, match="monitor report deployment_ready=False"):
            assert_deploy_admission(
                thesis_path=Path("dummy.json"),
                runtime_mode="trading",
                monitor_report_path=monitor_path,
                data_root=tmp_path
            )

def test_deploy_admission_passes_through_thesis_store_errors():
    """Verify that ThesisStore (and thus DeploymentGate) errors are propagated"""
    with patch("project.live.deploy_admission.ThesisStore.from_path", side_effect=RuntimeError("Gate failure")):
        with pytest.raises(RuntimeError, match="Gate failure"):
            assert_deploy_admission(
                thesis_path=Path("dummy.json"),
                runtime_mode="monitor_only"
            )

def test_deploy_admission_real_artifact_live_enabled_no_approval(tmp_path):
    """live_enabled without approval record -> should raise RuntimeError (via DeploymentGate)"""
    thesis_file = tmp_path / "promoted_theses.json"
    content = {
        "schema_version": "promoted_theses_v1",
        "run_id": "test_run",
        "generated_at_utc": "2026-04-27T00:00:00Z",
        "thesis_count": 1,
        "active_thesis_count": 1,
        "pending_thesis_count": 0,
        "theses": [
            {
                "thesis_id": "test_thesis",
                "deployment_state": "live_enabled",
                "status": "active",
                "timeframe": "5m",
                "primary_event_id": "VOL_SHOCK",
                "evidence": {"sample_size": 100},
                "lineage": {"run_id": "test_run", "candidate_id": "cand_1"},
            }
        ]
    }
    thesis_file.write_text(json.dumps(content))
    
    with patch("project.live.thesis_store.inspect_artifact_trust") as m_trust:
        m_trust.return_value.historical_trust_status = "trusted_under_current_rules"
        # DeploymentGate should raise because live_approval_status is empty
        with pytest.raises(RuntimeError, match="live_approval_status is '', expected 'approved'"):
            assert_deploy_admission(
                thesis_path=thesis_file,
                runtime_mode="trading"
            )

def test_deploy_admission_real_artifact_live_enabled_no_caps(tmp_path):
    """live_enabled + approval but NO caps -> should raise RuntimeError"""
    thesis_file = tmp_path / "promoted_theses.json"
    content = {
        "schema_version": "promoted_theses_v1",
        "run_id": "test_run",
        "generated_at_utc": "2026-04-27T00:00:00Z",
        "thesis_count": 1,
        "active_thesis_count": 1,
        "pending_thesis_count": 0,
        "theses": [
            {
                "thesis_id": "test_thesis",
                "deployment_state": "live_enabled",
                "deployment_mode_allowed": "live_enabled",
                "status": "active",
                "timeframe": "5m",
                "primary_event_id": "VOL_SHOCK",
                "live_approval": {
                    "live_approval_status": "approved",
                    "approved_by": "tester",
                    "approved_at": "2026-04-27T00:00:00Z",
                    "risk_profile_id": "standard"
                },
                "cap_profile": {
                    "max_notional": 0.0,
                    "max_position_notional": 0.0,
                    "max_daily_loss": 0.0
                },
                "evidence": {"sample_size": 100},
                "lineage": {"run_id": "test_run", "candidate_id": "cand_1"},
            }
        ]
    }
    thesis_file.write_text(json.dumps(content))
    
    with patch("project.live.thesis_store.inspect_artifact_trust") as m_trust:
        m_trust.return_value.historical_trust_status = "trusted_under_current_rules"
        with pytest.raises(RuntimeError, match="cap_profile has no hard caps configured"):
            assert_deploy_admission(
                thesis_path=thesis_file,
                runtime_mode="trading"
            )

def test_deploy_admission_real_artifact_trading_monitor_only_fails(tmp_path):
    """trading + monitor_only thesis -> should raise PermissionError"""
    thesis_file = tmp_path / "promoted_theses.json"
    content = {
        "schema_version": "promoted_theses_v1",
        "run_id": "test_run",
        "generated_at_utc": "2026-04-27T00:00:00Z",
        "thesis_count": 1,
        "active_thesis_count": 0,
        "pending_thesis_count": 0,
        "theses": [
            {
                "thesis_id": "test_thesis",
                "deployment_state": "monitor_only",
                "timeframe": "5m",
                "primary_event_id": "VOL_SHOCK",
                "evidence": {"sample_size": 100},
                "lineage": {"run_id": "test_run", "candidate_id": "cand_1"},
            }
        ]
    }
    thesis_file.write_text(json.dumps(content))
    
    with patch("project.live.thesis_store.inspect_artifact_trust") as m_trust:
        m_trust.return_value.historical_trust_status = "trusted_under_current_rules"
        with pytest.raises(PermissionError, match="Requires 'live_enabled'"):
            assert_deploy_admission(
                thesis_path=thesis_file,
                runtime_mode="trading"
            )
