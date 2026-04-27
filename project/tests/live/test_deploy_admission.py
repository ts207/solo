import pytest
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

def test_deploy_admission_trading_allowed_for_live_enabled_ready(mock_store, mock_thesis):
    """live_enabled + trading + deployment_ready=True -> pass"""
    mock_thesis.deployment_state = "live_enabled"
    with patch("project.live.deploy_admission.ThesisStore.from_path", return_value=mock_store):
        with patch("project.live.deploy_admission.json.loads") as m_json:
            m_json.return_value = {"deployment_ready": True}
            with patch("pathlib.Path.exists", return_value=True):
                with patch("pathlib.Path.read_text", return_value="{}"):
                    assert_deploy_admission(
                        thesis_path=Path("dummy.json"),
                        runtime_mode="trading",
                        monitor_report_path=Path("report.json")
                    )

def test_deploy_admission_trading_blocked_for_live_enabled_not_ready(mock_store, mock_thesis):
    """live_enabled + trading + deployment_ready=False -> fail"""
    mock_thesis.deployment_state = "live_enabled"
    with patch("project.live.deploy_admission.ThesisStore.from_path", return_value=mock_store):
        with patch("project.live.deploy_admission.json.loads") as m_json:
            m_json.return_value = {"deployment_ready": False}
            with patch("pathlib.Path.exists", return_value=True):
                with patch("pathlib.Path.read_text", return_value="{}"):
                    with pytest.raises(PermissionError, match="monitor report deployment_ready=False"):
                        assert_deploy_admission(
                            thesis_path=Path("dummy.json"),
                            runtime_mode="trading",
                            monitor_report_path=Path("report.json")
                        )

def test_deploy_admission_passes_through_thesis_store_errors():
    """Verify that ThesisStore (and thus DeploymentGate) errors are propagated"""
    with patch("project.live.deploy_admission.ThesisStore.from_path", side_effect=RuntimeError("Gate failure")):
        with pytest.raises(RuntimeError, match="Gate failure"):
            assert_deploy_admission(
                thesis_path=Path("dummy.json"),
                runtime_mode="monitor_only"
            )
