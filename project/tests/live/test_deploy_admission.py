import pytest
from pathlib import Path
from project.live.deploy_admission import assert_deploy_admission

def test_deploy_admission_monitor_only_allowed():
    """monitor_only state + runtime_mode=monitor_only -> pass"""
    # Should not raise
    assert_deploy_admission(
        thesis_state="monitor_only",
        runtime_mode="monitor_only",
        deployment_ready=False
    )

def test_deploy_admission_promoted_simulation_blocked():
    """promoted state + runtime_mode=simulation + not ready -> fail"""
    with pytest.raises(PermissionError, match="Simulation mode blocked"):
        assert_deploy_admission(
            thesis_state="promoted",
            runtime_mode="simulation",
            deployment_ready=False
        )

def test_deploy_admission_trading_blocked_for_all_non_live():
    """Any non-live_enabled state + runtime_mode=trading -> fail"""
    for state in ["monitor_only", "paper_only", "promoted", "paper_enabled"]:
        with pytest.raises(PermissionError, match="Trading mode blocked"):
            assert_deploy_admission(
                thesis_state=state,
                runtime_mode="trading",
                deployment_ready=True # Even if ready, needs live_enabled state
            )

def test_deploy_admission_paper_enabled_simulation_allowed():
    """paper_enabled + simulation -> pass (even if not deployment_ready by monitor)"""
    # Deployment ready usually refers to live readiness. 
    # For paper, we might allow it if state is paper_enabled.
    assert_deploy_admission(
        thesis_state="paper_enabled",
        runtime_mode="simulation",
        deployment_ready=False
    )
