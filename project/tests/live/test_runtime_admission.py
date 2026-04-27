import pytest
from unittest.mock import MagicMock
from project.live.runtime_admission import validate_runtime_mode_against_theses

def test_runtime_admission_trading_refuses_research():
    thesis = MagicMock()
    thesis.deployment_state = "monitor_only"
    with pytest.raises(ValueError, match="cannot run in trading mode"):
        validate_runtime_mode_against_theses("trading", [thesis])

def test_runtime_admission_simulation_refuses_research():
    thesis = MagicMock()
    thesis.deployment_state = "promoted"
    with pytest.raises(ValueError, match="cannot run in simulation mode"):
        validate_runtime_mode_against_theses("simulation", [thesis])

def test_runtime_admission_monitor_accepts_research():
    thesis = MagicMock()
    thesis.deployment_state = "monitor_only"
    validate_runtime_mode_against_theses("monitor_only", [thesis]) # Should not raise

def test_runtime_admission_trading_accepts_live_enabled():
    thesis = MagicMock()
    thesis.deployment_state = "live_enabled"
    validate_runtime_mode_against_theses("trading", [thesis]) # Should not raise

def test_runtime_admission_rejects_unknown_mode():
    with pytest.raises(ValueError, match="Unsupported runtime_mode"):
        validate_runtime_mode_against_theses("invalid_mode", [])

def test_runtime_admission_simulation_accepts_paper_states():
    paper_states = ["paper_enabled", "paper_approved", "live_eligible", "live_enabled"]
    for state in paper_states:
        thesis = MagicMock()
        thesis.deployment_state = state
        validate_runtime_mode_against_theses("simulation", [thesis]) # Should not raise
