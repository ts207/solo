import subprocess
from pathlib import Path

from project.research.campaign_controller import CampaignConfig, CampaignController


def test_scan_trigger_types_default_includes_all():
    config = CampaignConfig(program_id="test")
    assert "STATE" in config.scan_trigger_types
    assert "TRANSITION" in config.scan_trigger_types
    assert "FEATURE_PREDICATE" in config.scan_trigger_types
    assert "SEQUENCE" in config.scan_trigger_types
    assert "INTERACTION" in config.scan_trigger_types

def test_context_conditioning_enabled_by_default():
    config = CampaignConfig(program_id="test")
    assert config.enable_context_conditioning is True
    assert config.proposal_context_dimensions == ["vol_regime", "carry_state"]

def test_mi_scan_enabled_by_default():
    config = CampaignConfig(program_id="test")
    assert config.auto_run_mi_scan is True

def test_max_runs_adequate_default():
    config = CampaignConfig(program_id="test")
    assert config.max_runs >= 50

def test_execute_pipeline_includes_memory_update(monkeypatch):
    """_execute_pipeline cmd must include --run_campaign_memory_update 1"""
    calls = []
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: calls.append(cmd))
    config = CampaignConfig(program_id="test")
    # Use dummy paths for testing
    ctrl = CampaignController(config, Path("/tmp"), Path("/tmp"))
    cfg_path = Path("/tmp/cfg.yaml")
    cfg_path.write_text("instrument_scope: {}", encoding="utf-8")
    try:
        ctrl._execute_pipeline(cfg_path, "run_test_1")
    except Exception:
        pass
    assert calls, "subprocess.run was not called"
    cmd_str = " ".join(str(x) for x in calls[0])
    assert "--run_campaign_memory_update" in cmd_str
    assert "1" in cmd_str
    assert "--program_id" in cmd_str
    assert "test" in cmd_str
