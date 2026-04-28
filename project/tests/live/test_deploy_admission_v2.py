import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from project.live.deploy_admission import assert_deploy_admission

@pytest.fixture
def mock_thesis_store():
    with patch("project.live.deploy_admission.ThesisStore") as mock:
        store = MagicMock()
        mock.from_path.return_value = store
        yield store

def test_admission_trading_all_pass(tmp_path, mock_thesis_store):
    thesis_id = "t1"
    run_id = "r1"
    
    thesis = MagicMock()
    thesis.thesis_id = thesis_id
    thesis.deployment_state = "live_enabled"
    thesis.lineage.run_id = run_id
    mock_thesis_store.all.return_value = [thesis]
    
    # Forward confirmation
    fc_dir = tmp_path / "reports" / "validation" / run_id
    fc_dir.mkdir(parents=True)
    (fc_dir / "forward_confirmation.json").write_text(json.dumps({
        "method": "oos_frozen_thesis_replay_v1"
    }))
    
    # Paper summary
    paper_dir = tmp_path / "reports" / "paper" / thesis_id
    paper_dir.mkdir(parents=True)
    (paper_dir / "paper_quality_summary.json").write_text(json.dumps({
        "trade_count": 30,
        "mean_net_bps": 1.0,
        "cumulative_net_bps": 30.0,
        "hit_rate": 0.51,
        "degraded_cost_fraction": 0.1
    }))
    
    # Monitor report
    monitor_path = tmp_path / "monitor.json"
    monitor_path.write_text(json.dumps({"deployment_ready": True}))
    
    assert_deploy_admission(
        thesis_path=Path("dummy.json"),
        runtime_mode="trading",
        monitor_report_path=monitor_path,
        data_root=tmp_path
    )

def test_admission_trading_fail_no_fc(tmp_path, mock_thesis_store):
    thesis_id = "t1"
    run_id = "r1"
    
    thesis = MagicMock()
    thesis.thesis_id = thesis_id
    thesis.deployment_state = "live_enabled"
    thesis.lineage.run_id = run_id
    mock_thesis_store.all.return_value = [thesis]
    
    monitor_path = tmp_path / "monitor.json"
    monitor_path.write_text(json.dumps({"deployment_ready": True}))
    
    with pytest.raises(PermissionError, match="forward confirmation missing"):
        assert_deploy_admission(
            thesis_path=Path("dummy.json"),
            runtime_mode="trading",
            monitor_report_path=monitor_path,
            data_root=tmp_path
        )

def test_admission_trading_fail_gate(tmp_path, mock_thesis_store):
    thesis_id = "t1"
    run_id = "r1"
    
    thesis = MagicMock()
    thesis.thesis_id = thesis_id
    thesis.deployment_state = "live_enabled"
    thesis.lineage.run_id = run_id
    mock_thesis_store.all.return_value = [thesis]
    
    # Forward confirmation
    fc_dir = tmp_path / "reports" / "validation" / run_id
    fc_dir.mkdir(parents=True)
    (fc_dir / "forward_confirmation.json").write_text(json.dumps({
        "method": "oos_frozen_thesis_replay_v1"
    }))
    
    # Failing Paper summary
    paper_dir = tmp_path / "reports" / "paper" / thesis_id
    paper_dir.mkdir(parents=True)
    (paper_dir / "paper_quality_summary.json").write_text(json.dumps({
        "trade_count": 10, # Too low
    }))
    
    monitor_path = tmp_path / "monitor.json"
    monitor_path.write_text(json.dumps({"deployment_ready": True}))
    
    with pytest.raises(PermissionError, match="paper gate failed"):
        assert_deploy_admission(
            thesis_path=Path("dummy.json"),
            runtime_mode="trading",
            monitor_report_path=monitor_path,
            data_root=tmp_path
        )
