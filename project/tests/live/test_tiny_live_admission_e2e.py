import json
import pytest
from pathlib import Path
from project.live.deploy_admission import assert_deploy_admission

def create_artifact_chain(tmp_path: Path, run_id: str, thesis_id: str):
    # 1. Promoted Theses
    thesis_dir = tmp_path / "live" / "theses" / run_id
    thesis_dir.mkdir(parents=True)
    thesis_path = thesis_dir / "promoted_theses.json"
    thesis_path.write_text(json.dumps({
        "schema_version": "promoted_theses_v1",
        "run_id": run_id,
        "generated_at_utc": "2026-04-27T00:00:00Z",
        "thesis_count": 1,
        "active_thesis_count": 1,
        "pending_thesis_count": 0,
        "theses": [
            {
                "thesis_id": thesis_id,
                "promotion_class": "production_promoted",
                "deployment_state": "live_enabled",
                "deployment_mode_allowed": "live_enabled",
                "status": "active",
                "timeframe": "5m",
                "primary_event_id": "test_event",
                "live_approval": {
                    "live_approval_status": "approved",
                    "approved_by": "operator",
                    "approved_at": "2026-04-27T00:00:00Z",
                    "risk_profile_id": "tiny_live_v1"
                },
                "cap_profile": {
                    "max_notional": 50.0,
                    "max_position_notional": 50.0,
                    "max_daily_loss": 10.0,
                    "max_active_orders": 3,
                    "max_active_positions": 1
                },
                "evidence": {
                    "metrics": {},
                    "equity_curve": [],
                    "trade_log": [],
                    "sample_size": 100
                },
                "lineage": {
                    "run_id": run_id,
                    "program_id": "p1",
                    "campaign_id": "c1",
                    "candidate_id": "cand1"
                }
            }
        ]
    }))

    # 2. Forward Confirmation
    fc_dir = tmp_path / "reports" / "validation" / run_id
    fc_dir.mkdir(parents=True)
    (fc_dir / "forward_confirmation.json").write_text(json.dumps({
        "method": "oos_frozen_thesis_replay_v1",
        "metrics": {
            "status": "success",
            "event_count": 50,
            "mean_return_net_bps": 5.0,
            "t_stat_net": 2.5
        }
    }))

    # 3. Paper Summary
    paper_dir = tmp_path / "reports" / "paper" / thesis_id
    paper_dir.mkdir(parents=True)
    (paper_dir / "paper_quality_summary.json").write_text(json.dumps({
        "trade_count": 35,
        "mean_net_bps": 10.0,
        "cumulative_net_bps": 350.0,
        "hit_rate": 0.60,
        "degraded_cost_fraction": 0.05,
        "paper_gate_ready": True,
        "max_drawdown_bps": 100.0
    }))

    # 4. Live Approval
    approval_dir = tmp_path / "reports" / "approval" / thesis_id
    approval_dir.mkdir(parents=True)
    (approval_dir / "live_approval.json").write_text(json.dumps({
        "schema_version": "live_approval_v1",
        "thesis_id": thesis_id,
        "approved_state": "live_enabled",
        "approved_by": "operator",
        "approved_at_utc": "2026-04-27T00:00:00Z",
        "cap_profile_id": "tiny_live_v1",
        "risk_acknowledgement": True
    }))

    # 5. Monitor Report
    monitor_path = tmp_path / "monitor.json"
    monitor_path.write_text(json.dumps({
        "deployment_ready": True
    }))

    return thesis_path, monitor_path

def test_tiny_live_admission_e2e_pass(tmp_path):
    run_id = "rehearsal_run"
    thesis_id = "rehearsal_thesis"
    thesis_path, monitor_path = create_artifact_chain(tmp_path, run_id, thesis_id)
    
    # This should pass without raising
    assert_deploy_admission(
        thesis_path=thesis_path,
        runtime_mode="trading",
        monitor_report_path=monitor_path,
        data_root=tmp_path
    )

def test_tiny_live_admission_e2e_fail_missing_approval(tmp_path):
    run_id = "rehearsal_run"
    thesis_id = "rehearsal_thesis"
    thesis_path, monitor_path = create_artifact_chain(tmp_path, run_id, thesis_id)
    
    # Remove approval
    approval_path = tmp_path / "reports" / "approval" / thesis_id / "live_approval.json"
    approval_path.unlink()
    
    with pytest.raises(PermissionError, match="live approval missing"):
        assert_deploy_admission(
            thesis_path=thesis_path,
            runtime_mode="trading",
            monitor_report_path=monitor_path,
            data_root=tmp_path
        )

def test_tiny_live_admission_e2e_fail_exceed_caps(tmp_path):
    run_id = "rehearsal_run"
    thesis_id = "rehearsal_thesis"
    thesis_path, monitor_path = create_artifact_chain(tmp_path, run_id, thesis_id)
    
    # Update thesis with excessive caps
    data = json.loads(thesis_path.read_text())
    data["theses"][0]["cap_profile"]["max_notional"] = 500.0  # tiny_live_v1 limit is 50.0
    thesis_path.write_text(json.dumps(data))
    
    with pytest.raises(PermissionError, match="cap profile violates live approval"):
        assert_deploy_admission(
            thesis_path=thesis_path,
            runtime_mode="trading",
            monitor_report_path=monitor_path,
            data_root=tmp_path
        )
