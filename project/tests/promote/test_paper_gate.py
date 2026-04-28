import json
import pytest
from pathlib import Path
from project.promote.paper_gate import evaluate_paper_gate

def test_paper_gate_pass(tmp_path):
    summary = {
        "trade_count": 30,
        "mean_net_bps": 1.0,
        "cumulative_net_bps": 30.0,
        "hit_rate": 0.51,
        "degraded_cost_fraction": 0.1,
    }
    summary_path = tmp_path / "paper_quality_summary.json"
    summary_path.write_text(json.dumps(summary))
    
    result = evaluate_paper_gate(summary_path)
    assert result.status == "pass"
    assert result.eligible_next_state == "paper_approved"
    assert len(result.reason_codes) == 0

def test_paper_gate_fail_low_trades(tmp_path):
    summary = {
        "trade_count": 29,
        "mean_net_bps": 1.0,
        "cumulative_net_bps": 29.0,
        "hit_rate": 0.51,
        "degraded_cost_fraction": 0.1,
    }
    summary_path = tmp_path / "paper_quality_summary.json"
    summary_path.write_text(json.dumps(summary))
    
    result = evaluate_paper_gate(summary_path)
    assert result.status == "fail"
    assert "insufficient_paper_trades" in result.reason_codes

def test_paper_gate_fail_negative_net(tmp_path):
    summary = {
        "trade_count": 30,
        "mean_net_bps": -0.1,
        "cumulative_net_bps": -3.0,
        "hit_rate": 0.51,
        "degraded_cost_fraction": 0.1,
    }
    summary_path = tmp_path / "paper_quality_summary.json"
    summary_path.write_text(json.dumps(summary))
    
    result = evaluate_paper_gate(summary_path)
    assert result.status == "fail"
    assert "nonpositive_mean_net_bps" in result.reason_codes
    assert "nonpositive_cumulative_net_bps" in result.reason_codes

def test_paper_gate_fail_degraded_cost(tmp_path):
    summary = {
        "trade_count": 30,
        "mean_net_bps": 1.0,
        "cumulative_net_bps": 30.0,
        "hit_rate": 0.51,
        "degraded_cost_fraction": 0.21,
    }
    summary_path = tmp_path / "paper_quality_summary.json"
    summary_path.write_text(json.dumps(summary))
    
    result = evaluate_paper_gate(summary_path)
    assert result.status == "fail"
    assert "cost_attribution_degraded" in result.reason_codes

def test_paper_gate_missing_file(tmp_path):
    summary_path = tmp_path / "missing.json"
    result = evaluate_paper_gate(summary_path)
    assert result.status == "fail"
    assert "missing_paper_summary" in result.reason_codes
