from __future__ import annotations

from pathlib import Path

from project.portfolio.engine import PortfolioCapitalDecision
from project.portfolio.admission_policy import AdmissionResult
from project.portfolio.reporting import write_portfolio_decision_trace


def test_write_portfolio_decision_trace(tmp_path: Path) -> None:
    decisions = [
        PortfolioCapitalDecision(
            thesis_id="T1",
            symbol="BTCUSDT",
            family="vol",
            overlap_group_id="OG1",
            requested_notional=10000.0,
            allocated_notional=5000.0,
            risk_multiplier=0.5,
            cluster_multiplier=1.0,
            correlation_adjustment=1.0,
            incubation_state="live",
            admission=AdmissionResult(True, "ok"),
            decision_status="reduced",
            priority_score=1.2,
            available_capacity_notional=5000.0,
            clip_factors=("symbol_cap",),
            reasons=("clip:symbol_cap",),
        )
    ]
    payload = write_portfolio_decision_trace(decisions, tmp_path)
    assert payload["schema_version"] == "portfolio_decision_trace_v1"
    assert (tmp_path / "portfolio_decision_trace.json").exists()
    assert (tmp_path / "portfolio_decision_trace.md").exists()
