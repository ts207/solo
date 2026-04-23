from __future__ import annotations

from project.portfolio.engine import PortfolioDecisionEngine, ThesisIntent
from project.portfolio.marginal_risk import estimate_marginal_risk, marginal_risk_multiplier


def test_higher_marginal_drawdown_contribution_reduces_multiplier() -> None:
    low = marginal_risk_multiplier(
        estimate_marginal_risk(
            downside_bps=20.0,
            marginal_volatility=0.05,
            marginal_drawdown_contribution=0.02,
        )
    )
    high = marginal_risk_multiplier(
        estimate_marginal_risk(
            downside_bps=20.0,
            marginal_volatility=0.05,
            marginal_drawdown_contribution=0.20,
        )
    )

    assert high < low


def test_portfolio_engine_shrinks_high_marginal_drawdown_trade() -> None:
    engine = PortfolioDecisionEngine(
        family_budgets={"vol": 100_000.0},
        symbol_caps={"BTCUSDT": 100_000.0},
        correlation_limit=100_000.0,
    )
    low_risk = ThesisIntent(
        thesis_id="LOW",
        symbol="BTCUSDT",
        family="vol",
        overlap_group_id="OG1",
        requested_notional=10_000.0,
        support_score=1.0,
        marginal_drawdown_contribution=0.02,
    )
    high_risk = ThesisIntent(
        thesis_id="HIGH",
        symbol="BTCUSDT",
        family="vol",
        overlap_group_id="OG2",
        requested_notional=10_000.0,
        support_score=1.0,
        marginal_drawdown_contribution=0.20,
    )

    decisions = {decision.thesis_id: decision for decision in engine.decide([low_risk, high_risk])}

    assert decisions["HIGH"].allocated_notional < decisions["LOW"].allocated_notional
    assert decisions["HIGH"].marginal_risk_multiplier < decisions["LOW"].marginal_risk_multiplier
