from __future__ import annotations

import pytest

from project.live.risk import RiskEnforcer, RuntimeRiskCaps


def test_poor_realized_execution_quality_forces_lower_risk_budget() -> None:
    enforcer = RiskEnforcer(
        RuntimeRiskCaps(
            slippage_budget_bps=4.0,
            min_fill_rate=0.80,
            reject_on_breach=True,
        )
    )

    notional, breach = enforcer.check_and_apply_caps(
        thesis_id="T1",
        symbol="BTCUSDT",
        family="VOL",
        attempted_notional=10_000.0,
        portfolio_state={
            "execution_quality": {
                "realized_slippage_bps": 8.0,
                "fill_rate": 0.50,
            }
        },
        active_thesis_ids=[],
        timestamp="2026-04-19T12:00:00Z",
    )

    assert notional == pytest.approx(5_000.0)
    assert breach is not None
    assert breach.cap_type == "execution_quality"
    assert breach.action == "clipped"


def test_good_execution_quality_leaves_risk_budget_unchanged() -> None:
    enforcer = RiskEnforcer(
        RuntimeRiskCaps(
            slippage_budget_bps=4.0,
            min_fill_rate=0.80,
            reject_on_breach=True,
        )
    )

    notional, breach = enforcer.check_and_apply_caps(
        thesis_id="T1",
        symbol="BTCUSDT",
        family="VOL",
        attempted_notional=10_000.0,
        portfolio_state={
            "execution_quality": {
                "realized_slippage_bps": 2.0,
                "fill_rate": 0.95,
            }
        },
        active_thesis_ids=[],
        timestamp="2026-04-19T12:00:00Z",
    )

    assert notional == 10_000.0
    assert breach is None
