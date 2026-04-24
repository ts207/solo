from __future__ import annotations

from project.live.risk import RiskEnforcer, RuntimeRiskCaps
from project.portfolio.admission_policy import PortfolioAdmissionPolicy


def test_family_budget_rejection():
    # Set a specific budget for TREND family
    caps = RuntimeRiskCaps(per_family_caps={"TREND": 1000.0}, reject_on_breach=True)
    enforcer = RiskEnforcer(caps)

    # Current state has 900 USD in TREND
    portfolio_state = {"family_exposures": {"TREND": 900.0}}

    # Attempting 200 more USD should breach (1100 > 1000)
    val, breach = enforcer.check_and_apply_caps(
        thesis_id="T1",
        symbol="BTCUSDT",
        family="TREND",
        attempted_notional=200.0,
        portfolio_state=portfolio_state,
        active_thesis_ids=[],
        timestamp="2026-04-12T12:00:00Z",
    )

    assert val == 0.0
    assert breach is not None
    assert breach.cap_type == "per_family_notional"
    assert breach.cap_value == 1000.0
    assert breach.attempted_value == 1100.0

def test_family_budget_clipping():
    # Set a specific budget for TREND family, but don't reject (clip instead)
    caps = RuntimeRiskCaps(per_family_caps={"TREND": 1000.0}, reject_on_breach=False)
    enforcer = RiskEnforcer(caps)

    # Current state has 900 USD in TREND
    portfolio_state = {"family_exposures": {"TREND": 900.0}}

    # Attempting 200 more USD should be clipped to 100 (900 + 100 = 1000)
    val, breach = enforcer.check_and_apply_caps(
        thesis_id="T1",
        symbol="BTCUSDT",
        family="TREND",
        attempted_notional=200.0,
        portfolio_state=portfolio_state,
        active_thesis_ids=[],
        timestamp="2026-04-12T12:00:00Z",
    )

    assert val == 100.0
    assert breach is not None
    assert breach.cap_type == "per_family_notional"
    assert breach.action == "clipped"

def test_portfolio_policy_family_admissibility():
    policy = PortfolioAdmissionPolicy(family_budgets={"TREND": 1000.0})

    # Case 1: Under budget
    res = policy.is_family_admissible("TREND", {"TREND": 500.0})
    assert res.admissible is True
    assert res.reason == "family_budget_available"

    # Case 2: At/Over budget
    res = policy.is_family_admissible("TREND", {"TREND": 1000.0})
    assert res.admissible is False
    assert "family_budget_exhausted" in res.reason

    # Case 3: Different family (no limit)
    res = policy.is_family_admissible("MEAN_REVERSION", {"TREND": 1000.0})
    assert res.admissible is True
    assert res.reason == "no_family_budget_limit"

def test_global_family_cap_still_applies():
    # Per-family budget is 2000, but global family cap is 1000
    caps = RuntimeRiskCaps(
        max_family_exposure=1000.0,
        per_family_caps={"TREND": 2000.0},
        reject_on_breach=True
    )
    enforcer = RiskEnforcer(caps)

    portfolio_state = {"family_exposures": {"TREND": 900.0}}

    # Attempting 200 more (Total 1100) -> global cap breach (1100 > 1000)
    # Global cap should be checked after specific per-family budget (which is 2000)
    val, breach = enforcer.check_and_apply_caps(
        thesis_id="T1",
        symbol="BTCUSDT",
        family="TREND",
        attempted_notional=200.0,
        portfolio_state=portfolio_state,
        active_thesis_ids=[],
        timestamp="2026-04-12T12:00:00Z",
    )

    assert val == 0.0
    assert breach is not None
    assert breach.cap_type == "family" # global family cap
