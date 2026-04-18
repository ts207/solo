from __future__ import annotations

import pytest

from project.portfolio.engine import (
    PortfolioCapitalDecision,
    PortfolioDecisionEngine,
    ThesisIntent,
)


def _engine(**kwargs) -> PortfolioDecisionEngine:
    defaults = dict(
        family_budgets={"vol": 50_000.0},
        symbol_caps={"BTCUSDT": 100_000.0},
        max_gross_leverage=1.0,
        target_vol=0.10,
        current_vol=0.10,
        gross_exposure=0.0,
        correlation_limit=1_000_000.0,  # effectively no correlation gate
        max_strategies_per_cluster=3,
    )
    defaults.update(kwargs)
    return PortfolioDecisionEngine(**defaults)


def _intent(**kwargs) -> ThesisIntent:
    defaults = dict(
        thesis_id="T1",
        symbol="BTCUSDT",
        family="vol",
        overlap_group_id="OG1",
        requested_notional=10_000.0,
        support_score=1.0,
    )
    defaults.update(kwargs)
    return ThesisIntent(**defaults)


class TestOverlapGating:
    def test_second_intent_in_same_overlap_group_is_blocked(self):
        engine = _engine()
        i1 = _intent(thesis_id="T1", overlap_group_id="OG1", support_score=2.0)
        i2 = _intent(thesis_id="T2", overlap_group_id="OG1", support_score=1.0)
        decisions = engine.decide([i1, i2])
        assert decisions[0].is_allocated
        assert not decisions[1].is_allocated
        assert any("overlap" in r for r in decisions[1].reasons)

    def test_pre_existing_active_overlap_group_blocks(self):
        engine = _engine()
        intent = _intent(thesis_id="T1", overlap_group_id="OG1")
        decisions = engine.decide([intent], active_overlap_groups={"OG1"})
        assert not decisions[0].is_allocated

    def test_different_overlap_groups_both_allocated(self):
        engine = _engine()
        i1 = _intent(thesis_id="T1", overlap_group_id="OG1")
        i2 = _intent(thesis_id="T2", overlap_group_id="OG2")
        decisions = engine.decide([i1, i2])
        assert all(d.is_allocated for d in decisions)


class TestFamilyBudget:
    def test_family_budget_exhaustion_blocks(self):
        engine = _engine(family_budgets={"vol": 5_000.0})
        intent = _intent(thesis_id="T1", requested_notional=10_000.0)
        decisions = engine.decide([intent], family_exposures={"vol": 5_000.0})
        assert not decisions[0].is_allocated
        assert any("family" in r for r in decisions[0].reasons)

    def test_family_budget_respected_incrementally(self):
        # budget exactly == first notional; after T1 commits, T2 sees exhausted budget
        engine = _engine(family_budgets={"vol": 10_000.0})
        i1 = _intent(thesis_id="T1", overlap_group_id="OG1", requested_notional=10_000.0, support_score=2.0)
        i2 = _intent(thesis_id="T2", overlap_group_id="OG2", requested_notional=10_000.0, support_score=1.0)
        decisions = engine.decide([i1, i2])
        assert decisions[0].is_allocated
        assert not decisions[1].is_allocated


class TestSymbolCap:
    def test_symbol_cap_exhaustion_blocks(self):
        engine = _engine(symbol_caps={"BTCUSDT": 5_000.0})
        intent = _intent()
        decisions = engine.decide([intent], symbol_exposures={"BTCUSDT": 5_000.0})
        assert not decisions[0].is_allocated
        assert any("symbol_cap" in r for r in decisions[0].reasons)


class TestIncubationGate:
    def test_incubating_intent_is_blocked(self):
        engine = _engine()
        intent = _intent(incubation_state="incubating")
        decisions = engine.decide([intent])
        assert not decisions[0].is_allocated
        assert any("incubating" in r for r in decisions[0].reasons)

    def test_live_intent_is_not_blocked_by_incubation(self):
        engine = _engine()
        intent = _intent(incubation_state="live")
        decisions = engine.decide([intent])
        assert decisions[0].is_allocated


class TestClusterThrottle:
    def test_smooth_throttle_reduces_allocation(self):
        engine = _engine(max_strategies_per_cluster=2)
        intent = _intent(cluster_id=7)
        decisions = engine.decide(
            [intent],
            active_cluster_counts={7: 3},
        )
        assert decisions[0].is_allocated
        assert decisions[0].cluster_multiplier < 1.0
        assert decisions[0].allocated_notional < intent.requested_notional


class TestPriorityOrdering:
    def test_higher_support_score_processed_first(self):
        # budget == requested; HIGH wins the budget slot because it has higher support_score
        engine = _engine(family_budgets={"vol": 10_000.0})
        low = _intent(thesis_id="LOW", overlap_group_id="OG1", support_score=0.5)
        high = _intent(thesis_id="HIGH", overlap_group_id="OG2", support_score=2.0)
        decisions = engine.decide([low, high])
        by_id = {d.thesis_id: d for d in decisions}
        assert by_id["HIGH"].is_allocated
        assert not by_id["LOW"].is_allocated


class TestDecisionAudit:
    def test_decision_summary_contains_allocated_status(self):
        engine = _engine()
        intent = _intent()
        d = engine.decide([intent])[0]
        summary = d.summary()
        assert "ALLOCATED" in summary or "BLOCKED" in summary
        assert intent.thesis_id in summary

    def test_blocked_decision_has_zero_allocated(self):
        engine = _engine()
        intent = _intent(incubation_state="incubating")
        d = engine.decide([intent])[0]
        assert d.allocated_notional == 0.0
        assert not d.is_allocated
