from __future__ import annotations

import pytest

from project.live.runtime_trace import (
    ReconciliationStateTransition,
    RuntimeTrace,
    ThesisArbitrationResult,
)


class TestRuntimeTrace:
    def _trace(self, **kwargs) -> RuntimeTrace:
        defaults = dict(
            trace_id="TRC-001",
            run_id="run-abc",
            symbol="BTCUSDT",
            event_type="VOL_SPIKE",
            event_bar_index=100,
            thesis_id="T1",
            template_id="mean_reversion",
            direction="long",
            horizon="24b",
            intent_notional=10_000.0,
            allocated_notional=8_000.0,
            risk_multiplier=0.8,
        )
        defaults.update(kwargs)
        return RuntimeTrace(**defaults)

    def test_was_allocated_true_when_notional_positive(self):
        assert self._trace().was_allocated

    def test_was_allocated_false_when_zero(self):
        assert not self._trace(allocated_notional=0.0).was_allocated

    def test_was_submitted_false_without_order_id(self):
        assert not self._trace().was_submitted

    def test_was_submitted_true_with_order_id(self):
        assert self._trace(oms_order_id="ORD-123").was_submitted

    def test_summary_contains_event_and_thesis(self):
        s = self._trace(oms_order_id="ORD-1", oms_status="filled").summary()
        assert "VOL_SPIKE" in s
        assert "T1" in s
        assert "FILLED" in s

    def test_summary_not_submitted_when_no_order_id(self):
        assert "NOT_SUBMITTED" in self._trace().summary()


class TestThesisArbitrationResult:
    def test_had_candidates_true(self):
        r = ThesisArbitrationResult(
            event_type="VOL_SPIKE",
            symbol="BTCUSDT",
            bar_index=10,
            candidate_thesis_ids=("T1", "T2"),
            selected_thesis_id="T1",
            selection_reason="highest_support",
        )
        assert r.had_candidates
        assert r.was_resolved

    def test_had_candidates_false_empty(self):
        r = ThesisArbitrationResult(
            event_type="VOL_SPIKE",
            symbol="BTCUSDT",
            bar_index=10,
            candidate_thesis_ids=(),
            selected_thesis_id=None,
            selection_reason="no_candidates",
        )
        assert not r.had_candidates
        assert not r.was_resolved


class TestReconciliationStateTransition:
    def _transition(self, **kwargs) -> ReconciliationStateTransition:
        defaults = dict(
            thesis_id="T1",
            symbol="BTCUSDT",
            from_state="pending_entry",
            to_state="active",
            trigger="fill_confirmed",
            bar_index=50,
        )
        defaults.update(kwargs)
        return ReconciliationStateTransition(**defaults)

    def test_valid_transition_accepted(self):
        t = self._transition()
        assert t.is_entry_transition
        assert not t.is_exit_transition

    def test_exit_transition_detected(self):
        t = self._transition(from_state="active", to_state="exited", trigger="stop_hit")
        assert t.is_exit_transition
        assert not t.is_entry_transition

    def test_invalid_from_state_raises(self):
        with pytest.raises(ValueError, match="from_state"):
            self._transition(from_state="nonexistent")

    def test_invalid_to_state_raises(self):
        with pytest.raises(ValueError, match="to_state"):
            self._transition(to_state="nonexistent")
