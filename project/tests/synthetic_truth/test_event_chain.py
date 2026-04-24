from __future__ import annotations

import pytest

from project.synthetic_truth.tools.temporal.event_chain import (
    PRECEDENCE_CHAINS,
    ChainProgress,
    ChainState,
    ChainStep,
    EventChain,
    EventChainEngine,
)


class TestChainStep:
    def test_default_required(self):
        step = ChainStep(event_type="TEST_EVENT")
        assert step.required is True

    def test_with_bounds(self):
        step = ChainStep(event_type="TEST_EVENT", min_bar=10, max_bar=50)
        assert step.min_bar == 10
        assert step.max_bar == 50


class TestChainProgress:
    def test_initial_state(self):
        progress = ChainProgress(chain_id="test", state=ChainState.PENDING)
        assert progress.state == ChainState.PENDING
        assert progress.is_complete is False
        assert progress.progress_pct < 100.0

    def test_completed_state(self):
        progress = ChainProgress(
            chain_id="test",
            state=ChainState.COMPLETED,
            triggered_events=["A", "B"],
            triggered_bars=[10, 20],
        )
        assert progress.is_complete is True
        assert progress.progress_pct == 100.0


class TestEventChain:
    def test_valid_chain(self):
        chain = EventChain(
            name="test",
            chain_id="test",
            steps=[
                ChainStep(event_type="A"),
                ChainStep(event_type="B"),
            ],
        )
        valid, msg = chain.validate()
        assert valid

    def test_invalid_empty_chain(self):
        chain = EventChain(
            name="test",
            chain_id="test",
            steps=[],
        )
        valid, msg = chain.validate()
        assert not valid

    def test_invalid_single_step(self):
        chain = EventChain(
            name="test",
            chain_id="test",
            steps=[ChainStep(event_type="A")],
        )
        valid, msg = chain.validate()
        assert not valid


class TestEventChainEngine:
    def test_process_single_chain(self):
        chain = EventChain(
            name="ab_chain",
            chain_id="ab",
            steps=[
                ChainStep(event_type="EVENT_A"),
                ChainStep(event_type="EVENT_B"),
            ],
        )

        engine = EventChainEngine([chain])

        engine.process_event("EVENT_A", bar_index=10)
        progress = engine.get_chain_progress("ab")

        assert len(progress) == 1
        assert progress[0].current_step == 1
        assert progress[0].state == ChainState.IN_PROGRESS

    def test_complete_chain(self):
        chain = EventChain(
            name="ab_chain",
            chain_id="ab",
            steps=[
                ChainStep(event_type="EVENT_A"),
                ChainStep(event_type="EVENT_B"),
            ],
            min_chain_duration_bars=5,
        )

        engine = EventChainEngine([chain])

        engine.process_event("EVENT_A", bar_index=10)
        engine.process_event("EVENT_B", bar_index=20)

        completed = engine.get_completed_chains()
        assert len(completed) == 1
        assert completed[0].state == ChainState.COMPLETED

    def test_wrong_event_type(self):
        chain = EventChain(
            name="ab_chain",
            chain_id="ab",
            steps=[
                ChainStep(event_type="EVENT_A"),
                ChainStep(event_type="EVENT_B"),
            ],
        )

        engine = EventChainEngine([chain])

        result = engine.process_event("EVENT_C", bar_index=10)

        assert len(result) == 0

    def test_timeout_on_max_bar(self):
        chain = EventChain(
            name="ab_chain",
            chain_id="ab",
            steps=[
                ChainStep(event_type="EVENT_A", max_bar=20),
                ChainStep(event_type="EVENT_B"),
            ],
        )

        engine = EventChainEngine([chain])

        engine.process_event("EVENT_A", bar_index=25)

        progress = engine.get_chain_progress("ab")
        assert progress[0].state == ChainState.TIMEOUT

    def test_precedence_chains(self):
        engine = EventChainEngine(PRECEDENCE_CHAINS)

        assert len(engine.chains) == 2
        assert "seq_liq_vacuum_then_depth_recovery" in engine.chains
        assert "seq_fnd_extreme_then_breakout" in engine.chains


class TestSequentialSyntheticEvents:
    def test_chain_completes_with_sequential_events(self):
        chain = EventChain(
            name="abc_chain",
            chain_id="abc",
            steps=[
                ChainStep(event_type="A"),
                ChainStep(event_type="B"),
                ChainStep(event_type="C"),
            ],
            min_chain_duration_bars=5,
        )

        engine = EventChainEngine([chain])

        engine.process_event("A", bar_index=10)
        engine.process_event("B", bar_index=20)
        engine.process_event("C", bar_index=30)

        completed = engine.get_completed_chains()
        assert len(completed) == 1
        assert len(completed[0].triggered_events) == 3

    def test_chain_fails_on_wrong_sequence(self):
        chain = EventChain(
            name="ab_chain",
            chain_id="ab",
            steps=[
                ChainStep(event_type="A"),
                ChainStep(event_type="B"),
            ],
        )

        engine = EventChainEngine([chain])

        engine.process_event("B", bar_index=10)

        progress = engine.get_chain_progress("ab")
        assert progress[0].current_step == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
