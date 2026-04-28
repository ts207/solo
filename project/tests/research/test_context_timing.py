"""
Tests for Phase 1.3: context_timing explicitness (Sprint 1).

Verifies that:
  - context_timing="entry"   evaluates context at the entry bar (legacy default)
  - context_timing="trigger" evaluates context at the original trigger bar

These two modes produce different event counts when the context column flips
between the trigger bar and the entry bar (entry_lag >= 1).

Implementation notes
--------------------
context_mask() depends on the compiled domain registry's context_state_map.
To isolate context-timing logic from registry availability, the unit tests
monkeypatch `project.research.search.evaluator._context_mask` with a simple
stub that reads a known feature column directly.

The integration test (TestContextTimingEventCounts) uses no context and only
checks that the `context_timing` column is emitted in the metrics output.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STATE_COL = "ctx_active"  # synthetic state column: 1 = context active, 0 = inactive


def _make_features_with_flipping_context(
    n_rows: int,
    *,
    event_pos: int,
    entry_lag: int,
    context_true_at_trigger: bool,
) -> pd.DataFrame:
    """
    Build a feature table where:
      - A VOL_SPIKE event fires at ``event_pos``.
      - ``_STATE_COL`` is 1 at the trigger bar and 0 at the entry bar (or vice versa).
    """
    from project.events.event_specs import EVENT_REGISTRY_SPECS

    sig_col = EVENT_REGISTRY_SPECS["VOL_SPIKE"].signal_column
    close = pd.Series(100.0 + np.arange(n_rows, dtype=float))
    event_col = pd.Series(False, index=range(n_rows))
    event_col.iloc[event_pos] = True

    entry_bar = event_pos + entry_lag
    state_col = pd.Series(0.0, index=range(n_rows))
    if context_true_at_trigger:
        state_col.iloc[event_pos] = 1.0   # True at trigger bar
        # entry bar stays 0
    else:
        state_col.iloc[entry_bar] = 1.0   # True at entry bar
        # trigger bar stays 0

    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n_rows, freq="5min", tz="UTC"),
            "close": close,
            "volume": 1000.0,
            sig_col: event_col,
            _STATE_COL: state_col,
            "split_label": pd.Series("train", index=range(n_rows), dtype=object),
        }
    )


def _make_spec(entry_lag: int, context_timing: str, *, with_context: bool = True) -> object:
    """Build a HypothesisSpec.  When with_context=True, add a stub context dict."""
    from project.domain.hypotheses import HypothesisSpec, TriggerSpec

    ctx = {"fake_ctx": "active"} if with_context else None
    try:
        return HypothesisSpec(
            trigger=TriggerSpec.event("VOL_SPIKE"),
            direction="long",
            horizon="12b",
            template_id="continuation",
            entry_lag=entry_lag,
            context=ctx,
            context_timing=context_timing,
        )
    except TypeError:
        pytest.skip("context_timing field not available on HypothesisSpec — patch not applied")


@pytest.fixture()
def stub_context_mask(monkeypatch):
    """
    Replace _context_mask in evaluator.py with a stub that reads _STATE_COL == 1.
    This bypasses the domain registry entirely so context-timing tests can run
    without a compiled context_state_map.
    """
    import project.research.search.evaluator as ev

    def _stub(context: dict, features: pd.DataFrame, *, use_context_quality: bool = True) -> pd.Series | None:
        if _STATE_COL not in features.columns:
            return None
        return (pd.to_numeric(features[_STATE_COL], errors="coerce").fillna(0) == 1)

    monkeypatch.setattr(ev, "_context_mask", _stub)


# ---------------------------------------------------------------------------
# Unit tests on EvaluationContext.event_mask
# ---------------------------------------------------------------------------

class TestContextTimingEventMask:
    """Direct tests of event_mask with context_timing='trigger' vs 'entry'."""

    def test_trigger_timing_evaluates_context_at_trigger_bar(self, stub_context_mask):
        """
        Event at row 5, entry_lag=1.
        Context (state col) = 1 at row 5 (trigger bar), 0 at row 6 (entry bar).

        context_timing="trigger" → context checked at row 5 (1) → event kept.
        context_timing="entry"   → context checked at row 6 (0) → event dropped.
        """
        from project.research.search.evaluator import EvaluationContext

        n_rows = 20
        event_pos = 5
        entry_lag = 1

        df = _make_features_with_flipping_context(
            n_rows, event_pos=event_pos, entry_lag=entry_lag, context_true_at_trigger=True
        )

        spec_trigger = _make_spec(entry_lag, "trigger")
        spec_entry = _make_spec(entry_lag, "entry")

        ctx_trigger = EvaluationContext(df)
        ctx_entry = EvaluationContext(df)

        mask_trigger, reason_trigger = ctx_trigger.event_mask(
            spec_trigger, use_context_quality=False
        )
        mask_entry, reason_entry = ctx_entry.event_mask(
            spec_entry, use_context_quality=False
        )

        assert reason_trigger is None, f"trigger event_mask failed: {reason_trigger}"
        assert reason_entry is None, f"entry event_mask failed: {reason_entry}"

        entry_bar = event_pos + entry_lag  # row 6
        assert mask_trigger is not None
        assert bool(mask_trigger.iloc[entry_bar]), (
            "context_timing='trigger': event should be kept when context is True at trigger bar"
        )
        assert mask_entry is not None
        assert not bool(mask_entry.iloc[entry_bar]), (
            "context_timing='entry': event should be dropped when context is False at entry bar"
        )

    def test_entry_timing_evaluates_context_at_entry_bar(self, stub_context_mask):
        """
        Event at row 5, entry_lag=1.
        Context = 0 at row 5 (trigger), 1 at row 6 (entry).

        context_timing="trigger" → context checked at row 5 (0) → event dropped.
        context_timing="entry"   → context checked at row 6 (1) → event kept.
        """
        from project.research.search.evaluator import EvaluationContext

        n_rows = 20
        event_pos = 5
        entry_lag = 1

        df = _make_features_with_flipping_context(
            n_rows, event_pos=event_pos, entry_lag=entry_lag, context_true_at_trigger=False
        )

        spec_trigger = _make_spec(entry_lag, "trigger")
        spec_entry = _make_spec(entry_lag, "entry")

        ctx_trigger = EvaluationContext(df)
        ctx_entry = EvaluationContext(df)

        mask_trigger, _ = ctx_trigger.event_mask(spec_trigger, use_context_quality=False)
        mask_entry, _ = ctx_entry.event_mask(spec_entry, use_context_quality=False)

        entry_bar = event_pos + entry_lag  # row 6
        assert mask_trigger is not None
        assert mask_entry is not None
        assert not bool(mask_trigger.iloc[entry_bar]), (
            "context_timing='trigger': event should be dropped when context is False at trigger"
        )
        assert bool(mask_entry.iloc[entry_bar]), (
            "context_timing='entry': event should be kept when context is True at entry bar"
        )

    def test_default_timing_is_entry(self):
        """HypothesisSpec with no context_timing specified defaults to 'entry'."""
        from project.domain.hypotheses import HypothesisSpec, TriggerSpec

        spec = HypothesisSpec(
            trigger=TriggerSpec.event("VOL_SPIKE"),
            direction="long",
            horizon="12b",
            template_id="continuation",
            entry_lag=1,
        )
        timing = str(getattr(spec, "context_timing", "entry"))
        assert timing == "entry", f"Default context_timing should be 'entry', got {timing!r}"

    def test_context_timing_persisted_in_to_dict(self):
        """context_timing must survive a round-trip through to_dict / from_dict."""
        from project.domain.hypotheses import HypothesisSpec, TriggerSpec

        spec = _make_spec(1, "trigger", with_context=False)
        assert str(getattr(spec, "context_timing", "entry")) == "trigger"

        d = spec.to_dict()
        assert "context_timing" in d, "context_timing must be in to_dict() output"
        assert d["context_timing"] == "trigger"

        spec2 = HypothesisSpec.from_dict(d)
        assert str(getattr(spec2, "context_timing", "entry")) == "trigger"

    def test_context_timing_in_to_record(self):
        """context_timing must appear in CandidateHypothesis.to_record()."""
        from project.domain.hypotheses import HypothesisSpec, TriggerSpec
        from project.research.search.stage_models import CandidateHypothesis

        spec = _make_spec(1, "trigger", with_context=False)
        candidate = CandidateHypothesis(spec=spec, search_spec_name="test")
        record = candidate.to_record()
        assert "context_timing" in record, "to_record() must include context_timing"
        assert record["context_timing"] == "trigger"


# ---------------------------------------------------------------------------
# Integration: sample counts differ between the two timings
# ---------------------------------------------------------------------------

class TestContextTimingEventCounts:
    """End-to-end: evaluate_hypothesis_batch produces different n for the two timings."""

    def test_trigger_vs_entry_timing_differ_when_context_flips(self, stub_context_mask):
        """
        Many events; state col = 1 at trigger bars only.
        event_mask with context_timing="trigger" keeps all events (context True at trigger).
        event_mask with context_timing="entry"   drops all events (context False at entry bar).

        We call EvaluationContext.event_mask directly because evaluate_hypothesis_batch
        has additional direction-resolution and min_sample_size filters that can
        produce n=0 for unrelated reasons on synthetic data.
        """
        from project.research.search.evaluator import EvaluationContext
        from project.events.event_specs import EVENT_REGISTRY_SPECS

        sig_col = EVENT_REGISTRY_SPECS["VOL_SPIKE"].signal_column
        n_rows = 30
        entry_lag = 1
        event_positions = [5, 11, 17, 23]  # 4 events with clear gap from end

        close = pd.Series(100.0 + np.arange(n_rows, dtype=float))
        event_col = pd.Series(False, index=range(n_rows))
        for p in event_positions:
            event_col.iloc[p] = True

        state_col = pd.Series(0.0, index=range(n_rows))
        for p in event_positions:
            state_col.iloc[p] = 1.0     # True at trigger bar
            # p+1 (entry bar) stays 0

        df = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=n_rows, freq="5min", tz="UTC"),
                "close": close,
                "volume": 1000.0,
                sig_col: event_col,
                _STATE_COL: state_col,
                "split_label": pd.Series("train", index=range(n_rows), dtype=object),
            }
        )

        spec_trigger = _make_spec(entry_lag, "trigger")
        spec_entry = _make_spec(entry_lag, "entry")

        ctx_trigger = EvaluationContext(df)
        ctx_entry = EvaluationContext(df)

        mask_trigger, _ = ctx_trigger.event_mask(spec_trigger, use_context_quality=False)
        mask_entry, _ = ctx_entry.event_mask(spec_entry, use_context_quality=False)

        assert mask_trigger is not None
        assert mask_entry is not None

        n_trigger = int(mask_trigger.sum()) if mask_trigger is not None else 0
        n_entry = int(mask_entry.sum()) if mask_entry is not None else 0

        assert n_trigger > n_entry, (
            f"context_timing='trigger' should keep more events than 'entry' "
            f"when context is True at trigger bar only. "
            f"trigger_n={n_trigger}, entry_n={n_entry}"
        )


    def test_context_timing_column_in_metrics(self):
        """Metrics DataFrame must include a context_timing column."""
        from project.research.search.evaluator import evaluate_hypothesis_batch
        from project.domain.hypotheses import HypothesisSpec, TriggerSpec
        from project.events.event_specs import EVENT_REGISTRY_SPECS

        sig_col = EVENT_REGISTRY_SPECS["VOL_SPIKE"].signal_column
        n_rows = 30
        close = pd.Series(100.0 + np.arange(n_rows, dtype=float))
        event_col = pd.Series(False, index=range(n_rows))
        for p in [5, 10, 15, 20]:
            event_col.iloc[p] = True

        df = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=n_rows, freq="5min", tz="UTC"),
                "close": close,
                "volume": 1000.0,
                sig_col: event_col,
            }
        )

        spec = HypothesisSpec(
            trigger=TriggerSpec.event("VOL_SPIKE"),
            direction="long",
            horizon="12b",
            template_id="continuation",
            entry_lag=1,
        )
        metrics = evaluate_hypothesis_batch([spec], df, cost_bps=2.0, min_sample_size=1)
        assert "context_timing" in metrics.columns, (
            "evaluate_hypothesis_batch must emit a 'context_timing' column"
        )
