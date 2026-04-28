"""
Tests for Phase 1.2: future-looking interaction trigger fix (Sprint 1).

Verifies that _materialize_interaction_trigger_columns no longer uses
right_mask.shift(-1) (1-bar forward look) for 'and' and 'exclude' operators.

After the fix:
  "and"     → simultaneous co-occurrence (both signals true on the same bar)
  "exclude" → left fires NOW, right NOT seen in the past lag window
  "confirm" → right fires NOW, left fired in the past lag window (unchanged)
  "or"      → union of current signals (unchanged)

IMPORTANT — lag unit:
  _interaction_lag_steps() interprets raw_lag as MINUTES and divides by
  bar_minutes to get bar steps.  For 5-minute bar data:
    lag=5  → 1 bar step
    lag=30 → 6 bar steps
    lag=60 → 12 bar steps

Uses real registry events (BREAKOUT_TRIGGER + VOL_SPIKE) to avoid registry
validation errors.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from project.core.column_registry import ColumnRegistry
from project.domain.hypotheses import HypothesisSpec, TriggerSpec
from project.events.event_specs import EVENT_REGISTRY_SPECS
from project.research.phase2_search_engine import _materialize_interaction_trigger_columns


# ---------------------------------------------------------------------------
# Column name helpers
# ---------------------------------------------------------------------------

LEFT_EVENT = "BREAKOUT_TRIGGER"
RIGHT_EVENT = "VOL_SPIKE"
LEFT_COL = EVENT_REGISTRY_SPECS[LEFT_EVENT].signal_column
RIGHT_COL = EVENT_REGISTRY_SPECS[RIGHT_EVENT].signal_column

# Lag in minutes for 5-min bar data (each bar = 5 min)
LAG_MINUTES_SHORT = 30   # 6 bar steps
LAG_MINUTES_LONG  = 60   # 12 bar steps


def _make_features(n_rows: int, *, left_bools: list, right_bools: list) -> pd.DataFrame:
    """Synthetic feature table driven by explicit bool lists for left/right events."""
    assert len(left_bools) == n_rows and len(right_bools) == n_rows
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n_rows, freq="5min", tz="UTC"),
            "symbol": ["BTCUSDT"] * n_rows,
            "close": [100.0 + i for i in range(n_rows)],
            LEFT_COL: left_bools,
            RIGHT_COL: right_bools,
        }
    )


def _materialize(df: pd.DataFrame, *, op: str, lag: int) -> list[bool]:
    """Materialise interaction mask and return as a plain bool list."""
    interaction_id = f"INT_TEST_X_{op.upper()}_{lag}M"
    spec = HypothesisSpec(
        trigger=TriggerSpec.interaction(
            interaction_id,
            LEFT_EVENT,
            RIGHT_EVENT,
            op,
            lag=lag,
        ),
        direction="long",
        horizon="12b",
        template_id="continuation",
        entry_lag=1,
    )
    out = _materialize_interaction_trigger_columns(df, [spec])
    col = ColumnRegistry.interaction_cols(interaction_id)[0]
    if col not in out.columns:
        return [False] * len(df)
    return out[col].fillna(False).astype(bool).tolist()


# ---------------------------------------------------------------------------
# "and" operator tests
# ---------------------------------------------------------------------------

class TestAndOperatorNoLookahead:
    """'and' must fire only when BOTH left and right are true on the SAME bar."""

    def test_and_does_not_fire_when_right_is_only_in_the_future(self):
        """
        Left at row 3; right at row 7 (4 bars later).
        Old code: interaction fires at row 3 via shift(-1) lookahead.
        Fixed:    interaction must NOT fire at row 3 — right is not simultaneously true.
        """
        n = 12
        left  = [False, False, False, True,  False, False, False, False, False, False, False, False]
        right = [False, False, False, False, False, False, False, True,  False, False, False, False]
        mask = _materialize(_make_features(n, left_bools=left, right_bools=right),
                            op="and", lag=LAG_MINUTES_LONG)
        assert not mask[3], "AND must not fire at left-only bar when right is only in the future"

    def test_and_fires_when_both_simultaneous(self):
        """Left and right both fire at row 5 → interaction fires at row 5."""
        n = 12
        left  = [False, False, False, False, False, True,  False, False, False, False, False, False]
        right = [False, False, False, False, False, True,  False, False, False, False, False, False]
        mask = _materialize(_make_features(n, left_bools=left, right_bools=right),
                            op="and", lag=LAG_MINUTES_SHORT)
        assert mask[5], "AND must fire when left and right are simultaneously true"

    def test_and_does_not_fire_when_right_only_in_past(self):
        """Left at row 4, right at row 2 (past). Simultaneous AND: row 4 must NOT fire."""
        n = 12
        left  = [False, False, False, False, True,  False, False, False, False, False, False, False]
        right = [False, False, True,  False, False, False, False, False, False, False, False, False]
        mask = _materialize(_make_features(n, left_bools=left, right_bools=right),
                            op="and", lag=LAG_MINUTES_SHORT)
        assert not mask[4], "AND must not fire when right fired in the past but not simultaneously"

    def test_and_all_false_when_events_never_overlap(self):
        """Left and right never co-occur → no 'and' fires anywhere."""
        n = 12
        left  = [True, False, True,  False, True,  False, False, False, False, False, False, False]
        right = [False, True, False, True,  False, True,  False, False, False, False, False, False]
        mask = _materialize(_make_features(n, left_bools=left, right_bools=right),
                            op="and", lag=LAG_MINUTES_SHORT)
        assert not any(mask), "AND interaction should never fire when events never overlap"


# ---------------------------------------------------------------------------
# "exclude" operator tests
# ---------------------------------------------------------------------------

class TestExcludeOperatorNoLookahead:
    """'exclude' fires at left-NOW only when right has NOT fired in the past lag window."""

    def test_exclude_fires_when_right_not_in_past_window(self):
        """
        Left at row 8; right at row 1 (7 bars before, outside lag=30min=6bar window).
        Exclude should fire at row 8.
        """
        n = 12
        left  = [False, False, False, False, False, False, False, False, True,  False, False, False]
        right = [False, True,  False, False, False, False, False, False, False, False, False, False]
        mask = _materialize(_make_features(n, left_bools=left, right_bools=right),
                            op="exclude", lag=LAG_MINUTES_SHORT)  # 6-bar window
        assert mask[8], "EXCLUDE should fire when left fires and right is outside the past lag window"

    def test_exclude_does_not_fire_when_right_in_past_window(self):
        """
        Left at row 8; right at row 5 (3 bars before, inside lag=30min=6bar window).
        Exclude must NOT fire at row 8.
        """
        n = 12
        left  = [False, False, False, False, False, False, False, False, True,  False, False, False]
        right = [False, False, False, False, False, True,  False, False, False, False, False, False]
        mask = _materialize(_make_features(n, left_bools=left, right_bools=right),
                            op="exclude", lag=LAG_MINUTES_SHORT)  # 6-bar window
        assert not mask[8], "EXCLUDE must not fire when right fired within the past lag window"

    def test_exclude_fires_when_right_only_in_future(self):
        """
        Left at row 4; right at row 10 (6 bars ahead, lag=30min=6bar window).
        Old code: exclude did NOT fire (future_right was True via lookahead).
        Fixed:    exclude SHOULD fire (right not in past window).
        """
        n = 14
        left  = [False, False, False, False, True,  False, False, False, False, False, False, False, False, False]
        right = [False, False, False, False, False, False, False, False, False, False, True,  False, False, False]
        mask = _materialize(_make_features(n, left_bools=left, right_bools=right),
                            op="exclude", lag=LAG_MINUTES_SHORT)
        assert mask[4], (
            "EXCLUDE should fire when left fires and right is only in the future "
            "(past-only window, no lookahead)"
        )

    def test_exclude_all_left_fire_when_right_never_fires(self):
        """No right events ever → all left events should fire on exclude."""
        n = 10
        left  = [False, True, False, True, False, True, False, False, False, False]
        right = [False] * n
        mask = _materialize(_make_features(n, left_bools=left, right_bools=right),
                            op="exclude", lag=LAG_MINUTES_SHORT)
        for pos, left_val in enumerate(left):
            if left_val:
                assert mask[pos], f"EXCLUDE should fire at row {pos} when right never fires"


# ---------------------------------------------------------------------------
# "confirm" and "or" operators (unchanged — regression guard)
# ---------------------------------------------------------------------------

class TestConfirmAndOrUnchanged:
    """confirm and or were already safe. These are regression guards for the fix."""

    def test_confirm_fires_when_left_in_past_and_right_now(self):
        """
        Left at row 2; right at row 5 (3 bars later, within lag=30min=6bar window).
        Confirm fires at row 5.
        """
        n = 10
        left  = [False, False, True,  False, False, False, False, False, False, False]
        right = [False, False, False, False, False, True,  False, False, False, False]
        mask = _materialize(_make_features(n, left_bools=left, right_bools=right),
                            op="confirm", lag=LAG_MINUTES_SHORT)
        assert mask[5], "CONFIRM should fire when right fires and left is in the past lag window"

    def test_confirm_does_not_fire_when_left_only_in_future(self):
        """Left at row 8; right at row 3 (left is in the future). Confirm must NOT fire at row 3."""
        n = 12
        left  = [False, False, False, False, False, False, False, False, True,  False, False, False]
        right = [False, False, False, True,  False, False, False, False, False, False, False, False]
        mask = _materialize(_make_features(n, left_bools=left, right_bools=right),
                            op="confirm", lag=LAG_MINUTES_SHORT)
        assert not mask[3], "CONFIRM must not fire when left is only in the future"

    def test_or_fires_at_either_event(self):
        """Left at row 2; right at row 7. OR fires at both bars."""
        n = 10
        left  = [False, False, True,  False, False, False, False, False, False, False]
        right = [False, False, False, False, False, False, False, True,  False, False]
        mask = _materialize(_make_features(n, left_bools=left, right_bools=right),
                            op="or", lag=LAG_MINUTES_SHORT)
        assert mask[2], "OR should fire at left event bar"
        assert mask[7], "OR should fire at right event bar"
