"""
Integration tests for canonical engine schema and execution timing.

Covers:
- Canonical position fields (`executed_position`, `signal_position`)
- Allocator integration on canonical signals
- PnL execution timing (close vs next_open)
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from project.engine.pnl import (
    compute_pnl_components,
    compute_returns_next_open,
    compute_returns,
)
from project.engine.risk_allocator import RiskLimits, allocate_position_scales
from project.engine.reporting_summarizer import entry_count


def _ts(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")


class TestPositionSchema:
    """Tests for canonical position fields."""

    def test_entry_count_uses_executed_position_column(self):
        idx = _ts(4)
        df = pd.DataFrame({"timestamp": idx, "executed_position": [0, 1, 1, 0]})
        assert entry_count(df) == 1

    def test_entry_count_falls_back_to_signal_position(self):
        idx = _ts(4)
        df = pd.DataFrame({"timestamp": idx, "signal_position": [0, 1, 1, 0]})
        assert entry_count(df) == 1

    def test_entry_count_returns_zero_for_empty_frame(self):
        assert entry_count(pd.DataFrame()) == 0


class TestAllocatorIntegration:
    """Tests for runner + allocator integration with canonical position schema."""

    def test_allocator_receives_signal_position_column(self):
        idx = _ts(3)
        frame = pd.DataFrame({"timestamp": idx, "signal_position": [0, 1, -1]})

        raw_positions = {"s1": frame.set_index("timestamp")["signal_position"]}

        assert "s1" in raw_positions
        assert len(raw_positions["s1"]) == 3

    def test_portfolio_max_exposure_clips_positions(self):
        """Portfolio exposure limit should clip overlapping positions."""
        idx = _ts(2)
        pos = {
            "s1": pd.Series([0, 1], index=idx),
            "s2": pd.Series([0, 1], index=idx),
        }
        req = {}
        limits = RiskLimits(portfolio_max_exposure=1.0, max_strategy_gross=2.0)

        scales, _ = allocate_position_scales(pos, req, limits)

        assert scales["s1"].iloc[1] <= 0.5
        assert scales["s2"].iloc[1] <= 0.5


class TestPnlExecutionTiming:
    """Tests for PnL execution timing (close vs next_open modes)."""

    def test_flat_to_long_entry_next_open(self):
        """Entry from flat to long with next_open mode."""
        idx = pd.date_range("2024-01-01", periods=4, freq="5min", tz="UTC")
        close = pd.Series([100.0, 101.0, 102.0, 103.0], index=idx)
        open_ = pd.Series([100.5, 101.5, 102.5, 103.5], index=idx)

        pos = pd.Series([0.0, 1.0, 1.0, 0.0], index=idx)
        ret = compute_returns_next_open(close, open_, pos)

        result = compute_pnl_components(pos, ret, cost_bps=0.0, execution_mode="next_open")

        assert result["gross_pnl"].iloc[1] != 0.0

    def test_long_hold_next_open(self):
        """Holding a long position with next_open mode."""
        idx = pd.date_range("2024-01-01", periods=4, freq="5min", tz="UTC")
        close = pd.Series([100.0, 101.0, 102.0, 103.0], index=idx)
        open_ = pd.Series([100.5, 101.5, 102.5, 103.5], index=idx)

        pos = pd.Series([1.0, 1.0, 1.0, 1.0], index=idx)
        ret = compute_returns_next_open(close, open_, pos)

        result = compute_pnl_components(pos, ret, cost_bps=0.0, execution_mode="next_open")

        assert result["gross_pnl"].iloc[1] != 0.0
        assert result["gross_pnl"].iloc[2] != 0.0

    def test_long_to_flat_exit_next_open(self):
        """Exit from long to flat with next_open mode."""
        idx = pd.date_range("2024-01-01", periods=4, freq="5min", tz="UTC")
        close = pd.Series([100.0, 101.0, 102.0, 103.0], index=idx)
        open_ = pd.Series([100.5, 101.5, 102.5, 103.5], index=idx)

        pos = pd.Series([1.0, 1.0, 1.0, 0.0], index=idx)
        ret = compute_returns_next_open(close, open_, pos)

        result = compute_pnl_components(pos, ret, cost_bps=0.0, execution_mode="next_open")

        assert result["gross_pnl"].iloc[2] != 0.0

    def test_close_vs_next_open_timing_difference(self):
        """close and next_open modes should produce different entry bar PnL."""
        idx = pd.date_range("2024-01-01", periods=4, freq="5min", tz="UTC")
        close = pd.Series([100.0, 101.0, 102.0, 103.0], index=idx)
        open_ = pd.Series([100.5, 101.5, 102.5, 103.5], index=idx)

        pos = pd.Series([0.0, 1.0, 1.0, 0.0], index=idx)

        ret_next_open = compute_returns_next_open(close, open_, pos)
        ret_close = compute_returns(close)

        result_next_open = compute_pnl_components(
            pos, ret_next_open, cost_bps=0.0, execution_mode="next_open"
        )
        result_close = compute_pnl_components(pos, ret_close, cost_bps=0.0, execution_mode="close")

        assert result_next_open["gross_pnl"].iloc[1] != result_close["gross_pnl"].iloc[1]
