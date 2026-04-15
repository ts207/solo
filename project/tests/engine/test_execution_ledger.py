from __future__ import annotations

import pandas as pd
import pytest

from project.engine.pnl import build_execution_state, compute_pnl_ledger


def _ts(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")


def test_close_mode_uses_prior_target_as_executed_position() -> None:
    idx = _ts(4)
    close = pd.Series([100.0, 101.0, 103.0, 104.0], index=idx)
    target = pd.Series([0.0, 1.0, 1.0, 0.0], index=idx)

    state = build_execution_state(target, close, execution_mode="close")

    assert state["executed_position"].tolist() == [0.0, 0.0, 1.0, 1.0]
    assert state["prior_executed_position"].tolist() == [0.0, 0.0, 0.0, 1.0]

    ledger = compute_pnl_ledger(target, close, execution_mode="close", cost_bps=0.0)
    assert ledger["gross_pnl"].iloc[2] == pytest.approx(103.0 / 101.0 - 1.0)
    assert ledger["gross_pnl"].iloc[1] == pytest.approx(0.0)


def test_next_open_entry_bar_only_accrues_open_to_close_leg() -> None:
    idx = _ts(4)
    close = pd.Series([100.0, 101.0, 103.0, 104.0], index=idx)
    open_ = pd.Series([99.0, 100.5, 102.0, 103.5], index=idx)
    target = pd.Series([0.0, 1.0, 1.0, 1.0], index=idx)

    ledger = compute_pnl_ledger(
        target,
        close,
        open_=open_,
        execution_mode="next_open",
        cost_bps=0.0,
    )

    # Signal appears at bar 1, fill occurs at bar 2 open, so first live PnL is on bar 2.
    assert ledger["executed_position"].iloc[2] == pytest.approx(1.0)
    assert ledger["gross_pnl"].iloc[1] == pytest.approx(0.0)
    assert ledger["gross_pnl"].iloc[2] == pytest.approx(close.iloc[2] / open_.iloc[2] - 1.0)


def test_next_open_exit_bar_only_accrues_gap_leg_before_fill() -> None:
    idx = _ts(5)
    close = pd.Series([100.0, 101.0, 102.0, 100.0, 99.0], index=idx)
    open_ = pd.Series([99.5, 100.5, 101.5, 98.0, 98.5], index=idx)
    target = pd.Series([1.0, 1.0, 0.0, 0.0, 0.0], index=idx)

    ledger = compute_pnl_ledger(
        target,
        close,
        open_=open_,
        execution_mode="next_open",
        cost_bps=0.0,
    )

    # Exit signal at bar 2 means position is still live through the gap into bar 3 open,
    # then flat for the bar 3 intrabar leg.
    expected_gap_only = open_.iloc[3] / close.iloc[2] - 1.0
    assert ledger["executed_position"].iloc[3] == pytest.approx(0.0)
    assert ledger["gross_pnl"].iloc[3] == pytest.approx(expected_gap_only)
