from __future__ import annotations

import pandas as pd

from project.reliability.regression_checks import assert_next_open_entry_economics_preserved


def test_next_open_entry_accounting_regression():
    idx = pd.date_range("2024-01-01", periods=4, freq="5min", tz="UTC")
    close = pd.Series([100.0, 101.0, 102.0, 103.0], index=idx)
    open_ = pd.Series([99.5, 100.5, 101.5, 102.5], index=idx)
    target = pd.Series([0.0, 1.0, 1.0, 0.0], index=idx)
    ledger = assert_next_open_entry_economics_preserved(close, open_, target)
    changed = ledger["turnover"] > 0
    assert abs(float(ledger.loc[changed, "gross_pnl"].iloc[0])) > 0.0
