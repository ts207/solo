from __future__ import annotations

import pandas as pd

from project.reliability.contracts import validate_portfolio_ledger


def test_validate_portfolio_ledger_accepts_required_columns():
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=2, freq="5min", tz="UTC"),
            "gross_pnl": [0.0, 0.02],
            "net_pnl": [0.0, 0.015],
            "equity": [1.0, 1.015],
            "equity_return": [0.0, 0.015],
            "gross_exposure": [0.0, 1.0],
            "net_exposure": [0.0, 1.0],
            "turnover": [0.0, 1.0],
        }
    )
    out = validate_portfolio_ledger(df)
    assert float(out["equity"].iloc[-1]) == 1.015
