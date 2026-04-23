from __future__ import annotations

import pandas as pd

from project.reliability.contracts import validate_strategy_trace


def test_validate_strategy_trace_accepts_required_columns():
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=2, freq="5min", tz="UTC"),
            "strategy": ["s", "s"],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "signal_position": [0.0, 1.0],
            "target_position": [0.0, 1.0],
            "executed_position": [0.0, 1.0],
            "gross_pnl": [0.0, 0.01],
            "net_pnl": [0.0, 0.009],
            "equity_return": [0.0, 0.009],
        }
    )
    out = validate_strategy_trace(df)
    assert len(out) == 2
