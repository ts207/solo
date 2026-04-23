from __future__ import annotations

import pandas as pd
import pytest

from project.reliability.regression_checks import assert_runner_requires_signal_position


def test_runner_position_contract_rejects_legacy_pos_only():
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=2, freq="5min", tz="UTC"),
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "pos": [0.0, 1.0],
        }
    )
    with pytest.raises(Exception):
        assert_runner_requires_signal_position(frame)
