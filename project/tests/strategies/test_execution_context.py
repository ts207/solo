from __future__ import annotations

import pandas as pd

from project.strategy.runtime.dsl_runtime.execution_context import build_signal_frame


def test_build_signal_frame_defaults_missing_funding_to_zero_when_absent() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "close": [100.0],
            "volume": [5.0],
            "funding_rate": [0.0001],
        }
    )

    out = build_signal_frame(frame)
    assert out["funding_rate_scaled"].tolist() == [0.0]
    assert out["funding_bps_abs"].tolist() == [0.0]
    assert out["funding_rate_scaled_available"].tolist() == [False]
