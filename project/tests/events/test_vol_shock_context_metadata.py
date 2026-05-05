from __future__ import annotations

import numpy as np
import pandas as pd

from project.events.detectors.volatility_base import VolShockDetectorV2


def _frame(include_context: bool = True) -> pd.DataFrame:
    rows = 360
    close = pd.Series(np.linspace(100.0, 130.0, rows))
    close.iloc[-1] = close.iloc[-2] * 1.02
    out = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=rows, freq="5min", tz="UTC"),
            "close": close,
            "rv_96": np.linspace(0.001, 0.01, rows),
            "range_96": np.linspace(10.0, 20.0, rows),
            "range_med_2880": np.linspace(9.0, 12.0, rows),
            "spread_bps": 2.0,
            "depth_usd": 100000.0,
            "expected_cost_bps": 3.0,
        }
    )
    if include_context:
        out["ms_vol_state"] = 2.0
        out["ms_vol_confidence"] = 0.8
        out["ms_vol_entropy"] = 0.2
    return out


def test_vol_shock_records_present_context_metadata() -> None:
    detector = VolShockDetectorV2.__new__(VolShockDetectorV2)
    features = detector.prepare_features(_frame(include_context=True))
    meta = detector.compute_metadata(len(_frame()) - 1, features)

    assert "ms_vol_state" in meta["context_columns_present"]
    assert meta["context_defaulted"] == []
    assert meta["context_quality"] == "ok"
    assert "signed_move_bps" in meta
    assert "signal_context" in meta
    assert "execution_context" in meta


def test_vol_shock_records_defaulted_missing_context() -> None:
    detector = VolShockDetectorV2.__new__(VolShockDetectorV2)
    frame = _frame(include_context=False)
    features = detector.prepare_features(frame)
    meta = detector.compute_metadata(len(frame) - 1, features)

    assert "ms_vol_state" in meta["context_columns_missing"]
    assert "ms_vol_state" in meta["context_defaulted"]
    assert meta["context_quality"] == "defaulted"
