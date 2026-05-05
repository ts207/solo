from __future__ import annotations

import numpy as np
import pandas as pd

from project.events.detectors.liquidation_base import LiquidationCascadeDetectorV2
from project.events.detectors.volatility_base import BreakoutTriggerDetectorV2, VolSpikeDetectorV2


def _vol_frame(rows: int = 360) -> pd.DataFrame:
    close = pd.Series(np.linspace(100.0, 110.0, rows))
    close.iloc[-1] = close.iloc[-2] * 1.02
    high = close * 1.002
    low = close * 0.998
    high.iloc[-1] = close.iloc[-1] * 1.01
    low.iloc[-1] = close.iloc[-1] * 0.999
    return pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=rows, freq="5min", tz="UTC"),
            "close": close,
            "high": high,
            "low": low,
            "rv_96": np.linspace(0.001, 0.02, rows),
            "range_96": np.linspace(3.0, 20.0, rows),
            "range_med_2880": np.linspace(10.0, 12.0, rows),
            "ms_vol_state": 2.0,
            "ms_vol_confidence": 0.8,
            "ms_vol_entropy": 0.2,
            "spread_bps": 2.0,
            "depth_usd": 100000.0,
            "expected_cost_bps": 3.0,
        }
    )


def test_vol_spike_records_context_metadata() -> None:
    detector = VolSpikeDetectorV2.__new__(VolSpikeDetectorV2)
    frame = _vol_frame()
    features = detector.prepare_features(frame)
    meta = detector.compute_metadata(len(frame) - 1, features)

    assert meta["event_semantics"] == "volatility_spike"
    assert meta["detector_family"] == "volatility"
    assert meta["directionality"] == "signed_move"
    assert "signal_context" in meta
    assert "execution_context" in meta
    assert meta["context_quality"] == "ok"


def test_breakout_trigger_records_side_and_context_metadata() -> None:
    detector = BreakoutTriggerDetectorV2.__new__(BreakoutTriggerDetectorV2)
    frame = _vol_frame()
    features = detector.prepare_features(frame)
    meta = detector.compute_metadata(len(frame) - 1, features)

    assert meta["event_semantics"] == "breakout_trigger"
    assert meta["breakout_side"] in {"up", "down", "ambiguous"}
    assert "breakout_dist" in meta
    assert "comp_ratio" in meta
    assert "signal_context" in meta
    assert "execution_context" in meta


def test_liquidation_cascade_records_side_and_context_quality() -> None:
    detector = LiquidationCascadeDetectorV2.__new__(LiquidationCascadeDetectorV2)
    rows = 120
    close = pd.Series(np.linspace(100.0, 90.0, rows))
    close.iloc[-1] = close.iloc[-2] * 0.98
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=rows, freq="5min", tz="UTC"),
            "liquidation_notional": np.linspace(1000.0, 200000.0, rows),
            "oi_delta_1h": np.linspace(-100.0, -8000.0, rows),
            "oi_notional": 1_000_000.0,
            "funding_rate": 0.001,
            "close": close,
            "high": close * 1.002,
            "low": close * 0.998,
        }
    )
    features = detector.prepare_features(frame)
    meta = detector.compute_metadata(len(frame) - 1, features)

    assert meta["event_semantics"] == "cascade_episode"
    assert meta["detector_family"] == "liquidation"
    assert meta["cascade_side"] in {"longs_liquidated", "shorts_liquidated", "ambiguous"}
    assert "oi_delta_fraction" in meta
    assert "funding_rate" in meta
    assert meta["context_quality"] == "ok"
