import numpy as np
import pandas as pd

from project.events.detectors.volatility_base import VolSpikeDetectorV2


def test_vol_spike_detector_v2_shapes():
    detector = VolSpikeDetectorV2()
    assert detector.event_name == "VOL_SPIKE"

    ts = pd.date_range("2024-01-01", periods=3000, freq="5min", tz="UTC")

    # 2880 window required for range_med_2880
    rv = np.random.normal(0.01, 0.001, 3000)
    rv[-50:] = [0.2 + i * 0.01 for i in range(50)]

    df = pd.DataFrame({
        "timestamp": ts,
        "close": np.random.normal(100, 1, 3000),
        "rv_96": rv,
        "range_96": np.random.normal(0.02, 0.005, 3000),
        "range_med_2880": [0.02] * 3000,
        "ms_vol_state": [2.0] * 3000,
        "ms_vol_confidence": [1.0] * 3000,
        "ms_vol_entropy": [0.0] * 3000,
    })

    events = detector.detect_events(df, {"symbol": "BTCUSDT", "timeframe": "5m"})
    assert not events.empty
    assert "event_name" in events.columns
    assert "data_quality_flag" in events.columns
    assert events.iloc[-1]["severity"] is None or events.iloc[-1]["severity"] >= 0.4
    assert events.iloc[-1]["confidence"] is None or events.iloc[-1]["confidence"] > 0.0
