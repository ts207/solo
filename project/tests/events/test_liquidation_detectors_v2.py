import pandas as pd
import pytest
import numpy as np

from project.events.detectors.liquidation_base import LiquidationCascadeDetectorV2

def test_liquidation_cascade_detector_v2_shapes():
    detector = LiquidationCascadeDetectorV2()
    assert detector.event_name == "LIQUIDATION_CASCADE"
    
    ts = pd.date_range("2024-01-01", periods=1000, freq="5min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": ts,
        "close": [100.0] * 990 + [95, 90, 85, 80, 75, 70, 65, 60, 55, 50],
        "high": [101.0] * 990 + [96, 91, 86, 81, 76, 71, 66, 61, 56, 51],
        "low": [99.0] * 990 + [94, 89, 84, 79, 74, 69, 64, 59, 54, 49],
        "liquidation_notional": [100.0] * 990 + [10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000],
        "oi_delta_1h": [0.0] * 990 + [-100, -200, -300, -400, -500, -600, -700, -800, -900, -1000],
        "oi_notional": [10000.0] * 1000,
    })
    
    events = detector.detect_events(df, {"symbol": "BTCUSDT", "timeframe": "5m", "liq_median_window": 20})
    assert not events.empty
    assert "event_name" in events.columns
    assert "data_quality_flag" in events.columns
    assert events.iloc[-1]["severity"] is None or events.iloc[-1]["severity"] >= 0.4
    assert events.iloc[-1]["confidence"] is None or events.iloc[-1]["confidence"] > 0.0
    assert events.iloc[-1]["detector_metadata"]["total_liquidation_notional"] > 0
    assert events.iloc[-1]["detector_metadata"]["price_drawdown"] > 0
