import numpy as np
import pandas as pd

from project.events.detectors.liquidity_base import (
    DirectLiquidityStressDetectorV2,
    LiquidityShockDetectorV2,
    ProxyLiquidityStressDetectorV2,
)


def test_direct_liquidity_stress_detector_v2_shapes():
    detector = DirectLiquidityStressDetectorV2()
    assert detector.event_name == "LIQUIDITY_STRESS_DIRECT"

    ts = pd.date_range("2024-01-01", periods=100, freq="5min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": ts,
        "close": np.random.normal(100, 1, 100),
        "high": np.random.normal(101, 1, 100),
        "low": np.random.normal(99, 1, 100),
        "depth_usd": [100000.0] * 90 + [5000.0] * 10, # Drop in depth
        "spread_bps": [2.0] * 90 + [25.0] * 10, # Spike in spread
    })

    events = detector.detect_events(df, {"symbol": "BTCUSDT", "timeframe": "5m"})
    assert not events.empty
    assert "event_name" in events.columns
    assert "data_quality_flag" in events.columns
    assert events.iloc[-1]["severity"] is None or events.iloc[-1]["severity"] >= 0.4
    assert events.iloc[-1]["confidence"] is None or events.iloc[-1]["confidence"] > 0.0

def test_proxy_liquidity_stress_detector_v2_shapes():
    detector = ProxyLiquidityStressDetectorV2()
    assert detector.event_name == "LIQUIDITY_STRESS_PROXY"

    ts = pd.date_range("2024-01-01", periods=100, freq="5min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": ts,
        "close": [100.0] * 90 + [100.0, 95.0, 90.0, 85.0, 80.0, 75.0, 70.0, 65.0, 60.0, 55.0],
        "high": [100.5] * 90 + [101.0, 105.0, 110.0, 115.0, 120.0, 125.0, 130.0, 135.0, 140.0, 145.0],
        "low": [99.5] * 90 + [99.0, 85.0, 80.0, 75.0, 70.0, 65.0, 60.0, 55.0, 50.0, 45.0],
        "volume": [1000.0] * 90 + [100.0] * 10, # Drop in volume
    })

    events = detector.detect_events(df, {"symbol": "BTCUSDT", "timeframe": "5m"})
    assert not events.empty

def test_liquidity_shock_detector_v2_routing():
    detector = LiquidityShockDetectorV2()

    ts = pd.date_range("2024-01-01", periods=100, freq="5min", tz="UTC")
    df_proxy = pd.DataFrame({
        "timestamp": ts,
        "close": [100.0] * 90 + [100.0, 95.0, 90.0, 85.0, 80.0, 75.0, 70.0, 65.0, 60.0, 55.0],
        "high": [100.5] * 90 + [101.0, 105.0, 110.0, 115.0, 120.0, 125.0, 130.0, 135.0, 140.0, 145.0],
        "low": [99.5] * 90 + [99.0, 85.0, 80.0, 75.0, 70.0, 65.0, 60.0, 55.0, 50.0, 45.0],
        "volume": [1000.0] * 90 + [100.0] * 10,
    })

    events = detector.detect_events(df_proxy, {"symbol": "BTCUSDT", "timeframe": "5m"})
    # Since depth_usd/spread_bps are missing, it should use proxy features which will have lower confidence
    assert not events.empty
    if not events.empty:
        assert events.iloc[-1]["detector_metadata"]["evidence_tier"] == "proxy"
