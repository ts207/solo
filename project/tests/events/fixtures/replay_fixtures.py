import numpy as np
import pandas as pd


def create_liquidity_shock_fixture():
    ts = pd.date_range("2024-01-01", periods=100, freq="5min", tz="UTC")
    df = pd.DataFrame({
        "timestamp": ts,
        "close": np.random.normal(100, 1, 100),
        "high": np.random.normal(101, 1, 100),
        "low": np.random.normal(99, 1, 100),
        "depth_usd": [100000.0] * 90 + [5000.0] * 10,
        "spread_bps": [2.0] * 90 + [25.0] * 10,
    })
    return df

def create_vol_spike_fixture():
    ts = pd.date_range("2024-01-01", periods=3000, freq="5min", tz="UTC")
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
    return df

def create_liquidation_cascade_fixture():
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
    return df
