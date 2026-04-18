from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from project.events.families.volatility import detect_volatility_family, analyze_volatility_family
from project.events.families.trend import detect_trend_family, analyze_trend_family
from project.events.families.statistical import (
    detect_statistical_family,
    analyze_statistical_family,
)
from project.events.families.liquidity import detect_liquidity_family, analyze_liquidity_family
from project.events.families.regime import detect_regime_family, analyze_regime_family
from project.events.families.temporal import detect_temporal_family, analyze_temporal_family
from project.events.families.desync import detect_desync_family, analyze_desync_family


def create_mock_df(periods: int = 3500) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=periods, freq="5min", tz="UTC")
    close = 100.0 * (1.0 + np.cumsum(np.random.normal(0, 0.001, periods)))
    df = pd.DataFrame(
        {
            "timestamp": ts,
            "close": close,
            "high": close * 1.001,
            "low": close * 0.999,
            "volume": np.random.uniform(1000, 5000, periods),
            "rv_96": np.random.uniform(0.001, 0.002, periods),
            "range_96": np.random.uniform(0.001, 0.002, periods),
            "range_med_2880": pd.Series([0.0015] * periods),
            "spread_zscore": np.random.normal(0, 1, periods),
            "basis_zscore": np.random.normal(0, 1, periods),
            "cross_exchange_spread_z": np.random.normal(0, 1, periods),
            "ms_vol_state": pd.Series([2.0] * periods),
            "ms_vol_confidence": pd.Series([0.9] * periods),
            "ms_vol_entropy": pd.Series([0.1] * periods),
            "ms_spread_state": pd.Series([2.0] * periods),
            "ms_spread_confidence": pd.Series([0.9] * periods),
            "ms_spread_entropy": pd.Series([0.1] * periods),
        }
    )
    return df


def test_volatility_hardening():
    df = create_mock_df()
    # Inject a spike
    df.loc[400:405, "rv_96"] = df["rv_96"].max() * 10
    events = detect_volatility_family(df, "TEST", event_type="VOL_SPIKE")
    assert not events.empty
    assert "VOL_SPIKE" in events["event_type"].values


def test_trend_hardening():
    df = create_mock_df()
    # Inject a breakout
    df.loc[450:, "close"] = df["close"].iloc[449] * 1.10
    events = detect_trend_family(df, "TEST", event_type="RANGE_BREAKOUT")
    assert not events.empty
    assert "RANGE_BREAKOUT" in events["event_type"].values


def test_statistical_hardening():
    df = create_mock_df()
    # Inject a z-score stretch
    df.loc[400:410, "close"] = df["close"].iloc[399] * 1.20
    events = detect_statistical_family(df, "TEST", event_type="ZSCORE_STRETCH")
    assert not events.empty
    assert "ZSCORE_STRETCH" in events["event_type"].values


def test_liquidity_hardening():
    df = create_mock_df()
    # Inject a spread blowout
    df.loc[400:405, "spread_zscore"] = 10.0
    events = detect_liquidity_family(df, "TEST", event_type="SPREAD_BLOWOUT")
    assert not events.empty
    assert "SPREAD_BLOWOUT" in events["event_type"].values


def test_regime_hardening():
    df = create_mock_df()
    # Inject a regime shift (low to high vol)
    df.loc[0:250, "rv_96"] = 0.0001
    df.loc[251:, "rv_96"] = 0.01
    events = detect_regime_family(df, "TEST", event_type="VOL_REGIME_SHIFT", lookback_window=100)
    assert not events.empty
    assert "VOL_REGIME_SHIFT_EVENT" in events["event_type"].values


def test_temporal_hardening():
    df = create_mock_df()
    # Session open is time-based, mock should have some 00:00:00
    events = detect_temporal_family(df, "TEST", event_type="SESSION_OPEN_EVENT")
    assert not events.empty
    assert "SESSION_OPEN_EVENT" in events["event_type"].values


def test_desync_hardening():
    df = create_mock_df()
    # Inject a desync
    df.loc[400:405, "basis_zscore"] = 10.0
    df.loc[400:405, "close"] = df["close"].shift(1) * 1.05
    events = detect_desync_family(df, "TEST", event_type="INDEX_COMPONENT_DIVERGENCE")
    assert not events.empty
    assert "INDEX_COMPONENT_DIVERGENCE" in events["event_type"].values


def test_regime_family_legacy_shape_compatibility():
    df = create_mock_df()
    df.loc[400:405, "basis_zscore"] = 10.0
    df.loc[400:405, "spread_zscore"] = 10.0
    df.loc[400:405, "close"] = df["close"].shift(1) * 0.95
    events = detect_regime_family(df, "TEST", event_type="CORRELATION_BREAKDOWN_EVENT")
    assert not events.empty
    assert "CORRELATION_BREAKDOWN_EVENT" in events["event_type"].values


if __name__ == "__main__":
    pytest.main([__file__])
