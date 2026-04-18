from __future__ import annotations

import numpy as np
import pandas as pd

from project.events.detectors.liquidity_base import (
    DepthCollapseDetectorV2,
    LiquidityGapDetectorV2,
    LiquidityVacuumDetectorV2,
)
from project.events.detectors.liquidation_base import LiquidationCascadeProxyDetectorV2
from project.events.detectors.volatility_base import VolRelaxationStartDetectorV2, VolShockDetectorV2


def _base_df(n: int = 800) -> pd.DataFrame:
    ts = pd.date_range('2024-01-01', periods=n, freq='5min', tz='UTC')
    close = np.full(n, 100.0)
    high = close + 0.5
    low = close - 0.5
    return pd.DataFrame({'timestamp': ts, 'close': close, 'high': high, 'low': low})


def test_depth_collapse_v2_emits_event() -> None:
    df = _base_df()
    df['depth_usd'] = 100_000.0
    df['spread_bps'] = 1.0
    df.loc[df.index[-5:], 'depth_usd'] = 10_000.0
    df.loc[df.index[-5:], 'spread_bps'] = 3.0
    events = DepthCollapseDetectorV2().detect_events(df, {'symbol': 'BTCUSDT', 'timeframe': '5m'})
    assert not events.empty
    assert set(['event_name', 'event_version', 'data_quality_flag']).issubset(events.columns)
    assert events.iloc[-1]['event_name'] == 'DEPTH_COLLAPSE'


def test_liquidity_gap_and_vacuum_v2_emit_events() -> None:
    df = _base_df()
    df['depth_usd'] = 100_000.0
    df['spread_bps'] = 1.0
    df['volume'] = 1_000.0
    df.loc[df.index[-3], 'close'] = 108.0
    df.loc[df.index[-3], 'high'] = 109.0
    df.loc[df.index[-3], 'low'] = 104.0
    df.loc[df.index[-2:], 'depth_usd'] = 8_000.0
    df.loc[df.index[-2:], 'spread_bps'] = 5.0
    gap_events = LiquidityGapDetectorV2().detect_events(df, {'symbol': 'BTCUSDT', 'timeframe': '5m'})
    vac_events = LiquidityVacuumDetectorV2().detect_events(df, {'symbol': 'BTCUSDT', 'timeframe': '5m'})
    assert not gap_events.empty
    assert not vac_events.empty
    assert vac_events.iloc[-1]['event_name'] == 'LIQUIDITY_VACUUM'


def test_liquidation_cascade_proxy_v2_emits_event() -> None:
    df = _base_df(1000)
    df['volume'] = 1000.0
    df['oi_notional'] = 10000.0
    df['oi_delta_1h'] = 0.0
    df.loc[df.index[-10:], 'close'] = np.linspace(100, 85, 10)
    df.loc[df.index[-10:], 'low'] = np.linspace(99, 80, 10)
    df.loc[df.index[-10:], 'volume'] = np.linspace(1000, 10000, 10)
    df.loc[df.index[-10:], 'oi_delta_1h'] = np.linspace(-50, -600, 10)
    events = LiquidationCascadeProxyDetectorV2().detect_events(df, {'symbol': 'BTCUSDT', 'timeframe': '5m'})
    assert not events.empty
    assert events.iloc[-1]['event_name'] == 'LIQUIDATION_CASCADE_PROXY'
    assert events.iloc[-1]['data_quality_flag'] == 'degraded'


def test_vol_shock_and_relaxation_split_emit_distinct_events() -> None:
    n = 3200
    ts = pd.date_range('2024-01-01', periods=n, freq='5min', tz='UTC')
    rv = np.full(n, 0.01)
    rv[-20:-10] = np.linspace(0.01, 0.3, 10)
    rv[-10:] = np.linspace(0.3, -0.2, 10)
    close = np.full(n, 100.0)
    close[-20:-10] = np.linspace(100, 110, 10)
    close[-10:] = np.linspace(110, 102, 10)
    df = pd.DataFrame({
        'timestamp': ts,
        'close': close,
        'rv_96': rv,
        'range_96': np.full(n, 0.02),
        'range_med_2880': np.full(n, 0.02),
        'ms_vol_state': np.full(n, 2.0),
        'ms_vol_confidence': np.full(n, 1.0),
        'ms_vol_entropy': np.zeros(n),
    })
    shock_events = VolShockDetectorV2().detect_events(df, {'symbol': 'BTCUSDT', 'timeframe': '5m'})
    relax_events = VolRelaxationStartDetectorV2().detect_events(df, {'symbol': 'BTCUSDT', 'timeframe': '5m'})
    assert not shock_events.empty
    assert not relax_events.empty
    assert shock_events.iloc[-1]['event_name'] == 'VOL_SHOCK'
    assert relax_events.iloc[-1]['event_name'] == 'VOL_RELAXATION_START'
