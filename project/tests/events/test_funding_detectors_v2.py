from __future__ import annotations

import numpy as np
import pandas as pd

from project.events.detectors.positioning_base import (
    FundingExtremeOnsetDetectorV2,
    FundingFlipDetectorV2,
    FundingNormalizationDetectorV2,
    FundingPersistenceDetectorV2,
)
from project.events.registry import get_detector_contract


def _funding_df(n: int = 3200) -> pd.DataFrame:
    ts = pd.date_range('2024-01-01', periods=n, freq='5min', tz='UTC')
    funding_abs_pct = np.full(n, 10.0)
    funding_abs = np.full(n, 0.0001)
    funding_scaled = np.full(n, 0.0001)
    # onset / persistence block
    funding_abs_pct[-20:-8] = np.linspace(20.0, 99.0, 12)
    funding_abs[-20:-8] = np.linspace(0.0001, 0.0011, 12)
    funding_scaled[-20:-8] = np.linspace(0.0001, 0.0011, 12)
    # normalization
    funding_abs_pct[-8:-3] = [98.0, 96.0, 92.0, 60.0, 40.0]
    funding_abs[-8:-3] = [0.0010, 0.0009, 0.0008, 0.0005, 0.0003]
    funding_scaled[-8:-3] = [0.0010, 0.0009, 0.0008, 0.0005, 0.0003]
    # sign flip with persistence
    funding_abs_pct[-3:] = [70.0, 80.0, 85.0]
    funding_abs[-3:] = [0.0005, 0.0006, 0.0007]
    funding_scaled[-3:] = [-0.0005, -0.0006, -0.0007]
    df = pd.DataFrame({
        'timestamp': ts,
        'funding_abs_pct': funding_abs_pct,
        'funding_abs': funding_abs,
        'funding_rate_scaled': funding_scaled,
    })
    df['fp_active'] = 0.0
    df['fp_age_bars'] = 0.0
    df['fp_severity'] = 0.0
    df.loc[df.index[-16:-8], 'fp_active'] = 1.0
    df.loc[df.index[-16:-8], 'fp_age_bars'] = np.arange(0, 8)
    df.loc[df.index[-16:-8], 'fp_severity'] = np.linspace(0.6, 1.4, 8)
    return df


def test_funding_wave2_detectors_emit() -> None:
    df = _funding_df()
    params = {'symbol': 'BTCUSDT', 'timeframe': '5m'}
    onset = FundingExtremeOnsetDetectorV2().detect_events(df, params)
    persistence = FundingPersistenceDetectorV2().detect_events(df, params)
    normalization = FundingNormalizationDetectorV2().detect_events(df, params)
    flip = FundingFlipDetectorV2().detect_events(df, params)
    assert not onset.empty
    assert not persistence.empty
    assert not normalization.empty
    assert not flip.empty
    assert onset.iloc[-1]['event_name'] == 'FUNDING_EXTREME_ONSET'
    assert flip.iloc[-1]['event_name'] == 'FUNDING_FLIP'
    contract = get_detector_contract('FUNDING_FLIP')
    assert contract.event_version == 'v2'
    assert contract.detector_class == 'FundingFlipDetectorV2'
    assert contract.runtime_default is False
    assert contract.promotion_eligible is True
