from __future__ import annotations

import numpy as np
import pandas as pd

from project.events.detectors.desync_base import BetaSpikeDetectorV2, CorrelationBreakdownDetectorV2
from project.events.registry import get_detector_contract


def _regime_pair_df(n: int = 500) -> pd.DataFrame:
    ts = pd.date_range('2024-01-01', periods=n, freq='5min', tz='UTC')
    base = np.sin(np.arange(n) / 15) / 500
    close = np.empty(n)
    pair = np.empty(n)
    close[0] = 100.0
    pair[0] = 100.0
    for i in range(1, n):
        close[i] = close[i - 1] * (1 + base[i])
        pair[i] = pair[i - 1] * (1 + base[i] * 0.98)
    for i in range(n - 80, n):
        step = np.sin(i / 6) / 80
        close[i] = close[i - 1] * (1 + step)
        pair[i] = pair[i - 1] * (1 - step)
    rv_96 = np.concatenate([np.full(n - 80, 0.0015), np.linspace(0.002, 0.03, 80)])
    return pd.DataFrame({'timestamp': ts, 'close': close, 'pair_close': pair, 'rv_96': rv_96})


def test_regime_transition_wave3_detectors_emit() -> None:
    df = _regime_pair_df()
    corr = CorrelationBreakdownDetectorV2().detect_events(
        df,
        {'symbol': 'BTCUSDT', 'timeframe': '5m', 'regime_window': 60, 'transition_z_threshold': 1.5, 'corr_floor': 0.4, 'min_prior_corr': 0.7},
    )
    beta = BetaSpikeDetectorV2().detect_events(
        df,
        {'symbol': 'BTCUSDT', 'timeframe': '5m', 'regime_window': 60, 'transition_z_threshold': 1.5, 'rv_quantile': 0.6},
    )
    assert not corr.empty
    assert not beta.empty
    assert corr.iloc[-1]['event_name'] == 'CORRELATION_BREAKDOWN_EVENT'
    assert beta.iloc[-1]['event_name'] == 'BETA_SPIKE_EVENT'
    assert get_detector_contract('CORRELATION_BREAKDOWN_EVENT').event_version == 'v2'
    assert get_detector_contract('BETA_SPIKE_EVENT').detector_class == 'BetaSpikeDetectorV2'
