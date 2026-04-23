from __future__ import annotations

import numpy as np
import pandas as pd

from project.events.detectors.positioning_base import (
    OIFlushDetectorV2,
    OISpikeNegativeDetectorV2,
    OISpikePositiveDetectorV2,
)
from project.events.detectors.registry import get_detector, load_all_detectors
from project.events.registry import get_detector_contract


def _oi_df(n: int = 400) -> pd.DataFrame:
    ts = pd.date_range('2024-01-01', periods=n, freq='5min', tz='UTC')
    close = np.full(n, 100.0)
    oi = np.linspace(10000.0, 10200.0, n)
    close[96:120] = np.linspace(100.0, 108.0, 24)
    oi[96:120] = np.linspace(10200.0, 18000.0, 24)
    close[180:204] = np.linspace(108.0, 95.0, 24)
    oi[180:204] = np.concatenate([np.linspace(18000.0, 52000.0, 4), np.linspace(52000.0, 70000.0, 20)])
    close[320:] = np.linspace(95.0, 85.0, n - 320)
    oi[320:] = np.linspace(42000.0, 15000.0, n - 320)
    return pd.DataFrame({
        'timestamp': ts,
        'close': close,
        'oi_notional': oi,
        'ms_oi_state': np.full(n, 2.5),
        'ms_oi_confidence': np.full(n, 1.0),
        'ms_oi_entropy': np.zeros(n),
    })


def test_oi_wave2_detectors_emit() -> None:
    df = _oi_df()
    params = {'symbol': 'BTCUSDT', 'timeframe': '5m'}
    pos = OISpikePositiveDetectorV2().detect_events(df.iloc[:170].copy(), params)
    neg = OISpikeNegativeDetectorV2().detect_events(df.iloc[:260].copy(), params)
    flush_df = df.copy()
    flush_df['ms_oi_state'] = -1.0
    flush = OIFlushDetectorV2().detect_events(flush_df, params)
    assert not pos.empty
    assert not neg.empty
    assert not flush.empty
    assert pos.iloc[-1]['event_name'] == 'OI_SPIKE_POSITIVE'
    assert neg.iloc[-1]['event_name'] == 'OI_SPIKE_NEGATIVE'
    assert flush.iloc[-1]['event_name'] == 'OI_FLUSH'
    contract = get_detector_contract('OI_FLUSH')
    assert contract.event_version == 'v2'
    assert contract.detector_class == 'OIFlushDetectorV2MetadataAdapter'
    load_all_detectors()
    assert get_detector('OI_FLUSH').__class__.__name__ == 'OIFlushDetectorV2'
