from __future__ import annotations

import numpy as np
import pandas as pd

from project.events.detectors.desync_base import (
    CrossAssetDesyncDetectorV2,
    CrossVenueDesyncDetectorV2,
    IndexComponentDivergenceDetectorV2,
    LeadLagBreakDetectorV2,
)
from project.events.detectors.registry import get_detector, load_all_detectors
from project.events.registry import get_detector_contract


def _cross_venue_df(n: int = 220) -> pd.DataFrame:
    ts = pd.date_range('2024-01-01', periods=n, freq='5min', tz='UTC')
    close_spot = np.full(n, 100.0)
    close_perp = np.full(n, 100.0)
    close_perp[20:60] = 100.02
    close_perp[60:100] = 99.98
    close_perp[-6:] = [100.0, 100.0, 104.0, 104.0, 104.0, 104.0]
    return pd.DataFrame({'timestamp': ts, 'close_spot': close_spot, 'close_perp': close_perp})


def _pair_df(n: int = 400) -> pd.DataFrame:
    ts = pd.date_range('2024-01-01', periods=n, freq='5min', tz='UTC')
    close = np.full(n, 100.0)
    pair = np.full(n, 100.0)
    for i in range(1, n):
        step = np.sin(i / 20) / 500
        close[i] = close[i - 1] * (1 + step)
        pair[i] = pair[i - 1] * (1 + step * 0.98)
    close[-8:] = close[-9] * np.array([1.0, 1.01, 1.03, 1.06, 1.10, 1.15, 1.20, 1.25])
    pair[-8:] = pair[-9] * np.array([1.0, 1.002, 1.004, 1.006, 1.008, 1.010, 1.012, 1.014])
    return pd.DataFrame({
        'timestamp': ts,
        'close': close,
        'pair_close': pair,
        'rv_96': np.concatenate([np.full(n - 8, 0.001), np.linspace(0.002, 0.02, 8)]),
    })


def test_desync_wave3_detectors_emit_and_contracts_are_v2() -> None:
    cross_venue = CrossVenueDesyncDetectorV2().detect_events(
        _cross_venue_df(),
        {'symbol': 'BTCUSDT', 'timeframe': '5m', 'lookback_window': 40, 'threshold': 2.0, 'persistence_bars': 2, 'min_basis_bps': 5},
    )
    assert not cross_venue.empty
    assert cross_venue.iloc[-1]['event_name'] == 'CROSS_VENUE_DESYNC'

    pair_df = _pair_df()
    params = {'symbol': 'BTCUSDT', 'timeframe': '5m', 'lookback_window': 120, 'threshold_z': 2.0, 'threshold_quantile': 0.9}
    cross_asset = CrossAssetDesyncDetectorV2().detect_events(pair_df, params)
    divergence = IndexComponentDivergenceDetectorV2().detect_events(pair_df, params)
    lead_lag = LeadLagBreakDetectorV2().detect_events(pair_df, params)
    assert not cross_asset.empty
    assert not divergence.empty
    assert not lead_lag.empty
    assert get_detector_contract('CROSS_ASSET_DESYNC_EVENT').event_version == 'v2'
    assert get_detector_contract('CROSS_VENUE_DESYNC').detector_class == 'CrossVenueDesyncDetectorV2MetadataAdapter'
    load_all_detectors()
    assert get_detector('CROSS_VENUE_DESYNC').__class__.__name__ == 'CrossVenueDesyncDetectorV2'


def test_wave3_family_modules_register_v2_detectors_under_legacy_names() -> None:
    load_all_detectors()
    cross_venue = get_detector('CROSS_VENUE_DESYNC')
    cross_asset = get_detector('CROSS_ASSET_DESYNC_EVENT')
    assert cross_venue is not None
    assert cross_asset is not None
    assert cross_venue.__class__.__name__ == 'CrossVenueDesyncDetectorV2'
    assert cross_asset.__class__.__name__ == 'CrossAssetDesyncDetectorV2'
