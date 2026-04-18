from __future__ import annotations

import numpy as np
import pandas as pd

from project.events.detectors.dislocation_base import (
    BasisDislocationDetectorV2,
    FndDislocDetectorV2,
    SpotPerpBasisShockDetectorV2,
)
from project.events.detectors.registry import get_detector, load_all_detectors
from project.events.registry import get_detector_contract


def _basis_df(n: int = 3200) -> pd.DataFrame:
    ts = pd.date_range('2024-01-01', periods=n, freq='5min', tz='UTC')
    close_spot = np.full(n, 100.0)
    close_perp = np.full(n, 100.1)
    funding = np.full(n, 0.00002)
    close_perp[-6:] = [100.0, 100.4, 100.9, 102.0, 104.0, 106.0]
    funding[-6:] = [0.00002, 0.00004, 0.00007, 0.00020, 0.00035, 0.00045]
    return pd.DataFrame({
        'timestamp': ts,
        'close_spot': close_spot,
        'close_perp': close_perp,
        'funding_rate_scaled': funding,
        'ms_funding_state': np.full(n, 2.5),
        'ms_funding_confidence': np.full(n, 1.0),
        'ms_funding_entropy': np.zeros(n),
    })


def test_basis_wave2_detectors_emit_and_contracts_are_v2() -> None:
    df = _basis_df()
    params = {'symbol': 'BTCUSDT', 'timeframe': '5m'}
    events = BasisDislocationDetectorV2().detect_events(df, params)
    fnd_events = FndDislocDetectorV2().detect_events(df, params)
    shock_events = SpotPerpBasisShockDetectorV2().detect_events(df, params)
    assert not events.empty
    assert not fnd_events.empty
    assert not shock_events.empty
    assert events.iloc[-1]['event_name'] == 'BASIS_DISLOC'
    assert fnd_events.iloc[-1]['event_name'] == 'FND_DISLOC'
    assert shock_events.iloc[-1]['event_name'] == 'SPOT_PERP_BASIS_SHOCK'
    contract = get_detector_contract('BASIS_DISLOC')
    assert contract.event_version == 'v2'
    assert contract.detector_class == 'BasisDislocationDetectorV2MetadataAdapter'
    load_all_detectors()
    assert get_detector('BASIS_DISLOC').__class__.__name__ == 'BasisDislocationDetectorV2'
