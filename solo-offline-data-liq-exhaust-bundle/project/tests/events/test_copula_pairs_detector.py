from __future__ import annotations

import numpy as np
import pandas as pd

from project.events.families.temporal import CopulaPairsTradingDetector


def test_copula_pairs_detector_uses_pair_columns_when_available() -> None:
    n = 400
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = pd.Series(100.0 + np.sin(np.linspace(0, 20, n)).cumsum() / 10.0)
    pair_close = close + np.concatenate([np.zeros(n - 12), np.linspace(0.0, 7.5, 12)])
    frame = pd.DataFrame(
        {
            "timestamp": ts,
            "close": close,
            "pair_close": pair_close,
            "spread_zscore": np.concatenate([np.zeros(n - 8), np.linspace(0.5, 3.5, 8)]),
            "symbol": "BTCUSDT",
        }
    )
    det = CopulaPairsTradingDetector()
    features = det.prepare_features(frame)
    assert bool(features["pair_in_universe"].iloc[-1]) is True
    assert int(features["partner_count"].iloc[-1]) >= 1


def test_copula_pairs_detector_emits_on_strong_spread_reversion() -> None:
    n = 500
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    close = np.full(n, 100.0)
    close[460:470] = np.array([100.0, 101.2, 103.0, 104.5, 104.0, 103.0, 101.5, 100.3, 99.8, 100.1])
    pair_close = np.full(n, 100.0)
    pair_close[460:470] = np.array([100.0, 98.8, 97.0, 95.5, 96.0, 97.0, 98.5, 99.7, 100.2, 99.9])
    frame = pd.DataFrame(
        {
            "timestamp": ts,
            "close": close,
            "pair_close": pair_close,
            "spread_zscore": np.concatenate([np.full(455, 0.2), np.linspace(0.8, 3.2, 45)]),
            "symbol": "BTCUSDT",
        }
    )
    det = CopulaPairsTradingDetector()
    out = det.detect(frame, symbol="BTCUSDT")
    assert not out.empty
    assert "COPULA_PAIRS_TRADING" in set(out["event_type"].astype(str))
