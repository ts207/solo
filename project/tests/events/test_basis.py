from __future__ import annotations

import pandas as pd

from project.events.families.basis import BasisDislocationDetector, FndDislocDetector


def test_basis_dislocation_floor():
    # Create data with high Z-score but low absolute basis
    # Basis = (perp - spot) / spot * 10000
    # Let's say spot is 100.0. 1 bps = 0.01.
    # We want basis < 5 bps (DEFAULT_MIN_BPS).
    # e.g. basis = 2 bps -> perp = 100.02.

    n = 1000
    timestamps = pd.date_range("2021-01-01", periods=n, freq="5min")
    close_spot = pd.Series(100.0, index=timestamps)
    # Basis is constant at 2 bps
    close_perp = pd.Series(100.02, index=timestamps)

    df = pd.DataFrame({
        "timestamp": timestamps,
        "close_perp": close_perp,
        "close_spot": close_spot,
    })

    detector = BasisDislocationDetector()
    # Mock prepare_features to give high Z-score but keep basis_bps low
    features = detector.prepare_features(df)
    # Force high Z-score
    features["basis_zscore"] = pd.Series(10.0, index=timestamps)
    features["dynamic_th"] = pd.Series(3.5, index=timestamps)

    # default min_basis_bps is 5.0
    mask = detector.compute_raw_mask(df, features=features)
    assert not mask.any(), "Should not trigger events when basis < 5 bps even if Z-score is high"

    # Now set basis to 10 bps
    df["close_perp"] = 100.10
    features = detector.prepare_features(df)
    features["basis_zscore"] = pd.Series(10.0, index=timestamps)
    features["dynamic_th"] = pd.Series(3.5, index=timestamps)

    mask = detector.compute_raw_mask(df, features=features)
    assert mask.any(), "Should trigger events when basis > 5 bps and Z-score is high"

def test_fnd_disloc_floor():
    n = 1000
    timestamps = pd.date_range("2021-01-01", periods=n, freq="5min")
    close_spot = pd.Series(100.0, index=timestamps)
    close_perp = pd.Series(100.02, index=timestamps) # 2 bps basis (low)
    funding = pd.Series(0.001, index=timestamps) # 10 bps funding (high)

    df = pd.DataFrame({
        "timestamp": timestamps,
        "close_perp": close_perp,
        "close_spot": close_spot,
        "funding_rate_scaled": funding,
        "ms_funding_state": 2.0, # Stress state
        "ms_funding_confidence": 0.9,
        "ms_funding_entropy": 0.1,
    })

    detector = FndDislocDetector()
    features = detector.prepare_features(df)
    # Force high Z-score for basis part
    features["basis_zscore"] = pd.Series(10.0, index=timestamps)
    features["dynamic_th"] = pd.Series(3.5, index=timestamps)

    # default min_basis_bps is 5.0
    mask = detector.compute_raw_mask(df, features=features)
    assert not mask.any(), "FND_DISLOC should also honor absolute basis floor"

    # Increase basis to 10 bps
    df["close_perp"] = 100.10
    features = detector.prepare_features(df)
    features["basis_zscore"] = pd.Series(10.0, index=timestamps)
    features["dynamic_th"] = pd.Series(3.5, index=timestamps)

    mask = detector.compute_raw_mask(df, features=features)
    assert mask.any(), "FND_DISLOC should trigger when basis floor and other conditions are met"
