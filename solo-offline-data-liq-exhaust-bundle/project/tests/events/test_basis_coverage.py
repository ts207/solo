from __future__ import annotations

import numpy as np
import pandas as pd

from project.events.families.basis import (
    BasisDislocationDetector,
    CrossVenueDesyncDetector,
    FndDislocDetector,
    SpotPerpBasisShockDetector,
)


def _make_frame(n: int = 32, perp: float = 100.10, spot: float = 100.0) -> pd.DataFrame:
    timestamps = pd.date_range("2021-01-01", periods=n, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "close_perp": np.full(n, perp),
            "close_spot": np.full(n, spot),
            "funding_rate_scaled": np.full(n, 0.001),
            "ms_funding_state": np.full(n, 2.0),
            "ms_funding_confidence": np.full(n, 0.9),
            "ms_funding_entropy": np.full(n, 0.1),
        }
    )


def test_cross_venue_desync_requires_persistent_shock_and_floor() -> None:
    df = _make_frame()
    detector = CrossVenueDesyncDetector()
    features = detector.prepare_features(df)
    basis_bps = pd.Series(10.0, index=df.index)
    zscore = pd.Series(0.0, index=df.index)
    zscore.iloc[10] = 10.0
    zscore.iloc[11] = 10.0
    features["basis_bps"] = basis_bps
    features["basis_zscore"] = zscore
    features["persistent_shock"] = zscore.abs().rolling(2, min_periods=2).min()
    features["dynamic_th"] = pd.Series(3.5, index=df.index)

    single_features = dict(features)
    single_features["persistent_shock"] = (zscore.where(zscore.index != 11, 0.0).abs().rolling(2, min_periods=2).min())
    single_bar = detector.compute_raw_mask(df, features=single_features)
    assert not single_bar.any()

    persistent = detector.compute_raw_mask(df, features=features)
    assert persistent.any()


def test_spot_perp_basis_shock_requires_delta_spike_and_floor() -> None:
    df = _make_frame()
    detector = SpotPerpBasisShockDetector()
    features = detector.prepare_features(df)
    features["basis_bps"] = pd.Series(10.0, index=df.index)
    features["basis_zscore"] = pd.Series(0.0, index=df.index)
    features["basis_zscore"].iloc[15] = 1.0
    features["basis_zscore"].iloc[16] = 6.0
    features["shock_change"] = features["basis_zscore"].diff().abs()
    features["shock_q90"] = pd.Series(2.0, index=df.index)
    features["dynamic_th"] = pd.Series(3.5, index=df.index)

    mask = detector.compute_raw_mask(df, features=features)
    assert mask.any()
    assert bool(mask.iloc[16])


def test_fnd_disloc_rejects_sign_mismatch_false_positive() -> None:
    df = _make_frame(perp=99.90, spot=100.0)
    df["funding_rate_scaled"] = 0.001
    detector = FndDislocDetector()
    features = detector.prepare_features(df)
    features["basis_bps"] = pd.Series(-10.0, index=df.index)
    features["basis_zscore"] = pd.Series(10.0, index=df.index)
    features["dynamic_th"] = pd.Series(3.5, index=df.index)

    mask = detector.compute_raw_mask(df, features=features)
    assert not mask.any()
