import numpy as np
import pandas as pd
import pytest
from project.events.families.basis import (
    CrossVenueDesyncDetector,
    FndDislocDetector,
    SpotPerpBasisShockDetector,
    detect_basis_family as detect_basis_dislocations,
)


def test_detect_basis_dislocations_basic():
    ts = pd.date_range("2024-09-01", periods=100, freq="5min", tz="UTC")
    perp_df = pd.DataFrame({"timestamp": ts, "close": [100.0] * 100, "vol_regime": ["low"] * 100})
    spot_df = pd.DataFrame({"timestamp": ts, "close": [100.0] * 100})

    # Create some background variance in basis to avoid NaN in z-score
    # Rows 0-20: 1 bps
    perp_df.loc[0:20, "close"] = 100.01
    # Rows 21-40: -1 bps
    perp_df.loc[21:40, "close"] = 99.99

    # Trigger basis shock at index 60 (well after window warmup)
    perp_df.loc[60, "close"] = 105.0  # 5% = 500 bps

    events = detect_basis_dislocations(
        perp_df, spot_df, "BTCUSDT", z_threshold=2.0, lookback_window=40
    )

    assert len(events) >= 1
    # Check that we caught the big one
    found_big = any(abs(e["basis_bps"] - 500.0) < 1.0 for _, e in events.iterrows())
    assert found_big


def test_cross_venue_desync_accepts_canonical_basis_feature_columns():
    ts = pd.date_range("2024-09-01", periods=120, freq="5min", tz="UTC")
    close_perp = pd.Series(np.full(120, 100.0))
    close_spot = pd.Series(np.full(120, 100.0))

    # Seed a modest history, then inject a clear desync after warmup.
    close_perp.iloc[10:30] = 100.02
    close_perp.iloc[30:50] = 99.98
    close_perp.iloc[80] = 104.0

    features = pd.DataFrame(
        {
            "timestamp": ts,
            "close_perp": close_perp,
            "close_spot": close_spot,
        }
    )

    events = CrossVenueDesyncDetector().detect(
        features,
        symbol="BTCUSDT",
        lookback_window=40,
        threshold=2.0,
        cooldown_bars=4,
    )

    assert isinstance(events, pd.DataFrame)
    assert not events.empty
    assert (events["event_type"] == "CROSS_VENUE_DESYNC").all()


def test_cross_venue_desync_emits_onset_not_every_persistent_bar():
    ts = pd.date_range("2024-09-01", periods=160, freq="5min", tz="UTC")
    close_perp = pd.Series(np.full(160, 100.0))
    close_spot = pd.Series(np.full(160, 100.0))
    close_perp.iloc[20:60] = 100.02
    close_perp.iloc[60:100] = 99.98
    close_perp.iloc[110:130] = 104.0

    features = pd.DataFrame({"timestamp": ts, "close_perp": close_perp, "close_spot": close_spot})

    events = CrossVenueDesyncDetector().detect(
        features,
        symbol="BTCUSDT",
        lookback_window=40,
        threshold=2.0,
        cooldown_bars=4,
        persistence_bars=2,
    )

    assert 1 <= len(events) <= 3


def test_basis_variants_use_distinct_conditions():
    ts = pd.date_range("2024-09-01", periods=160, freq="5min", tz="UTC")
    close_perp = pd.Series(np.full(160, 100.0))
    close_spot = pd.Series(np.full(160, 100.0))
    close_perp.iloc[20:60] = 100.02
    close_perp.iloc[60:100] = 99.98
    close_perp.iloc[120] = 104.0
    funding = pd.Series(np.zeros(160))
    funding.iloc[120] = 4.0

    features = pd.DataFrame(
        {
            "timestamp": ts,
            "close_perp": close_perp,
            "close_spot": close_spot,
            "funding_rate_scaled": funding,
        }
    )

    fnd_events = FndDislocDetector().detect(
        features,
        symbol="BTCUSDT",
        lookback_window=40,
        z_threshold=2.0,
        cooldown_bars=4,
        threshold_bps=2.0,
    )
    shock_events = SpotPerpBasisShockDetector().detect(
        features,
        symbol="BTCUSDT",
        lookback_window=40,
        z_threshold=2.0,
        cooldown_bars=4,
        shock_change_floor=0.5,
    )

    assert not fnd_events.empty
    assert not shock_events.empty
    assert len(fnd_events) <= len(shock_events)


def test_funding_dislocation_requires_canonical_funding_rate_scaled():
    ts = pd.date_range("2024-09-01", periods=120, freq="5min", tz="UTC")
    features = pd.DataFrame(
        {
            "timestamp": ts,
            "close_perp": np.full(120, 100.0),
            "close_spot": np.full(120, 100.0),
            "funding_rate": np.zeros(120),
        }
    )

    with pytest.raises(ValueError, match="funding_rate_scaled"):
        FndDislocDetector().detect(
            features,
            symbol="BTCUSDT",
            lookback_window=40,
            z_threshold=2.0,
            cooldown_bars=4,
            threshold_bps=2.0,
        )


def test_funding_dislocation_threshold_bps_floor_uses_decimal_funding_units():
    ts = pd.date_range("2024-09-01", periods=160, freq="5min", tz="UTC")
    close_spot = pd.Series(np.full(160, 100.0))
    close_perp = close_spot.copy()
    close_perp.iloc[20:60] = 100.02
    close_perp.iloc[60:100] = 99.98
    close_perp.iloc[120] = 104.0
    funding = pd.Series(np.zeros(160), dtype=float)
    funding.iloc[120] = 0.0006

    features = pd.DataFrame(
        {
            "timestamp": ts,
            "close_perp": close_perp,
            "close_spot": close_spot,
            "funding_rate_scaled": funding,
        }
    )

    events = FndDislocDetector().detect(
        features,
        symbol="BTCUSDT",
        lookback_window=40,
        z_threshold=2.0,
        cooldown_bars=4,
        threshold_bps=2.0,
    )

    assert not events.empty
    assert (events["event_type"] == "FND_DISLOC").all()
