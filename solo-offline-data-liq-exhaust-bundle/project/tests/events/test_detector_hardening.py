from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from project.events.families.oi import DeleveragingWaveDetector
from project.events.families.basis import FndDislocDetector
from project.events.families.canonical_proxy import (
    AbsorptionProxyDetector,
    DepthStressProxyDetector,
)
from project.events.families.canonical_proxy import PriceVolImbalanceProxyDetector
from project.events.detectors.funding import (
    FundingFlipDetector,
    FundingNormalizationDetector,
    FundingPersistenceDetector,
)
from project.events.detectors.exhaustion import TrendExhaustionDetector, MomentumDivergenceDetector
from project.events.detectors.liquidity import DirectLiquidityStressDetector, DepthCollapseDetector
from project.events.detectors.volatility import (
    BreakoutTriggerDetector,
    VolSpikeDetector,
    VolRelaxationDetector,
)
from project.events.families.temporal import SpreadRegimeWideningDetector
from project.events.detectors.trend import SREventDetector, TrendAccelerationDetector
from project.events.policy import (
    LIVE_SAFE_EVENT_TYPES,
    RETROSPECTIVE_EVENT_TYPES,
    is_legacy_event_type,
)


def create_mock_data(n=2000):
    rng = np.random.default_rng(42)
    # Price with some trends and vol
    returns = rng.normal(0, 0.001, n)
    # Add a strong trend
    returns[1000:1100] += 0.005
    close = 100 * np.exp(np.cumsum(returns))

    # OI and Vol for DeleveragingWave
    oi_delta_1h = rng.normal(0, 1.0, n)
    # Inject several sharp drops of varying magnitude
    oi_delta_1h[100] = -10.0
    oi_delta_1h[500] = -5.0
    oi_delta_1h[1500] = -8.0

    rv_96 = rng.uniform(0.001, 0.002, n)
    # Inject vol spikes
    rv_96[100] = 0.01
    rv_96[500] = 0.005
    rv_96[1500] = 0.008

    close_ser = pd.Series(close)
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close": close_ser,
            "oi_delta_1h": oi_delta_1h,
            "rv_96": rv_96,
            "open": close_ser.shift(1).fillna(close_ser[0]),
            "high": close_ser * 1.001,
            "low": close_ser * 0.999,
            "volume": 1000.0,
        }
    )
    return df


def test_deleveraging_wave_default_is_tight():
    df = create_mock_data()
    detector = DeleveragingWaveDetector()
    events = detector.detect(df, symbol="BTCUSDT")

    # Assert fewer than 3 events on this mock data.
    # The shock at 500 should be filtered out by tighter quantiles.
    assert len(events) <= 2


def test_trend_acceleration_default_is_tight():
    df = create_mock_data()
    detector = TrendAccelerationDetector()
    events = detector.detect(df, symbol="BTCUSDT")

    # Assert fewer than 5 events on 2000 bars.
    assert len(events) <= 5


def test_trend_acceleration_requires_canonical_active_trend_when_present():
    detector = TrendAccelerationDetector()
    index = pd.RangeIndex(3)
    features = {
        "trend_abs": pd.Series([0.02, 0.03, 0.04], index=index),
        "trend_delta": pd.Series([0.01, 0.02, 0.03], index=index),
        "ret_1": pd.Series([0.01, 0.01, 0.01], index=index),
        "trend_raw": pd.Series([0.02, 0.03, 0.04], index=index),
        "trend_q_ext": pd.Series([0.005, 0.005, 0.005], index=index),
        "accel_q_threshold": pd.Series([0.001, 0.001, 0.001], index=index),
        "canonical_trend_state": pd.Series([0.0, 0.0, 0.0], index=index),
    }

    mask = detector.compute_raw_mask(pd.DataFrame(index=index), features=features)

    assert not mask.any()


def test_direct_liquidity_stress_requires_canonical_wide_spread_when_present():
    detector = DirectLiquidityStressDetector()
    index = pd.RangeIndex(3)
    features = {
        "depth": pd.Series([10.0, 10.0, 10.0], index=index),
        "spread": pd.Series([5.0, 5.0, 5.0], index=index),
        "depth_median": pd.Series([100.0, 100.0, 100.0], index=index),
        "spread_median": pd.Series([1.0, 1.0, 1.0], index=index),
        "imbalance": pd.Series([0.0, 0.0, 0.0], index=index),
        "canonical_spread_wide": pd.Series([False, False, False], index=index),
    }

    mask = detector.compute_raw_mask(pd.DataFrame(index=index), features=features)

    assert not mask.any()


def test_zscore_stretch_uses_spec_quantile_and_windows(monkeypatch):
    import project.events.families.statistical as statistical_family
    from project.events.thresholding import rolling_mean_std_zscore

    df = create_mock_data()

    monkeypatch.setattr(
        statistical_family,
        "load_event_spec",
        lambda event_type: {
            "parameters": {
                "lookback_window": 48,
                "threshold_window": 120,
                "min_periods": 24,
                "zscore_quantile": 0.75,
            }
        }
        if event_type == "ZSCORE_STRETCH"
        else {},
    )

    features = statistical_family.ZScoreStretchDetector().prepare_features(df)
    px_z = rolling_mean_std_zscore(df["close"].pct_change(12).fillna(0.0), window=48)
    px_abs = px_z.abs()
    expected = px_abs.rolling(120, min_periods=24).quantile(0.75).shift(1)

    pd.testing.assert_series_equal(features["px_abs"], px_abs, check_names=False)
    pd.testing.assert_series_equal(features["px_threshold"], expected, check_names=False)


def test_depth_stress_proxy_uses_spec_weights(monkeypatch):
    import project.events.families.canonical_proxy as canonical_proxy_family

    df = create_mock_data()
    df["spread_zscore"] = np.linspace(0.5, 3.5, len(df))
    df["micro_depth_depletion"] = np.linspace(0.1, 0.9, len(df))

    monkeypatch.setattr(
        canonical_proxy_family,
        "load_event_spec",
        lambda event_type: {
            "parameters": {
                "spread_weight": 0.50,
                "rv_weight": 0.30,
                "depth_weight": 0.20,
            }
        }
        if event_type == "DEPTH_STRESS_PROXY"
        else {},
    )

    features = canonical_proxy_family.DepthStressProxyDetector().prepare_features(df)
    expected = (
        canonical_proxy_family._safe_ratio(features["spread"], features["spread_q"]) * 0.50
        + canonical_proxy_family._safe_ratio(features["rv_z"].clip(lower=0.0), features["rv_q"])
        * 0.30
        + canonical_proxy_family._safe_ratio(features["depth_depletion"], features["depth_q"])
        * 0.20
    )

    pd.testing.assert_series_equal(features["stress_score"], expected, check_names=False)


def test_depth_collapse_uses_spec_lookback_window(monkeypatch):
    import project.events.families.canonical_proxy as canonical_proxy_family
    from project.events.thresholding import rolling_mean_std_zscore, rolling_quantile_threshold

    df = create_mock_data()
    df["spread_zscore"] = np.linspace(0.5, 3.5, len(df))
    df["micro_depth_depletion"] = np.linspace(0.1, 0.9, len(df))

    monkeypatch.setattr(
        canonical_proxy_family,
        "load_event_spec",
        lambda event_type: {
            "parameters": {
                "lookback_window": 64,
                "collapse_quantile": 0.80,
                "spread_weight": 0.45,
                "rv_weight": 0.35,
                "depth_weight": 0.20,
            }
        }
        if event_type == "DEPTH_COLLAPSE"
        else {},
    )

    features = canonical_proxy_family.DepthCollapseDetector().prepare_features(df)
    expected_rv_z = rolling_mean_std_zscore(df["rv_96"].ffill(), window=64)
    collapse_impulse = (
        features["spread"].diff().abs().fillna(0.0) + features["depth_depletion"].diff().abs().fillna(0.0)
    ).astype(float)
    expected_collapse_q = rolling_quantile_threshold(
        collapse_impulse.ffill(), quantile=0.80, window=64
    )

    pd.testing.assert_series_equal(features["rv_z"], expected_rv_z, check_names=False)
    pd.testing.assert_series_equal(features["collapse_q"], expected_collapse_q, check_names=False)


def test_statistical_spec_load_errors_are_not_suppressed(monkeypatch):
    import project.events.families.statistical as statistical_family

    df = create_mock_data()

    monkeypatch.setattr(
        statistical_family,
        "load_event_spec",
        lambda event_type: (_ for _ in ()).throw(ValueError("broken spec")),
    )

    with pytest.raises(ValueError, match="broken spec"):
        statistical_family.ZScoreStretchDetector().prepare_features(df)


def test_canonical_proxy_spec_load_errors_are_not_suppressed(monkeypatch):
    import project.events.families.canonical_proxy as canonical_proxy_family

    df = create_mock_data()
    df["spread_zscore"] = np.linspace(0.5, 3.5, len(df))
    df["micro_depth_depletion"] = np.linspace(0.1, 0.9, len(df))

    monkeypatch.setattr(
        canonical_proxy_family,
        "load_event_spec",
        lambda event_type: (_ for _ in ()).throw(ValueError("broken spec")),
    )

    with pytest.raises(ValueError, match="broken spec"):
        canonical_proxy_family.DepthStressProxyDetector().prepare_features(df)


def test_depth_collapse_requires_canonical_wide_spread_when_present():
    detector = DepthCollapseDetector()
    index = pd.RangeIndex(2)
    features = {
        "spread_z": pd.Series([4.0, 4.0], index=index),
        "rv_z": pd.Series([3.0, 3.0], index=index),
        "spread_q90": pd.Series([1.0, 1.0], index=index),
        "rv_q70": pd.Series([1.0, 1.0], index=index),
        "canonical_spread_wide": pd.Series([False, False], index=index),
    }

    mask = detector.compute_raw_mask(pd.DataFrame(index=index), features=features)

    assert not mask.any()


def test_vol_spike_requires_canonical_high_vol_state_when_present():
    detector = VolSpikeDetector()
    index = pd.RangeIndex(3)
    features = {
        "rv_z": pd.Series([3.0, 3.5, 4.0], index=index),
        "dynamic_threshold": pd.Series([1.0, 1.0, 1.0], index=index),
        "canonical_high_vol": pd.Series([False, False, False], index=index),
    }

    mask = detector.compute_raw_mask(pd.DataFrame(index=index), features=features)

    assert not mask.any()


def test_vol_relaxation_requires_prior_canonical_high_vol_state_when_present():
    detector = VolRelaxationDetector()
    index = pd.RangeIndex(3)
    features = {
        "rv_z": pd.Series([3.0, 0.5, 0.4], index=index),
        "rv_q95": pd.Series([2.0, 2.0, 2.0], index=index),
        "rv_q70": pd.Series([1.0, 1.0, 1.0], index=index),
        "canonical_from_high_vol": pd.Series([False, False, False], index=index),
    }

    mask = detector.compute_raw_mask(pd.DataFrame(index=index), features=features)

    assert not mask.any()


def test_vol_spike_uses_confident_canonical_high_vol_state_when_present():
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=4, freq="5min", tz="UTC"),
            "close": [100.0, 100.2, 100.4, 100.6],
            "rv_96": [0.01, 0.012, 0.013, 0.014],
            "range_96": [0.5, 0.5, 0.5, 0.5],
            "range_med_2880": [0.25, 0.25, 0.25, 0.25],
            "ms_vol_state": [3.0, 3.0, 3.0, 3.0],
            "ms_vol_confidence": [0.40, 0.40, 0.40, 0.40],
            "ms_vol_entropy": [0.10, 0.10, 0.10, 0.10],
        }
    )

    features = VolSpikeDetector().prepare_features(df)

    assert not features["canonical_high_vol"].any()


def test_false_breakout_distance_filtering():
    from project.events.detectors.trend import FalseBreakoutDetector

    # Create data with an 11bps breakout and a 50bps breakout
    n = 200
    close = np.full(n, 100.0)

    close[49] = 100.11  # 11bps breakout
    close[50] = 100.00  # Back in

    close[150] = 100.50  # 50bps breakout
    close[151] = 100.00  # Back in

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close": pd.Series(close),
        }
    )

    detector = FalseBreakoutDetector()
    # With default min_breakout_distance = 0.0025 (25bps), it should IGNORE the 11bps breakout
    # and ONLY detect the 50bps one.
    events = detector.detect(df, symbol="BTCUSDT", trend_window=40)

    assert len(events) == 1
    # Check detected_ts instead of timestamp because timestamp is signal_ts (next bar)
    assert events["detected_ts"].iloc[0] == df["timestamp"].iloc[151]


def test_false_breakout_direction_tracks_failed_breakout_side():
    from project.events.detectors.trend import FalseBreakoutDetector

    detector = FalseBreakoutDetector()
    features = {
        "close": pd.Series([100.0, 100.6, 99.4]),
        "rolling_max": pd.Series([100.0, 100.0, 100.0]),
        "rolling_min": pd.Series([100.0, 100.0, 100.0]),
    }

    assert detector.compute_direction(1, features) == "non_directional"
    assert detector.compute_direction(2, features) == "up"

    features = {
        "close": pd.Series([100.0, 99.4, 100.6]),
        "rolling_max": pd.Series([100.0, 100.0, 100.0]),
        "rolling_min": pd.Series([100.0, 100.0, 100.0]),
    }

    assert detector.compute_direction(2, features) == "down"


def test_deleveraging_wave_prefers_canonical_oi_decel_and_high_vol_states_when_present():
    detector = DeleveragingWaveDetector()
    index = pd.RangeIndex(2)
    features = {
        "oi_delta_1h": pd.Series([-10.0, -10.0], index=index),
        "rv_z": pd.Series([3.0, 3.0], index=index),
        "oi_q01": pd.Series([-5.0, -5.0], index=index),
        "rv_q90": pd.Series([2.0, 2.0], index=index),
        "canonical_oi_decel": pd.Series([False, False], index=index),
        "canonical_high_vol": pd.Series([False, False], index=index),
    }

    mask = detector.compute_raw_mask(pd.DataFrame(index=index), features=features)

    assert not mask.any()


def test_proxy_detectors_fail_closed_when_depth_columns_are_missing():
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=32, freq="5min", tz="UTC"),
            "close": 100.0,
            "high": 100.1,
            "low": 99.9,
        }
    )

    with pytest.raises(ValueError, match="ABSORPTION_PROXY requires columns"):
        AbsorptionProxyDetector().detect(df, symbol="BTCUSDT")

    with pytest.raises(ValueError, match="DEPTH_STRESS_PROXY requires columns"):
        DepthStressProxyDetector().detect(df, symbol="BTCUSDT")


def test_funding_flip_requires_meaningful_persistent_sign_change():
    n = 400
    funding = np.full(n, 0.0006, dtype=float)

    # Tiny oscillations around zero should not count as real flips.
    funding[290:294] = [0.00005, -0.00005, 0.00005, -0.00005]

    # One persistent and meaningful flip should count.
    funding[320:324] = [-0.0008, -0.0009, -0.00085, -0.00082]

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "funding_rate_scaled": funding,
        }
    )

    events = FundingFlipDetector().detect(df, symbol="BTCUSDT")

    assert len(events) == 1


def test_trend_exhaustion_default_is_tighter():
    df = create_mock_data()
    events = TrendExhaustionDetector().detect(df, symbol="BTCUSDT")

    assert len(events) <= 3


def test_trend_exhaustion_direction_prefers_canonical_trend_state():
    detector = TrendExhaustionDetector()
    features = {
        "canonical_trend_state": pd.Series([1.0]),
        "trend": pd.Series([-0.05]),
    }

    direction = detector.compute_direction(0, features)

    assert direction == "down"


def test_breakout_trigger_direction_tracks_breakout_side():
    detector = BreakoutTriggerDetector()
    features = {
        "close": pd.Series([101.0, 99.0]),
        "rolling_hi": pd.Series([100.0, 100.0]),
        "rolling_lo": pd.Series([100.0, 100.0]),
    }

    assert detector.compute_direction(0, features) == "up"
    assert detector.compute_direction(1, features) == "down"


def test_momentum_divergence_default_is_tighter():
    df = create_mock_data()
    events = MomentumDivergenceDetector().detect(df, symbol="BTCUSDT")

    assert len(events) <= 2


def test_momentum_divergence_accepts_canonical_chop_state():
    detector = MomentumDivergenceDetector()
    index = pd.RangeIndex(2)
    features = {
        "divergence": pd.Series([True, True], index=index),
        "divergence_turn": pd.Series([True, True], index=index),
        "extension_max": pd.Series([0.10, 0.11], index=index),
        "extension_q_threshold": pd.Series([0.05, 0.05], index=index),
        "canonical_trend_state": pd.Series([0.0, 0.0], index=index),
        "mom_slow_abs": pd.Series([2.0, 2.0], index=index),
        "slow_trend_q": pd.Series([1.0, 1.0], index=index),
        "trend_streak": pd.Series([200.0, 200.0], index=index),
        "accel_abs": pd.Series([2.0, 2.0], index=index),
        "accel_q_threshold": pd.Series([1.0, 1.0], index=index),
        "reversal_impulse": pd.Series([0.0, 0.0], index=index),
        "reversal_q70": pd.Series([1.0, 1.0], index=index),
    }

    mask = detector.compute_raw_mask(pd.DataFrame(index=index), features=features)

    assert mask.all()


def test_price_vol_imbalance_proxy_default_is_tighter():
    rng = np.random.default_rng(11)
    n = 2000
    close = pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 0.0015, n))))
    rv_96 = pd.Series(rng.uniform(0.001, 0.003, n))
    volume = pd.Series(rng.uniform(800, 1200, n))

    close.iloc[1200:1204] *= [1.0, 1.015, 1.018, 1.017]
    rv_96.iloc[1200:1204] = [0.008, 0.009, 0.010, 0.009]
    volume.iloc[1200:1204] = [2500.0, 3200.0, 3600.0, 3000.0]

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close": close,
            "high": close * 1.001,
            "low": close * 0.999,
            "rv_96": rv_96,
            "volume": volume,
        }
    )

    events = PriceVolImbalanceProxyDetector().detect(df, symbol="BTCUSDT")

    assert len(events) <= 2


def test_spread_regime_widening_requires_friction_not_just_spread():
    rng = np.random.default_rng(13)
    n = 2000
    spread_zscore = pd.Series(rng.normal(0.5, 0.3, n)).abs()
    volume = pd.Series(rng.uniform(900, 1200, n))

    spread_zscore.iloc[1500:1510] = np.linspace(2.5, 4.0, 10)
    volume.iloc[1500:1510] = np.linspace(500, 300, 10)

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "volume": volume,
            "spread_zscore": spread_zscore,
        }
    )

    events = SpreadRegimeWideningDetector().detect(df, symbol="BTCUSDT")

    assert len(events) <= 4


def test_spread_regime_widening_uses_canonical_ms_spread_state_when_present():
    rng = np.random.default_rng(131)
    n = 2000
    spread_zscore = pd.Series(rng.normal(0.5, 0.3, n)).abs()
    volume = pd.Series(rng.uniform(900, 1200, n))
    ms_spread_state = pd.Series(np.nan, index=range(n), dtype=float)

    spread_zscore.iloc[1500:1510] = np.linspace(2.5, 4.0, 10)
    volume.iloc[1500:1510] = np.linspace(500, 300, 10)
    ms_spread_state.iloc[1500:1510] = 0.0

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "volume": volume,
            "spread_zscore": spread_zscore,
            "ms_spread_state": ms_spread_state,
        }
    )

    events = SpreadRegimeWideningDetector().detect(df, symbol="BTCUSDT")

    assert events.empty


def test_spread_regime_widening_suppresses_low_confidence_canonical_spread_state():
    rng = np.random.default_rng(132)
    n = 2000
    spread_zscore = pd.Series(rng.normal(0.5, 0.3, n)).abs()
    volume = pd.Series(rng.uniform(900, 1200, n))
    ms_spread_state = pd.Series(np.nan, index=range(n), dtype=float)
    ms_spread_confidence = pd.Series(np.nan, index=range(n), dtype=float)
    ms_spread_entropy = pd.Series(np.nan, index=range(n), dtype=float)

    spread_zscore.iloc[1500:1510] = np.linspace(2.5, 4.0, 10)
    volume.iloc[1500:1510] = np.linspace(500, 300, 10)
    ms_spread_state.iloc[1500:1510] = 1.0
    ms_spread_confidence.iloc[1500:1510] = 0.40
    ms_spread_entropy.iloc[1500:1510] = 0.10

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "volume": volume,
            "spread_zscore": spread_zscore,
            "ms_spread_state": ms_spread_state,
            "ms_spread_confidence": ms_spread_confidence,
            "ms_spread_entropy": ms_spread_entropy,
        }
    )

    events = SpreadRegimeWideningDetector().detect(df, symbol="BTCUSDT")

    assert events.empty


def test_funding_persistence_preserves_sign_and_subtype():
    n = 500
    funding_rate_scaled = np.full(n, 0.0003, dtype=float)
    funding_abs_pct = np.full(n, 10.0, dtype=float)
    funding_abs = np.full(n, 0.0003, dtype=float)
    funding_rate_scaled[320:333] = -0.0016
    funding_abs_pct[320:333] = 97.0
    funding_abs[320:333] = 0.0016

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "funding_rate_scaled": funding_rate_scaled,
            "funding_abs_pct": funding_abs_pct,
            "funding_abs": funding_abs,
        }
    )

    events = FundingPersistenceDetector().detect(
        df, symbol="BTCUSDT", persistence_pct=85.0, persistence_bars=8
    )

    assert not events.empty
    assert set(events["direction"]) == {"down"}
    assert "funding_subtype" in events.columns
    assert set(events["funding_subtype"]).issubset({"acceleration", "persistence"})
    assert (events["fr_sign"] == -1.0).all()


def test_fnd_disloc_requires_canonical_funding_extreme_when_present():
    detector = FndDislocDetector()
    index = pd.RangeIndex(2)
    features = {
        "basis_bps": pd.Series([15.0, 16.0], index=index),
        "basis_zscore": pd.Series([4.0, 4.5], index=index),
        "dynamic_th": pd.Series([3.0, 3.0], index=index),
        "funding_abs": pd.Series([0.0020, 0.0021], index=index),
        "funding_q95": pd.Series([0.0010, 0.0010], index=index),
        "funding_sign": pd.Series([1.0, 1.0], index=index),
        "canonical_funding_extreme": pd.Series([False, False], index=index),
    }

    mask = detector.compute_raw_mask(pd.DataFrame(index=index), features=features)

    assert not mask.any()


def test_fnd_disloc_suppresses_low_confidence_funding_extreme_context():
    n = 400
    funding = np.full(n, 0.0020, dtype=float)
    close_spot = np.linspace(100.0, 102.0, n)
    close_perp = close_spot * 1.0015
    ms_funding_state = np.full(n, 2.0, dtype=float)
    ms_funding_confidence = np.full(n, 0.40, dtype=float)
    ms_funding_entropy = np.full(n, 0.10, dtype=float)

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close_perp": close_perp,
            "close_spot": close_spot,
            "funding_rate_scaled": funding,
            "ms_funding_state": ms_funding_state,
            "ms_funding_confidence": ms_funding_confidence,
            "ms_funding_entropy": ms_funding_entropy,
        }
    )

    features = FndDislocDetector().prepare_features(df)

    assert not features["canonical_funding_extreme"].any()


def test_funding_persistence_prefers_canonical_fp_state_when_present():
    n = 120
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "funding_rate_scaled": np.full(n, -0.0012, dtype=float),
            "funding_abs_pct": np.full(n, 10.0, dtype=float),
            "funding_abs": np.full(n, 0.0012, dtype=float),
            "fp_active": np.zeros(n, dtype=float),
            "fp_age_bars": np.zeros(n, dtype=float),
            "fp_severity": np.zeros(n, dtype=float),
        }
    )
    df.loc[80:86, "fp_active"] = 1.0
    df.loc[80:86, "fp_age_bars"] = np.arange(1, 8, dtype=float)
    df.loc[80:86, "fp_severity"] = 0.25

    events = FundingPersistenceDetector().detect(
        df, symbol="BTCUSDT", persistence_pct=85.0, persistence_bars=8
    )

    assert not events.empty
    assert set(events["funding_subtype"]) == {"persistence"}
    assert (events["fr_sign"] == -1.0).all()


def test_funding_normalization_preserves_source_sign_and_semantic_intensity():
    n = 420
    funding_rate_scaled = np.full(n, 0.0002, dtype=float)
    funding_abs_pct = np.full(n, 20.0, dtype=float)
    funding_abs = np.full(n, 0.0002, dtype=float)
    funding_rate_scaled[260:320] = 0.0015
    funding_abs_pct[260:320] = 97.0
    funding_abs[260:320] = 0.0015
    funding_rate_scaled[320:] = 0.00025
    funding_abs_pct[320:] = 25.0
    funding_abs[320:] = 0.00025

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "funding_rate_scaled": funding_rate_scaled,
            "funding_abs_pct": funding_abs_pct,
            "funding_abs": funding_abs,
        }
    )

    events = FundingNormalizationDetector().detect(
        df,
        symbol="BTCUSDT",
        extreme_pct=95.0,
        normalization_pct=50.0,
        normalization_lookback=96,
    )

    assert not events.empty
    assert set(events["direction"]) == {"up"}
    assert set(events["funding_subtype"]) == {"normalization"}
    assert (events["prior_extreme_pct"] >= 95.0).all()
    assert (events["evt_signal_intensity"] > 0.0).all()


def test_funding_normalization_ignores_low_magnitude_percentile_artifacts():
    n = 420
    funding_rate_scaled = np.full(n, 0.00003, dtype=float)
    funding_abs_pct = np.full(n, 20.0, dtype=float)
    funding_abs = np.full(n, 0.00003, dtype=float)
    funding_rate_scaled[260:320] = 0.00035
    funding_abs_pct[260:320] = 97.0
    funding_abs[260:320] = 0.00035
    funding_rate_scaled[320:] = 0.00002
    funding_abs_pct[320:] = 25.0
    funding_abs[320:] = 0.00002

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "funding_rate_scaled": funding_rate_scaled,
            "funding_abs_pct": funding_abs_pct,
            "funding_abs": funding_abs,
        }
    )

    events = FundingNormalizationDetector().detect(
        df,
        symbol="BTCUSDT",
        extreme_pct=95.0,
        normalization_pct=50.0,
        normalization_lookback=96,
    )

    assert events.empty


def test_support_resistance_break_is_implemented():
    n = 420
    close = np.full(n, 100.0, dtype=float)
    close[:360] += np.linspace(0.0, 1.0, 360)
    close[360:] = np.linspace(101.0, 106.0, n - 360)
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close": close,
        }
    )

    events = SREventDetector().detect(
        df, symbol="BTCUSDT", trend_window=96, breakout_z_threshold=1.0
    )

    assert not events.empty
    assert set(events["direction"]) == {"up"}
    assert (events["evt_signal_intensity"] > 0.0).all()


def test_detector_policy_sets_are_explicit():
    assert "LIQUIDITY_STRESS_DIRECT" in LIVE_SAFE_EVENT_TYPES
    assert "SUPPORT_RESISTANCE_BREAK" not in LIVE_SAFE_EVENT_TYPES
    assert "FUNDING_FLIP" in RETROSPECTIVE_EVENT_TYPES
    assert is_legacy_event_type("BASIS_SNAPBACK")
