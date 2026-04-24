from __future__ import annotations

import numpy as np
import pandas as pd

from project.events.detectors.exhaustion import PostDeleveragingReboundDetector


def create_synthetic_rebound_data(n=500):
    rng = np.random.default_rng(42)
    # Balanced noise
    returns = rng.normal(0, 0.0005, n)

    # 1. Inject a "forced flow" (liquidation spike + OI drop)
    # shock_start = 100, shock_len = 50
    shock_start = 100
    shock_len = 50
    returns[shock_start : shock_start + shock_len] -= 0.0030  # Sharp drop

    liq_notional = pd.Series(0.0, index=range(n))
    liq_notional[shock_start : shock_start + shock_len] = 1000.0  # Huge liquidation spike

    oi_delta_1h = pd.Series(0.0, index=range(n))
    oi_delta_1h[shock_start : shock_start + shock_len] = -500.0  # OI drop

    # 2. Inject a "rebound" (positive returns, declining vol)
    rebound_start = shock_start + shock_len
    rebound_len = 30
    returns[rebound_start : rebound_start + rebound_len] += 0.0050  # Sharp rebound

    # Volatility (rv_96)
    rv_96 = pd.Series(0.001, index=range(n))
    rv_96[shock_start : shock_start + shock_len] = 0.01  # Vol spike during shock
    # Peak at end of shock
    rv_96[rebound_start - 1] = 0.015
    # Decay during rebound
    rv_96[rebound_start : rebound_start + rebound_len] = np.linspace(0.012, 0.005, rebound_len)

    close = 100 * np.exp(np.cumsum(returns))
    close_ser = pd.Series(close)

    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC"),
            "close": close_ser,
            "high": close_ser * 1.001,
            "low": close_ser * 0.999,
            "oi_delta_1h": oi_delta_1h,
            "liquidation_notional": liq_notional,
            "rv_96": rv_96,
            "spread_bps": 3.0,
        }
    )
    return df


def test_debug_features():
    df = create_synthetic_rebound_data()
    detector = PostDeleveragingReboundDetector()
    features = detector.prepare_features(df, threshold_window=100)

    # Check forced_flow
    ff = features["forced_flow"]
    print(f"\nForced flow detected at: {df.index[ff].tolist()}")

    # Check vol_cooldown and other raw_mask components
    raw_mask = detector.compute_raw_mask(df, features=features, threshold_window=100)
    print(f"Raw mask detected at: {df.index[raw_mask].tolist()}")

    if len(df.index[raw_mask]) == 0:
        # Investigate why
        recent_cluster = (
            features["forced_flow"].rolling(48, min_periods=1).max().fillna(0).astype(bool)
        )
        cluster_direction = (
            np.sign(
                features["ret_1"]
                .where(features["forced_flow"], 0.0)
                .rolling(48, min_periods=1)
                .sum()
                .shift(1)
            )
            .replace(0.0, np.nan)
            .ffill()
            .fillna(0.0)
        )

        rebound_window = 12
        ff_cooldown = ~(
            features["forced_flow"]
            .rolling(rebound_window, min_periods=1)
            .max()
            .shift(1)
            .fillna(0)
            .astype(bool)
        )

        # Vol cooldown
        rv_peak = features["rv_96"].rolling(12, min_periods=1).max().shift(1)
        vol_cooldown = (
            (features["rv_96"] <= rv_peak * 0.95).fillna(False)
            & (
                features["liquidation_notional"]
                <= np.maximum(
                    features["liq_q85"].fillna(0.0) * 0.50,
                    500.0,
                )
            ).fillna(False)
            & (features["liq_delta"] <= 0.0).fillna(False)
        )

        rebound = (
            (features["rebound_ret"].abs() >= features["rebound_ret_q70"]).fillna(False)
            & (features["rebound_ret"].abs() >= 0.0015).fillna(False)
            & (np.sign(features["rebound_ret"]) == -cluster_direction).fillna(False)
        )

        reversal_impulse = (features["reversal_impulse"] >= features["reversal_q65"]).fillna(False)

        idx = 165
        print(f"recent_cluster at {idx}: {recent_cluster.iloc[idx]}")
        print(f"cluster_direction at {idx}: {cluster_direction.iloc[idx]}")
        print(f"ff_cooldown at {idx}: {ff_cooldown.iloc[idx]}")
        print(f"vol_cooldown at {idx}: {vol_cooldown.iloc[idx]}")
        print(f"rebound at {idx}: {rebound.iloc[idx]}")
        print(f"reversal_impulse at {idx}: {reversal_impulse.iloc[idx]}")

        # Check components of vol_cooldown
        print(f"rebound_ret at {idx}: {features['rebound_ret'].iloc[idx]}")
        print(f"rebound_ret_q70 at {idx}: {features['rebound_ret_q70'].iloc[idx]}")
        print(f"reversal_impulse at {idx}: {features['reversal_impulse'].iloc[idx]}")
        print(f"reversal_q65 at {idx}: {features['reversal_q65'].iloc[idx]}")

        # Check sign
        print(f"sign(rebound_ret) at {idx}: {np.sign(features['rebound_ret'].iloc[idx])}")
        print(f"-cluster_direction at {idx}: {-cluster_direction.iloc[idx]}")


def test_post_deleveraging_rebound_cooldown():
    df = create_synthetic_rebound_data()
    detector = PostDeleveragingReboundDetector()

    # default_rebound_window_bars = 6 (new default)
    # shock ends at 150.
    # forced_flow should stop at 150.
    # Cooldown says wait 6 bars after forced_flow.
    # So it should fire at 156 or later.

    events = detector.detect(df, symbol="BTCUSDT", threshold_window=100)

    assert len(events) > 0
    # The first event should be after the shock
    # shock_start = 100, shock_len = 50 -> shock ends at index 149.
    # bars are 5min.
    # detected_ts is the bar where the condition is met.

    first_event_idx = df.index[df["timestamp"] == events["detected_ts"].iloc[0]][0]
    assert first_event_idx >= 150 + 6  # Must respect the 6-bar cooldown


def test_post_deleveraging_rebound_no_fire_during_shock():
    df = create_synthetic_rebound_data()
    detector = PostDeleveragingReboundDetector()
    events = detector.detect(df, symbol="BTCUSDT", threshold_window=100)

    for _, event in events.iterrows():
        event_idx = df.index[df["timestamp"] == event["detected_ts"]][0]
        # Should not fire during the shock (100-149)
        assert event_idx >= 150


def test_forced_flow_cooldown_impact():
    """
    If we reduce the cooldown, it should fire earlier.
    """
    df = create_synthetic_rebound_data()
    detector = PostDeleveragingReboundDetector()

    # Default cooldown (rebound_window_bars = 12)
    events_default = detector.detect(df, symbol="BTCUSDT", threshold_window=100)
    idx_default = df.index[df["timestamp"] == events_default["detected_ts"].iloc[0]][0]

    # Shorter cooldown
    events_short = detector.detect(
        df, symbol="BTCUSDT", rebound_window_bars=4, threshold_window=100
    )
    idx_short = df.index[df["timestamp"] == events_short["detected_ts"].iloc[0]][0]

    assert idx_short < idx_default
    assert idx_short >= 150 + 4
