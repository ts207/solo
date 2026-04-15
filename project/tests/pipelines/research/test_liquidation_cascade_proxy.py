import numpy as np
import pandas as pd
import pytest
from project.events.families.liquidation import LiquidationCascadeProxyDetector


def _base_df(n=60, oi_notional=1000.0, close=100.0) -> pd.DataFrame:
    ts = pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "oi_notional": [oi_notional] * n,
            "oi_delta_1h": [1.0] * n,  # small positive delta → tiny oi_pct_drop
            "close": [close] * n,
            "high": [close * 1.002] * n,
            "low": [close * 0.999] * n,
            "volume": [10.0] * n,
        }
    )


_SMALL_PARAMS = dict(
    oi_window=20,
    vol_window=20,
    min_periods=5,
    oi_drop_quantile=0.95,
    vol_surge_quantile=0.90,
    ret_window=3,
    price_drop_th=0.003,
    max_gap=3,
)


def test_proxy_fires_when_all_conditions_met():
    df = _base_df(n=60)
    trigger = 50

    # Large OI drop at trigger bar
    df.loc[trigger, "oi_delta_1h"] = -200.0  # −200 / 1000 = −0.2 pct drop
    # High volume
    df.loc[trigger, "volume"] = 500.0
    # Price drawdown within ret_window
    df.loc[trigger - 1, "low"] = 95.0
    df.loc[trigger, "low"] = 94.0

    detector = LiquidationCascadeProxyDetector()
    events = detector.detect(df, symbol="BTCUSDT", **_SMALL_PARAMS)

    assert len(events) == 1
    assert events.iloc[0]["event_type"] == "LIQUIDATION_CASCADE_PROXY"
    assert events.iloc[0]["symbol"] == "BTCUSDT"


def test_proxy_no_fire_without_oi_drop():
    df = _base_df(n=60)
    trigger = 50

    # Seed bars within the rolling window before trigger with large OI drops
    # to push oi_drop_th above zero. Price stays flat so those bars don't fire.
    for i in range(30, 48):
        df.loc[i, "oi_delta_1h"] = -200.0  # oi_pct_drop = 0.2

    # Trigger bar: strong volume and price drop, but OI is growing (no drop).
    # oi_pct_drop = -(1.0/1000) = -0.001, far below elevated oi_drop_th ≈ 0.2.
    df.loc[trigger, "oi_delta_1h"] = 1.0
    df.loc[trigger, "volume"] = 500.0
    df.loc[trigger, "low"] = 90.0

    detector = LiquidationCascadeProxyDetector()
    events = detector.detect(df, symbol="BTCUSDT", **_SMALL_PARAMS)

    assert len(events) == 0


def test_proxy_no_fire_without_volume_surge():
    df = _base_df(n=60)
    trigger = 50

    # Seed bars within the rolling window before trigger with large volumes
    # to push vol_th above 10.0. Price stays flat so those bars don't fire.
    for i in range(30, 48):
        df.loc[i, "volume"] = 500.0

    # Trigger bar: big OI drop and price drop, but volume is at baseline (10).
    # vol_th at trigger ≈ 500 (dominated by seeded bars), so 10 < vol_th.
    df.loc[trigger, "oi_delta_1h"] = -200.0
    df.loc[trigger, "volume"] = 10.0
    df.loc[trigger, "low"] = 90.0

    detector = LiquidationCascadeProxyDetector()
    events = detector.detect(df, symbol="BTCUSDT", **_SMALL_PARAMS)

    assert len(events) == 0


def test_proxy_no_fire_without_price_drop():
    df = _base_df(n=60)
    trigger = 50

    # OI drop and volume surge, but close to flat price
    df.loc[trigger, "oi_delta_1h"] = -200.0
    df.loc[trigger, "volume"] = 500.0
    # low stays at 99.9, close is 100 → price_drop ≈ 0.001 < 0.003 threshold
    df.loc[trigger, "low"] = 99.9

    detector = LiquidationCascadeProxyDetector()
    events = detector.detect(df, symbol="BTCUSDT", **_SMALL_PARAMS)

    assert len(events) == 0


def test_proxy_oi_reduction_and_price_drawdown_enrichment():
    df = _base_df(n=60)
    trigger = 50

    df.loc[trigger, "oi_notional"] = 800.0   # dropped from 1000
    df.loc[trigger, "oi_delta_1h"] = -200.0
    df.loc[trigger, "volume"] = 500.0
    df.loc[trigger, "low"] = 90.0  # drawdown from 100 → 10%

    detector = LiquidationCascadeProxyDetector()
    events = detector.detect(df, symbol="BTCUSDT", **_SMALL_PARAMS)

    assert len(events) == 1
    # oi_reduction_pct: (1000 - 800) / 1000 = 0.2
    assert events.iloc[0]["oi_reduction_pct"] == pytest.approx(0.2, abs=1e-6)
    # price_drawdown: (100 - 90) / 100 = 0.1
    assert events.iloc[0]["price_drawdown"] == pytest.approx(0.1, abs=1e-6)


def test_proxy_can_require_min_episode_oi_reduction():
    df = _base_df(n=60)
    trigger = 50

    df.loc[trigger, "oi_notional"] = 995.0  # net episode reduction only 0.5%
    df.loc[trigger, "oi_delta_1h"] = -200.0
    df.loc[trigger, "volume"] = 500.0
    df.loc[trigger, "low"] = 90.0

    detector = LiquidationCascadeProxyDetector()
    events = detector.detect(
        df,
        symbol="BTCUSDT",
        min_episode_oi_reduction_pct=0.01,
        **_SMALL_PARAMS,
    )

    assert len(events) == 0


def test_proxy_multi_bar_episode():
    df = _base_df(n=70)

    # Cascade spanning 3 contiguous bars
    for i in range(50, 53):
        df.loc[i, "oi_delta_1h"] = -200.0
        df.loc[i, "volume"] = 500.0
        df.loc[i, "low"] = 90.0

    detector = LiquidationCascadeProxyDetector()
    events = detector.detect(df, symbol="BTCUSDT", **_SMALL_PARAMS)

    assert len(events) == 1
    assert events.iloc[0]["duration_bars"] >= 2


def test_proxy_accepts_first_anchor_rule_alias():
    df = _base_df(n=70)

    for i in range(50, 53):
        df.loc[i, "oi_delta_1h"] = -200.0
        df.loc[i, "volume"] = 500.0
        df.loc[i, "low"] = 90.0

    detector = LiquidationCascadeProxyDetector()
    events = detector.detect(df, symbol="BTCUSDT", anchor_rule="first", **_SMALL_PARAMS)

    assert len(events) == 1
    assert int(events.iloc[0]["event_idx"]) == 50
    assert events.iloc[0]["eval_bar_ts"] == df.loc[50, "timestamp"]
    assert events.iloc[0]["anchor_ts"] == df.loc[50, "timestamp"]


def test_proxy_two_separate_episodes():
    df = _base_df(n=100)

    for trigger in [30, 80]:
        df.loc[trigger, "oi_delta_1h"] = -200.0
        df.loc[trigger, "volume"] = 500.0
        df.loc[trigger, "low"] = 90.0

    detector = LiquidationCascadeProxyDetector()
    events = detector.detect(df, symbol="BTCUSDT", **_SMALL_PARAMS)

    assert len(events) == 2


def test_proxy_uses_taker_base_volume_when_available():
    df = _base_df(n=60)
    df["taker_base_volume"] = 10.0  # same as volume baseline
    trigger = 50

    df.loc[trigger, "oi_delta_1h"] = -200.0
    df.loc[trigger, "taker_base_volume"] = 500.0  # surge in taker vol
    df.loc[trigger, "volume"] = 10.0              # regular volume stays low
    df.loc[trigger, "low"] = 90.0

    detector = LiquidationCascadeProxyDetector()
    events = detector.detect(df, symbol="BTCUSDT", **_SMALL_PARAMS)

    assert len(events) == 1


def test_proxy_empty_df_returns_no_events():
    df = pd.DataFrame(
        columns=["timestamp", "oi_notional", "oi_delta_1h", "close", "high", "low", "volume"]
    )
    detector = LiquidationCascadeProxyDetector()
    events = detector.detect(df, symbol="BTCUSDT", **_SMALL_PARAMS)
    assert len(events) == 0


def test_proxy_respects_default_warmup_window():
    df = _base_df(n=100)
    trigger = 50

    df.loc[trigger, "oi_delta_1h"] = -200.0
    df.loc[trigger, "volume"] = 500.0
    df.loc[trigger - 1, "low"] = 95.0
    df.loc[trigger, "low"] = 94.0

    detector = LiquidationCascadeProxyDetector()
    events = detector.detect(df, symbol="BTCUSDT")

    assert len(events) == 0
