from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from project.core.causal_primitives import (
    trailing_mean,
    trailing_median,
    trailing_percentile_rank,
    trailing_quantile,
    trailing_std,
)
from project.events.thresholding import (
    dynamic_quantile_floor,
    ewma_zscore,
    percentile_rank,
    percentile_rank_historical,
    rolling_mean_std_zscore,
    rolling_percentile_rank,
    rolling_quantile_threshold,
    rolling_robust_zscore,
    rolling_vol_regime_factor,
)
from project.features.rolling_thresholds import lagged_rolling_quantile
from project.features import context_states as cs
from project.features import funding_persistence as fp
from project.features import liquidity_vacuum as lv
from project.features import microstructure as ms
from project.features import vol_regime as vr
from project.features import vol_shock_relaxation as vsr
from project.reliability.temporal_invariance import (
    InvarianceCheckSpec,
    PerturbationSpec,
    STANDARD_PERTURBATIONS,
    assert_future_invariance,
)


RNG = np.random.default_rng(7)


def _series_future_price_spike(series: pd.Series, cutoff_idx: int) -> pd.Series:
    out = series.copy()
    out.iloc[cutoff_idx + 1 :] = out.iloc[cutoff_idx + 1 :] * 10.0
    return out


def _series_future_missing_data(series: pd.Series, cutoff_idx: int) -> pd.Series:
    out = series.copy()
    out.iloc[cutoff_idx + 1 :] = np.nan
    return out


def _series_future_noise(series: pd.Series, cutoff_idx: int) -> pd.Series:
    out = series.copy()
    tail = len(out) - (cutoff_idx + 1)
    if tail > 0:
        noise = np.random.default_rng(12345).normal(0.0, 1.0, tail)
        out.iloc[cutoff_idx + 1 :] = out.iloc[cutoff_idx + 1 :].to_numpy(dtype=float) + noise
    return out


DF_PERTURBATIONS = STANDARD_PERTURBATIONS
SERIES_PERTURBATIONS = [
    PerturbationSpec("price_spike", _series_future_price_spike),
    PerturbationSpec("missing_data", _series_future_missing_data),
    PerturbationSpec("noise", _series_future_noise),
]



def _time_index(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")


def _base_series(n: int = 1800) -> pd.Series:
    idx = _time_index(n)
    t = np.arange(n, dtype=float)
    values = (
        100.0
        + 0.015 * t
        + 1.7 * np.sin(t / 13.0)
        + 0.9 * np.cos(t / 29.0)
        + RNG.normal(0.0, 0.08, n).cumsum() / 50.0
    )
    return pd.Series(values, index=idx, name="close")


def _market_frame(n: int = 1800) -> pd.DataFrame:
    idx = _time_index(n)
    t = np.arange(n, dtype=float)
    close = _base_series(n).to_numpy()
    high = close + 0.4 + 0.08 * np.sin(t / 11.0)
    low = close - 0.4 - 0.08 * np.cos(t / 15.0)
    volume = 1000.0 + 60.0 * np.sin(t / 19.0) + 15.0 * np.cos(t / 9.0)
    volume = np.maximum(volume + RNG.normal(0.0, 12.0, n), 25.0)
    buy_volume = volume * (0.48 + 0.06 * np.sin(t / 37.0))
    sell_volume = np.maximum(volume - buy_volume, 1.0)
    funding_rate_scaled = 0.012 * np.sin(t / 31.0) + 0.008 * np.cos(t / 47.0)
    quote_volume = close * volume * (0.97 + 0.03 * np.sin(t / 41.0))
    oi_delta_1h = 2.5 * np.sin(t / 23.0) + 0.4 * np.cos(t / 17.0)
    rv_pct = np.clip(45.0 + 28.0 * np.sin(t / 53.0) + 10.0 * np.cos(t / 71.0), 1.0, 99.0)
    trend_return = 0.002 * np.sin(t / 27.0) + 0.0012 * np.cos(t / 43.0)
    spread_z = 0.15 * np.sin(t / 15.0) + 0.45 * np.cos(t / 33.0)
    correlation = np.clip(0.52 + 0.22 * np.sin(t / 41.0), 0.0, 1.0)
    relative_vol_pct = np.clip(50.0 + 20.0 * np.cos(t / 37.0), 0.0, 100.0)

    return pd.DataFrame(
        {
            "timestamp": idx,
            "close": close,
            "high": high,
            "low": low,
            "volume": volume,
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
            "funding_rate_scaled": funding_rate_scaled,
            "funding_rate_bps": funding_rate_scaled * 100.0,
            "quote_volume": quote_volume,
            "oi_delta_1h": oi_delta_1h,
            "rv_pct": rv_pct,
            "trend_return": trend_return,
            "spread_z": spread_z,
            "correlation": correlation,
            "relative_vol_pct": relative_vol_pct,
        }
    )


def _liquidity_vacuum_frame() -> pd.DataFrame:
    df = _market_frame(260).copy()
    shock_idx = 90
    df.loc[shock_idx - 1 : shock_idx + 6, "volume"] = 1200.0
    df.loc[shock_idx, "close"] *= 1.05
    df.loc[shock_idx, "high"] = df.loc[shock_idx, "close"] * 1.02
    df.loc[shock_idx, "low"] = df.loc[shock_idx, "close"] * 0.98
    for i in range(shock_idx + 1, shock_idx + 5):
        df.loc[i, "volume"] = 90.0
        df.loc[i, "high"] = df.loc[i, "close"] * 1.06
        df.loc[i, "low"] = df.loc[i, "close"] * 0.94
    return df


def _vol_shock_relaxation_frame() -> pd.DataFrame:
    df = _market_frame(220).copy()
    shock_idx = 70
    close = df["close"].to_numpy(copy=True)
    close[shock_idx - 2 : shock_idx + 2] *= np.array([1.0, 1.0, 1.08, 1.10])
    close[shock_idx + 2 : shock_idx + 8] *= np.array([1.01, 1.00, 1.00, 0.999, 1.0, 1.0])
    df["close"] = close
    df["high"] = df["close"] * (1.01 + 0.01 * np.sin(np.arange(len(df)) / 9.0))
    df["low"] = df["close"] * (0.99 - 0.01 * np.cos(np.arange(len(df)) / 11.0))
    return df


LOOKBACK_PRIMITIVE_SPECS = [
    ("trailing_mean", lambda s: trailing_mean(s, window=48, lag=1)),
    ("trailing_std", lambda s: trailing_std(s, window=48, lag=1)),
    ("trailing_median", lambda s: trailing_median(s, window=48, lag=1)),
    ("trailing_quantile", lambda s: trailing_quantile(s, window=48, q=0.7, lag=1)),
    ("trailing_percentile_rank", lambda s: trailing_percentile_rank(s, window=48, lag=1)),
]


THRESHOLDING_SPECS = [
    ("rolling_mean_std_zscore", lambda s: rolling_mean_std_zscore(s, window=48, shift=1)),
    ("rolling_robust_zscore", lambda s: rolling_robust_zscore(s, window=48, shift=1)),
    (
        "rolling_quantile_threshold",
        lambda s: rolling_quantile_threshold(s, window=48, quantile=0.75, shift=1),
    ),
    ("ewma_zscore", lambda s: ewma_zscore(s, span=32, shift=1)),
    ("percentile_rank", lambda s: percentile_rank(s, window=48, shift=1)),
    ("rolling_percentile_rank", lambda s: rolling_percentile_rank(s, window=48, shift=1)),
    ("percentile_rank_historical", lambda s: percentile_rank_historical(s, window=48)),
    (
        "rolling_vol_regime_factor",
        lambda s: rolling_vol_regime_factor(s, window=64, shift=1),
    ),
    (
        "dynamic_quantile_floor",
        lambda s: dynamic_quantile_floor(s, window=64, quantile=0.9, floor=1.0, shift=1),
    ),
    ("lagged_rolling_quantile", lambda s: lagged_rolling_quantile(s, window=48, quantile=0.75)),
]


@pytest.mark.parametrize("name,builder", LOOKBACK_PRIMITIVE_SPECS)
def test_lookback_primitives_ignore_future_tails(name: str, builder):
    series = _base_series(800)
    spec = InvarianceCheckSpec(
        name=f"core_{name}",
        build_output=builder,
        extract_comparable_prefix=lambda out, cutoff, warmup: out.iloc[warmup : cutoff + 1],
        cutoffs=[320, 560],
        warmup=96,
        perturbations=SERIES_PERTURBATIONS,
    )
    assert_future_invariance(series, spec)


@pytest.mark.parametrize("name,builder", THRESHOLDING_SPECS)
def test_thresholding_family_ignores_future_tails(name: str, builder):
    series = _base_series(900)
    spec = InvarianceCheckSpec(
        name=f"thresholding_{name}",
        build_output=builder,
        extract_comparable_prefix=lambda out, cutoff, warmup: out.iloc[warmup : cutoff + 1],
        cutoffs=[360, 640],
        warmup=96,
        perturbations=SERIES_PERTURBATIONS,
    )
    assert_future_invariance(series, spec)


@pytest.mark.parametrize(
    "name,builder",
    [
        ("liquidity_volume", lambda s: cs.calculate_ms_liq_probabilities(s, window=64)),
        ("liquidity_state", lambda s: cs.calculate_ms_liq_state(s, window=64)),
        ("oi_probabilities", lambda s: cs.calculate_ms_oi_probabilities(s, window=64)),
        ("oi_state", lambda s: cs.calculate_ms_oi_state(s, window=64)),
        (
            "funding_probabilities",
            lambda s: cs.calculate_ms_funding_probabilities(s, window=32, window_long=96),
        ),
        (
            "funding_state",
            lambda s: cs.calculate_ms_funding_state(s, window=32, window_long=96),
        ),
        (
            "trend_probabilities",
            lambda s: cs.calculate_ms_trend_probabilities(
                s,
                rv=trailing_std(s, window=24, lag=1),
                window_long=96,
            ),
        ),
        (
            "trend_state",
            lambda s: cs.calculate_ms_trend_state(
                s,
                rv=trailing_std(s, window=24, lag=1),
                window_long=96,
            ),
        ),
    ],
)
def test_context_state_lookback_families_are_future_invariant(name: str, builder):
    series = _market_frame(900)[
        {
            "liquidity_volume": "quote_volume",
            "liquidity_state": "quote_volume",
            "oi_probabilities": "oi_delta_1h",
            "oi_state": "oi_delta_1h",
            "funding_probabilities": "funding_rate_bps",
            "funding_state": "funding_rate_bps",
            "trend_probabilities": "trend_return",
            "trend_state": "trend_return",
        }[name]
    ]
    spec = InvarianceCheckSpec(
        name=f"context_states_{name}",
        build_output=builder,
        extract_comparable_prefix=lambda out, cutoff, warmup: out.iloc[warmup : cutoff + 1],
        cutoffs=[300, 600],
        warmup=96,
        perturbations=SERIES_PERTURBATIONS,
    )
    assert_future_invariance(series, spec)


@pytest.mark.parametrize(
    "name,builder,series_key",
    [
        ("roll", lambda d: ms.calculate_roll(d["close"], window=24), "close"),
        (
            "roll_spread_bps",
            lambda d: ms.calculate_roll_spread_bps(d["close"], window=24),
            "close",
        ),
        (
            "amihud_illiquidity",
            lambda d: ms.calculate_amihud_illiquidity(d["close"], d["volume"], window=24),
            "close",
        ),
        (
            "kyle_lambda",
            lambda d: ms.calculate_kyle_lambda(d["close"], d["buy_volume"], d["sell_volume"], window=24),
            "close",
        ),
        ("vpin_score", lambda d: ms.calculate_vpin_score(d["volume"], d["buy_volume"], window=48), "volume"),
        (
            "imbalance",
            lambda d: ms.calculate_imbalance(d["buy_volume"], d["sell_volume"], window=24),
            "buy_volume",
        ),
    ],
)
def test_microstructure_family_is_future_safe(name: str, builder, series_key: str):
    frame = _market_frame(900)
    data = frame if series_key == "close" else frame
    spec = InvarianceCheckSpec(
        name=f"microstructure_{name}",
        build_output=builder,
        extract_comparable_prefix=lambda out, cutoff, warmup: out.iloc[warmup : cutoff + 1],
        cutoffs=[320, 640],
        warmup=96,
        perturbations=DF_PERTURBATIONS,
    )
    assert_future_invariance(data, spec)


@pytest.mark.parametrize(
    "name,builder",
    [
        ("rv_percentile_24h", lambda s: vr.calculate_rv_percentile_24h(s, window=24, lookback=96)),
    ],
)
def test_vol_regime_family_is_future_invariant(name: str, builder):
    series = _base_series(1200)
    spec = InvarianceCheckSpec(
        name=f"vol_regime_{name}",
        build_output=builder,
        extract_comparable_prefix=lambda out, cutoff, warmup: out.iloc[warmup : cutoff + 1],
        cutoffs=[500, 900],
        warmup=128,
        perturbations=SERIES_PERTURBATIONS,
    )
    assert_future_invariance(series, spec)


@pytest.mark.parametrize(
    "name,builder",
    [
        ("funding_persistence_state", lambda d: fp.build_funding_persistence_state(d, "BTC")),
    ],
)
def test_funding_persistence_family_is_future_invariant(name: str, builder):
    frame = _market_frame(600)
    spec = InvarianceCheckSpec(
        name=f"funding_persistence_{name}",
        build_output=builder,
        extract_comparable_prefix=lambda out, cutoff, warmup: out.iloc[warmup : cutoff + 1],
        cutoffs=[240, 420],
        warmup=120,
        perturbations=DF_PERTURBATIONS,
    )
    assert_future_invariance(frame, spec)


def _event_prefix(df: pd.DataFrame, cutoff: int, stable_margin: int = 0) -> pd.DataFrame:
    if df.empty:
        return df.copy().reset_index(drop=True)
    limit = cutoff - stable_margin
    if "exit_idx" in df.columns:
        mask = df["exit_idx"].astype(float) <= limit
    elif "enter_idx" in df.columns:
        mask = df["enter_idx"].astype(float) <= limit
    else:
        mask = df["eval_bar_ts"].notna()
    cols = [c for c in df.columns if c != "timestamp"]
    return df.loc[mask, cols].sort_values([c for c in ["enter_idx", "exit_idx", "event_id"] if c in df.columns]).reset_index(drop=True)


def test_liquidity_vacuum_detector_is_future_invariant():
    frame = _liquidity_vacuum_frame()
    cfg = lv.LiquidityVacuumConfig(
        volume_window=24,
        range_window=24,
        min_vacuum_bars=2,
        max_vacuum_bars=8,
        cooldown_bars=4,
        post_horizon_bars=24,
        auc_horizon_bars=24,
    )
    base_events = lv.detect_liquidity_vacuum_events(frame, "BTC", cfg=cfg, t_shock=0.03)
    assert not base_events.empty, "sanity check: synthetic fixture should trigger at least one event"

    spec = InvarianceCheckSpec(
        name="liquidity_vacuum_core",
        build_output=lambda d: lv._compute_core_series(d, cfg),
        extract_comparable_prefix=lambda out, cutoff, warmup: out.iloc[warmup : cutoff + 1],
        cutoffs=[120, 170],
        warmup=48,
        perturbations=DF_PERTURBATIONS,
    )
    assert_future_invariance(frame, spec)

    for cutoff in [120, 170]:
        mutated = frame.copy()
        mutated.loc[cutoff + 1 :, "close"] *= 10.0
        mutated.loc[cutoff + 1 :, ["high", "low", "volume"]] = np.nan
        alt_events = lv.detect_liquidity_vacuum_events(mutated, "BTC", cfg=cfg, t_shock=0.03)
        pd.testing.assert_frame_equal(
            _event_prefix(base_events, cutoff, stable_margin=cfg.post_horizon_bars),
            _event_prefix(alt_events, cutoff, stable_margin=cfg.post_horizon_bars),
            check_like=True,
        )


def test_vol_shock_relaxation_detector_is_future_invariant():
    frame = _vol_shock_relaxation_frame()
    cfg = vsr.VolShockRelaxationConfig(
        rv_window=5,
        baseline_window=24,
        shock_quantile=0.90,
        relax_threshold=1.15,
        relax_consecutive_bars=2,
        cooldown_bars=5,
        post_horizon_bars=24,
        auc_horizon_bars=24,
    )

    base_events, base_core, _ = vsr.detect_vol_shock_relaxation_events(frame, "BTC", cfg)
    assert not base_events.empty, "sanity check: synthetic fixture should trigger at least one event"

    for cutoff in [170, 190]:
        mutated = frame.copy()
        mutated.loc[cutoff + 1 :, "close"] *= 10.0
        mutated.loc[cutoff + 1 :, ["high", "low"]] = np.nan
        alt_events, alt_core, _ = vsr.detect_vol_shock_relaxation_events(mutated, "BTC", cfg)
        pd.testing.assert_frame_equal(
            base_core.iloc[: cutoff + 1].reset_index(drop=True),
            alt_core.iloc[: cutoff + 1].reset_index(drop=True),
        )
        pd.testing.assert_frame_equal(
            _event_prefix(base_events, cutoff, stable_margin=cfg.post_horizon_bars),
            _event_prefix(alt_events, cutoff, stable_margin=cfg.post_horizon_bars),
            check_like=True,
        )

        base_state = vsr.build_event_state_frame(base_core, base_events)
        alt_state = vsr.build_event_state_frame(alt_core, alt_events)
        pd.testing.assert_frame_equal(
            base_state.iloc[: cutoff + 1].reset_index(drop=True),
            alt_state.iloc[: cutoff + 1].reset_index(drop=True),
        )
