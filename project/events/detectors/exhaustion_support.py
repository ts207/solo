from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Any, Mapping
from project.features.rolling_thresholds import lagged_rolling_quantile
from project.events.thresholding import rolling_mean_std_zscore
from project.features.context_guards import optional_state


def onset_mask(mask: pd.Series) -> pd.Series:
    """Detect the first True in a sequence of Trues."""
    typed = mask.astype("boolean")
    return (typed & ~typed.shift(1, fill_value=False)).astype(bool)


def prepare_flow_exhaustion_features(
    df: pd.DataFrame, defaults: Mapping[str, Any], params: Mapping[str, Any]
) -> dict[str, pd.Series]:
    """Extracted feature preparation for FlowExhaustionDetector."""
    close = pd.to_numeric(df["close"], errors="coerce").astype(float)
    rv_96 = pd.to_numeric(df["rv_96"], errors="coerce").ffill().astype(float)
    oi_delta = pd.to_numeric(
        df.get("oi_delta_1h", pd.Series(0.0, index=df.index)), errors="coerce"
    ).fillna(0.0)
    liq_notional = pd.to_numeric(
        df.get("liquidation_notional", pd.Series(0.0, index=df.index)), errors="coerce"
    ).fillna(0.0)
    spread_bps = pd.to_numeric(
        df.get("spread_bps", pd.Series(0.0, index=df.index)), errors="coerce"
    ).fillna(0.0)
    ret_1 = close.pct_change(periods=1).fillna(0.0)
    ret_abs = ret_1.abs()

    oi_drop = (-oi_delta).clip(lower=0.0)

    window = int(params.get("threshold_window", defaults["threshold_window"]))
    min_periods = max(window // 10, 1)

    oi_drop_q = lagged_rolling_quantile(
        oi_drop,
        window=window,
        quantile=float(params.get("oi_drop_quantile", defaults["oi_drop_quantile"])),
        min_periods=min_periods,
    )
    liq_q = lagged_rolling_quantile(
        liq_notional,
        window=window,
        quantile=float(params.get("liquidation_quantile", defaults["liquidation_quantile"])),
        min_periods=min_periods,
    )
    spread_q = lagged_rolling_quantile(
        spread_bps,
        window=window,
        quantile=float(params.get("spread_quantile", defaults["spread_quantile"])),
        min_periods=min_periods,
    )
    ret_q = lagged_rolling_quantile(
        ret_abs,
        window=window,
        quantile=float(params.get("return_quantile", defaults["return_quantile"])),
        min_periods=min_periods,
    )

    rebound_ret = close.pct_change(
        periods=int(params.get("rebound_window", defaults["rebound_window"]))
    ).fillna(0.0)
    reversal_impulse = close.pct_change(
        periods=int(params.get("reversal_window", defaults["reversal_window"]))
    ).abs()
    reversal_q = lagged_rolling_quantile(
        reversal_impulse,
        window=window,
        quantile=float(params.get("reversal_quantile", defaults["reversal_quantile"])),
        min_periods=min_periods,
    )

    oi_drop_abs_min = float(params.get("oi_drop_abs_min", defaults["oi_drop_abs_min"]))
    liquidation_abs_min = float(params.get("liquidation_abs_min", defaults["liquidation_abs_min"]))
    liquidation_multiplier = float(
        params.get("liquidation_multiplier", defaults["liquidation_multiplier"])
    )
    return_abs_min = float(params.get("return_abs_min", defaults["return_abs_min"]))
    spread_abs_min = float(params.get("spread_abs_min", defaults["spread_abs_min"]))

    forced_flow = (
        (oi_drop >= np.maximum(oi_drop_q.fillna(0.0), oi_drop_abs_min)).fillna(False)
        & (liq_notional >= np.maximum(liq_q.fillna(0.0), liquidation_abs_min)).fillna(False)
    ) | (
        (
            liq_notional
            >= np.maximum(
                liq_q.fillna(0.0) * liquidation_multiplier,
                liquidation_abs_min,
            )
        ).fillna(False)
        & (ret_abs >= np.maximum(ret_q.fillna(0.0), return_abs_min)).fillna(False)
        & (
            spread_bps
            >= np.maximum(
                spread_q.fillna(0.0),
                spread_abs_min,
            )
        ).fillna(False)
    )

    rv_curr = rv_96
    rv_prev = rv_96.shift(1)
    rv_decay_ratio = float(params.get("rv_decay_ratio", defaults["rv_decay_ratio"]))
    exhaustion = (rv_curr < rv_prev).fillna(False) & (rv_curr <= rv_prev * rv_decay_ratio).fillna(
        False
    )
    direction = np.sign(ret_1).replace(0.0, np.nan).ffill().fillna(0.0)

    return {
        "direction": pd.Series(direction, index=df.index),
        "ret_1": ret_1,
        "rebound_ret": rebound_ret,
        "oi_drop": oi_drop,
        "oi_drop_q": oi_drop_q,
        "liquidation_notional": liq_notional,
        "liq_q": liq_q,
        "spread_bps": spread_bps,
        "spread_q": spread_q,
        "ret_abs": ret_abs,
        "ret_q": ret_q,
        "forced_flow": forced_flow,
        "exhaustion": exhaustion,
        "rv_96": rv_96,
        "reversal_impulse": reversal_impulse,
        "reversal_q": reversal_q,
    }


def prepare_post_deleveraging_features(
    df: pd.DataFrame, defaults: Mapping[str, Any], params: Mapping[str, Any]
) -> dict[str, pd.Series]:
    """Extracted feature preparation for PostDeleveragingReboundDetector."""
    close = pd.to_numeric(df["close"], errors="coerce").astype(float)
    rv_96 = pd.to_numeric(df["rv_96"], errors="coerce").ffill().astype(float)
    oi_delta = pd.to_numeric(
        df.get("oi_delta_1h", pd.Series(0.0, index=df.index)), errors="coerce"
    ).fillna(0.0)
    liq_notional = pd.to_numeric(
        df.get("liquidation_notional", pd.Series(0.0, index=df.index)),
        errors="coerce",
    ).fillna(0.0)
    spread_bps = pd.to_numeric(
        df.get("spread_bps", pd.Series(0.0, index=df.index)), errors="coerce"
    ).fillna(0.0)
    ret_1 = close.pct_change(periods=1).fillna(0.0)
    ret_abs = ret_1.abs()
    oi_drop = (-oi_delta).clip(lower=0.0)
    liq_delta = liq_notional.diff().fillna(0.0)
    wick_ratio = pd.Series(0.0, index=df.index, dtype=float)
    if "high" in df.columns and "low" in df.columns:
        high = pd.to_numeric(df["high"], errors="coerce").astype(float)
        low = pd.to_numeric(df["low"], errors="coerce").astype(float)
        open_proxy = close.shift(1).fillna(close)
        body = (close - open_proxy).abs()
        wick = (
            (high - np.maximum(open_proxy, close)) + (np.minimum(open_proxy, close) - low)
        ).clip(lower=0.0)
        wick_ratio = (wick / (body + wick).replace(0.0, np.nan)).fillna(0.0)

    window = int(params.get("threshold_window", defaults["threshold_window"]))
    min_periods = max(window // 10, 1)
    oi_drop_q80 = lagged_rolling_quantile(
        oi_drop,
        window=window,
        quantile=float(params.get("oi_drop_quantile", defaults["oi_drop_quantile"])),
        min_periods=min_periods,
    )
    liq_q85 = lagged_rolling_quantile(
        liq_notional,
        window=window,
        quantile=float(params.get("liquidation_quantile", defaults["liquidation_quantile"])),
        min_periods=min_periods,
    )
    spread_q70 = lagged_rolling_quantile(
        spread_bps,
        window=window,
        quantile=float(params.get("spread_quantile", defaults["spread_quantile"])),
        min_periods=min_periods,
    )
    ret_q75 = lagged_rolling_quantile(
        ret_abs,
        window=window,
        quantile=float(params.get("return_quantile", defaults["return_quantile"])),
        min_periods=min_periods,
    )
    wick_q70 = lagged_rolling_quantile(
        wick_ratio,
        window=window,
        quantile=float(params.get("wick_quantile", defaults["wick_quantile"])),
        min_periods=min_periods,
    )

    rebound_window = int(params.get("rebound_window", defaults["rebound_window"]))
    rebound_ret = close.pct_change(periods=rebound_window).fillna(0.0)
    rebound_ret_q70 = lagged_rolling_quantile(
        rebound_ret.abs(),
        window=window,
        quantile=float(params.get("rebound_quantile", defaults["rebound_quantile"])),
        min_periods=min_periods,
    )

    reversal_window = int(params.get("reversal_window", defaults["reversal_window"]))
    reversal_impulse = close.pct_change(periods=reversal_window).abs()
    reversal_q65 = lagged_rolling_quantile(
        reversal_impulse,
        window=window,
        quantile=float(params.get("reversal_quantile", defaults["reversal_quantile"])),
        min_periods=min_periods,
    )

    oi_drop_abs_min = float(params.get("oi_drop_abs_min", defaults["oi_drop_abs_min"]))
    liquidation_abs_min = float(params.get("liquidation_abs_min", defaults["liquidation_abs_min"]))
    liquidation_multiplier = float(
        params.get("liquidation_multiplier", defaults["liquidation_multiplier"])
    )
    return_abs_min = float(params.get("return_abs_min", defaults["return_abs_min"]))
    spread_abs_min = float(params.get("spread_abs_min", defaults["spread_abs_min"]))

    forced_flow = (
        (oi_drop >= np.maximum(oi_drop_q80.fillna(0.0), oi_drop_abs_min)).fillna(False)
        & (liq_notional >= np.maximum(liq_q85.fillna(0.0), liquidation_abs_min)).fillna(False)
    ) | (
        (
            liq_notional
            >= np.maximum(
                liq_q85.fillna(0.0) * liquidation_multiplier,
                liquidation_abs_min,
            )
        ).fillna(False)
        & (ret_abs >= np.maximum(ret_q75.fillna(0.0), return_abs_min)).fillna(False)
        & (
            spread_bps
            >= np.maximum(
                spread_q70.fillna(0.0),
                spread_abs_min,
            )
        ).fillna(False)
    )
    cluster_direction = (
        np.sign(ret_1.where(forced_flow, 0.0)).replace(0.0, np.nan).ffill().fillna(0.0)
    )
    return {
        "close": close,
        "ret_1": ret_1,
        "ret_abs": ret_abs,
        "rv_96": rv_96,
        "oi_drop": oi_drop,
        "oi_drop_q80": oi_drop_q80,
        "liquidation_notional": liq_notional,
        "liq_q85": liq_q85,
        "liq_delta": liq_delta,
        "spread_bps": spread_bps,
        "spread_q70": spread_q70,
        "forced_flow": forced_flow,
        "cluster_direction": pd.Series(cluster_direction, index=df.index),
        "rebound_ret": rebound_ret,
        "rebound_ret_q70": rebound_ret_q70,
        "reversal_impulse": reversal_impulse,
        "reversal_q65": reversal_q65,
        "wick_ratio": wick_ratio,
        "wick_q70": wick_q70,
    }


def compute_post_deleveraging_mask(
    features: Mapping[str, pd.Series], defaults: Mapping[str, Any], params: Mapping[str, Any]
) -> pd.Series:
    """Extracted mask computation for PostDeleveragingReboundDetector."""
    cluster_window = int(params.get("cluster_window", defaults["cluster_window"]))
    rebound_window = int(params.get("rebound_window_bars", defaults["rebound_window_bars"]))
    post_cluster_lookback = int(
        params.get("post_cluster_lookback", defaults["post_cluster_lookback"])
    )
    forced_flow = features["forced_flow"].fillna(False)
    recent_cluster = (
        forced_flow.rolling(post_cluster_lookback, min_periods=1).max().fillna(0).astype(bool)
    )
    cluster_direction = (
        np.sign(
            features["ret_1"]
            .where(forced_flow, 0.0)
            .rolling(post_cluster_lookback, min_periods=1)
            .sum()
            .shift(1)
        )
        .replace(0.0, np.nan)
        .ffill()
        .fillna(0.0)
    )

    rv_peak = features["rv_96"].rolling(cluster_window, min_periods=1).max().shift(1)
    rv_peak_decay_ratio = float(params.get("rv_peak_decay_ratio", defaults["rv_peak_decay_ratio"]))
    liq_cooldown_ratio = float(params.get("liq_cooldown_ratio", defaults["liq_cooldown_ratio"]))
    liquidation_cooldown_abs_max = float(
        params.get("liquidation_cooldown_abs_max", defaults["liquidation_cooldown_abs_max"])
    )

    vol_cooldown = (
        (features["rv_96"] <= rv_peak * rv_peak_decay_ratio).fillna(False)
        & (
            features["liquidation_notional"]
            <= np.maximum(
                features["liq_q85"].fillna(0.0) * liq_cooldown_ratio,
                liquidation_cooldown_abs_max,
            )
        ).fillna(False)
        & (features["liq_delta"] <= 0.0).fillna(False)
    )

    rebound_return_min = float(params.get("rebound_return_min", defaults["rebound_return_min"]))
    rebound = (
        (features["rebound_ret"].abs() >= features["rebound_ret_q70"]).fillna(False)
        & (features["rebound_ret"].abs() >= rebound_return_min).fillna(False)
        & (np.sign(features["rebound_ret"]) == -cluster_direction).fillna(False)
    )
    reversal_impulse = (features["reversal_impulse"] >= features["reversal_q65"]).fillna(False)

    wick_ratio_min = float(params.get("wick_ratio_min", defaults["wick_ratio_min"]))
    wick_confirm = (features["wick_ratio"] >= features["wick_q70"]).fillna(False) | (
        features["wick_ratio"] >= wick_ratio_min
    ).fillna(False)
    return (
        recent_cluster
        & ~forced_flow
        & cluster_direction.ne(0.0)
        & ~(
            forced_flow.rolling(rebound_window, min_periods=1).max().shift(1).fillna(0).astype(bool)
        )
        & vol_cooldown
        & rebound
        & (reversal_impulse | wick_confirm)
    ).fillna(False)


def prepare_trend_exhaustion_features(
    df: pd.DataFrame, defaults: Mapping[str, Any], params: Mapping[str, Any]
) -> dict[str, pd.Series]:
    """Extracted feature preparation for TrendExhaustionDetector."""
    close = pd.to_numeric(df["close"], errors="coerce").astype(float)
    rv_96 = pd.to_numeric(df["rv_96"], errors="coerce").ffill().astype(float)
    raw_canonical_trend_state = optional_state(df, "ms_trend_state")
    canonical_trend_state = optional_state(
        df,
        "ms_trend_state",
        min_confidence=float(
            params.get("context_min_confidence", defaults.get("context_min_confidence", 0.55))
        ),
        max_entropy=float(
            params.get("context_max_entropy", defaults.get("context_max_entropy", 0.90))
        ),
    )

    trend_window = int(params.get("trend_window", defaults.get("trend_window", 96)))
    trend = close.pct_change(periods=trend_window)
    trend_abs = trend.abs()
    trend_sign = np.sign(trend).fillna(0.0)
    trend_group = trend_sign.ne(trend_sign.shift(1)) | trend_sign.eq(0.0)
    trend_streak = trend_sign.groupby(trend_group.cumsum()).cumcount() + 1
    trend_streak = trend_streak.where(trend_sign != 0.0, 0).astype(float)

    vol_window = int(params.get("vol_window", defaults.get("vol_window", 288)))
    rv_z = rolling_mean_std_zscore(rv_96, window=vol_window)
    rv_median = rv_96.rolling(vol_window, min_periods=12).median().shift(1)

    slope_fast = close.diff(
        int(params.get("slope_fast_window", defaults.get("slope_fast_window", 12)))
    )
    slope_slow = close.diff(
        int(params.get("slope_slow_window", defaults.get("slope_slow_window", 48)))
    )

    pullback_window = int(params.get("pullback_window", defaults.get("pullback_window", 96)))
    rolling_high = close.rolling(pullback_window, min_periods=12).max().shift(1)
    rolling_low = close.rolling(pullback_window, min_periods=12).min().shift(1)
    pullback_up = ((rolling_high - close) / rolling_high.replace(0.0, np.nan)).clip(lower=0.0)
    pullback_down = ((close - rolling_low) / rolling_low.replace(0.0, np.nan)).clip(lower=0.0)

    threshold_window = int(params.get("threshold_window", defaults.get("threshold_window", 2880)))
    min_periods = max(threshold_window // 10, 1)
    trend_q_extreme = lagged_rolling_quantile(
        trend_abs,
        window=threshold_window,
        quantile=float(params.get("trend_quantile", defaults.get("trend_quantile", 0.95))),
        min_periods=min_periods,
    )
    trend_median = trend_abs.rolling(trend_window, min_periods=12).median().shift(1)

    rv_q35 = lagged_rolling_quantile(
        rv_z,
        window=threshold_window,
        quantile=float(params.get("cooldown_quantile", defaults.get("cooldown_quantile", 0.35))),
        min_periods=min_periods,
    )

    pullback_quantile = float(
        params.get("pullback_quantile", defaults.get("pullback_quantile", 0.70))
    )
    pullback_q70 = lagged_rolling_quantile(
        pd.concat([pullback_up, pullback_down], axis=1).max(axis=1),
        window=threshold_window,
        quantile=pullback_quantile,
        min_periods=min_periods,
    )

    reversal_window = int(params.get("reversal_window", defaults.get("reversal_window", 3)))
    reversal_impulse = close.pct_change(periods=reversal_window).abs()
    reversal_q65 = lagged_rolling_quantile(
        reversal_impulse,
        window=threshold_window,
        quantile=float(params.get("reversal_quantile", defaults.get("reversal_quantile", 0.65))),
        min_periods=min_periods,
    )

    reversal_alignment_window = int(
        params.get("reversal_alignment_window", defaults.get("reversal_alignment_window", 3))
    )
    reversal_confirmed = (reversal_impulse >= reversal_q65).fillna(False) | (
        (pullback_up >= pullback_q70).fillna(False) | (pullback_down >= pullback_q70).fillna(False)
    )
    any_reversal = (
        (
            (trend.shift(1) > 0).fillna(False)
            & ((slope_fast <= 0).fillna(False) | (pullback_up >= pullback_q70).fillna(False))
        )
        | (
            (trend.shift(1) < 0).fillna(False)
            & ((slope_fast >= 0).fillna(False) | (pullback_down >= pullback_q70).fillna(False))
        )
        | ((slope_fast * slope_slow) <= 0).fillna(False)
        | reversal_confirmed
    ).fillna(False)
    any_reversal_flex = (
        any_reversal.rolling(window=reversal_alignment_window, min_periods=1).max().astype(bool)
    )

    return {
        "trend": trend,
        "trend_abs": trend_abs,
        "trend_streak": trend_streak,
        "rv_z": rv_z,
        "rv_96": rv_96,
        "rv_median": rv_median,
        "slope_fast": slope_fast,
        "slope_slow": slope_slow,
        "pullback_up": pullback_up,
        "pullback_down": pullback_down,
        "trend_q_extreme": trend_q_extreme,
        "trend_median": trend_median,
        "rv_q35": rv_q35,
        "pullback_q70": pullback_q70,
        "reversal_impulse": reversal_impulse,
        "reversal_q65": reversal_q65,
        "canonical_trend_state": canonical_trend_state,
        "canonical_trend_present": raw_canonical_trend_state.notna(),
        "any_reversal_flex": any_reversal_flex,
    }


def compute_trend_exhaustion_mask(
    features: Mapping[str, pd.Series], defaults: Mapping[str, Any], params: Mapping[str, Any]
) -> pd.Series:
    """Extracted mask computation for TrendExhaustionDetector."""
    # 1. Structural Signal: Trend must be at a historical extreme
    trend_peak_multiplier = float(
        params.get("trend_peak_multiplier", defaults.get("trend_peak_multiplier", 1.30))
    )
    trend_strength_ratio = float(
        params.get("trend_strength_ratio", defaults.get("trend_strength_ratio", 3.0))
    )
    min_trend_duration_bars = int(
        params.get("min_trend_duration_bars", defaults.get("min_trend_duration_bars", 72))
    )

    trend_peak = (
        features["trend_abs"] >= features["trend_q_extreme"] * trend_peak_multiplier
    ).fillna(False) | (
        features["trend_abs"] >= features["trend_median"] * trend_strength_ratio
    ).fillna(False)
    sustained_trend = (features["trend_streak"] >= min_trend_duration_bars).fillna(False)
    canonical_trend_state = features["canonical_trend_state"]
    canonical_trend_present = features.get(
        "canonical_trend_present",
        canonical_trend_state.notna(),
    )
    canonical_trend_active = (
        pd.Series(True, index=canonical_trend_state.index, dtype=bool)
        if not canonical_trend_present.any()
        else canonical_trend_state.isin([1.0, 2.0]).fillna(False)
    )

    # 2. Cooldown Guard: Volatility must be decelerating or low
    cooldown_ratio = float(params.get("cooldown_ratio", defaults.get("cooldown_ratio", 0.90)))
    cooldown = (features["rv_z"] <= features["rv_q35"]).fillna(False) | (
        features["rv_96"] <= features["rv_median"] * cooldown_ratio
    ).fillna(False)

    return (
        trend_peak
        & sustained_trend
        & canonical_trend_active
        & cooldown
        & features["any_reversal_flex"]
    ).fillna(False)
