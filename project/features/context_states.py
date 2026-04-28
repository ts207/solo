from __future__ import annotations

import numpy as np
import pandas as pd

from project.contracts.temporal_contracts import TemporalContract
from project.core.causal_primitives import (
    trailing_mean,
    trailing_percentile_rank,
    trailing_quantile,
    trailing_std,
)

# --- Temporal Contract ---

TEMPORAL_CONTRACT = TemporalContract(
    name="context_states",
    output_mode="point_feature",
    observation_clock="bar_close",
    decision_lag_bars=1,
    lookback_bars=288,
    uses_current_observation=False,
    calibration_mode="rolling",
    fit_scope="streaming",
    approved_primitives=("trailing_quantile", "trailing_mean", "trailing_std"),
    notes="Contextual state indicators (Vol, Liq, OI, Funding, Trend, Spread). All trailing.",
)


def _sigmoid(series: pd.Series, scale: float = 1.0) -> pd.Series:
    scaled = pd.to_numeric(series, errors="coerce").astype(float) / max(scale, 1e-6)
    return 1.0 / (1.0 + np.exp(-scaled.clip(-40.0, 40.0)))


def _softmax_from_scores(scores: dict[str, pd.Series]) -> pd.DataFrame:
    frame = pd.DataFrame(scores).astype(float)
    row_max = frame.max(axis=1)
    stabilized = np.exp(frame.sub(row_max, axis=0))
    denom = stabilized.sum(axis=1).replace(0.0, np.nan)
    probs = stabilized.div(denom, axis=0)
    return probs


def _proximity_softmax(
    value: pd.Series,
    *,
    centers: dict[str, float],
    scale: float,
    mask: pd.Series | None = None,
) -> pd.DataFrame:
    numeric = pd.to_numeric(value, errors="coerce").astype(float)
    scores = {
        key: -(((numeric - center) / max(scale, 1e-6)) ** 2) for key, center in centers.items()
    }
    probs = _softmax_from_scores(scores)
    valid_mask = numeric.notna() if mask is None else mask.fillna(False)
    probs.loc[~valid_mask, :] = np.nan
    return probs


def _confidence_and_entropy(probs: pd.DataFrame, prefix: str) -> pd.DataFrame:
    numeric = probs.astype(float)
    confidence = numeric.max(axis=1)
    entropy = -(numeric * np.log(numeric.clip(lower=1e-12))).sum(axis=1)
    if numeric.shape[1] > 1:
        entropy = entropy / np.log(float(numeric.shape[1]))
    entropy = entropy.clip(lower=0.0, upper=1.0)
    confidence[numeric.isna().all(axis=1)] = np.nan
    entropy[numeric.isna().all(axis=1)] = np.nan
    return pd.DataFrame(
        {
            f"{prefix}_confidence": confidence.astype(float),
            f"{prefix}_entropy": entropy.astype(float),
        }
    )


def _state_from_probabilities(
    probs: pd.DataFrame,
    *,
    order: list[str],
    labels: dict[str, float],
) -> pd.Series:
    ordered = probs[order]
    best = ordered.fillna(-np.inf).idxmax(axis=1)
    state = best.map(labels).astype(float)
    state[ordered.isna().all(axis=1)] = np.nan
    return state


def calculate_ms_vol_probabilities(rv_pct: pd.Series) -> pd.DataFrame:
    """
    Volatility Dimension:
    0: LOW (0-33%)
    1: MID (33-66%)
    2: HIGH (66-95%)
    3: SHOCK (>95%)
    """
    rv_pct = pd.to_numeric(rv_pct, errors="coerce").astype(float)
    probs = _proximity_softmax(
        rv_pct,
        centers={
            "prob_vol_low": 16.5,
            "prob_vol_mid": 49.5,
            "prob_vol_high": 80.5,
            "prob_vol_shock": 97.5,
        },
        scale=12.5,
    )
    out = probs.copy()
    out["ms_vol_state"] = _state_from_probabilities(
        out,
        order=["prob_vol_low", "prob_vol_mid", "prob_vol_high", "prob_vol_shock"],
        labels={
            "prob_vol_low": 0.0,
            "prob_vol_mid": 1.0,
            "prob_vol_high": 2.0,
            "prob_vol_shock": 3.0,
        },
    )
    out = pd.concat([out, _confidence_and_entropy(probs, "ms_vol")], axis=1)
    return out


def calculate_ms_vol_state(rv_pct: pd.Series) -> pd.Series:
    return calculate_ms_vol_probabilities(rv_pct)["ms_vol_state"]


def calculate_ms_liq_probabilities(quote_volume: pd.Series, window: int = 288) -> pd.DataFrame:
    """
    Liquidity Dimension based on rolling 24h quote volume quantiles:
    0: THIN (Bottom 20%)
    1: NORMAL (20-80%)
    2: FLUSH (Top 20%)
    """
    min_p = min(window, max(24, window // 10))
    ranks = trailing_percentile_rank(quote_volume, window=window, lag=1, min_periods=min_p)
    ranks = ranks.where(ranks.notna(), 0.5)
    probs = _proximity_softmax(
        ranks,
        centers={
            "prob_liq_thin": 0.10,
            "prob_liq_normal": 0.50,
            "prob_liq_flush": 0.90,
        },
        scale=0.18,
        mask=ranks.notna(),
    )
    out = probs.copy()
    out["ms_liq_state"] = _state_from_probabilities(
        out,
        order=["prob_liq_thin", "prob_liq_normal", "prob_liq_flush"],
        labels={
            "prob_liq_thin": 0.0,
            "prob_liq_normal": 1.0,
            "prob_liq_flush": 2.0,
        },
    )
    out = pd.concat([out, _confidence_and_entropy(probs, "ms_liq")], axis=1)
    return out


def calculate_ms_liq_state(quote_volume: pd.Series, window: int = 288) -> pd.Series:
    return calculate_ms_liq_probabilities(quote_volume, window=window)["ms_liq_state"]


def calculate_ms_oi_probabilities(oi_delta_1h: pd.Series, window: int = 288) -> pd.DataFrame:
    """
    OI Dimension based on 1h OI delta z-score:
    0: DECEL
    1: STABLE
    2: ACCEL
    """
    min_p = min(window, max(24, window // 10))
    mean = trailing_mean(oi_delta_1h, window=window, lag=1, min_periods=min_p)
    std = trailing_std(oi_delta_1h, window=window, lag=1, min_periods=min_p)
    delta = oi_delta_1h - mean
    z = delta / std.replace(0.0, np.nan)
    z = z.mask((std == 0.0) & (delta < 0.0), -np.inf)
    z = z.mask((std == 0.0) & (delta > 0.0), np.inf)
    z = z.mask((std == 0.0) & (delta == 0.0), 0.0)
    z = z.where(z.notna(), 0.0)
    z = z.clip(lower=-4.0, upper=4.0)
    probs = _proximity_softmax(
        z,
        centers={
            "prob_oi_decel": -2.0,
            "prob_oi_stable": 0.0,
            "prob_oi_accel": 2.0,
        },
        scale=1.25,
        mask=oi_delta_1h.notna(),
    )
    out = probs.copy()
    out["ms_oi_state"] = _state_from_probabilities(
        out,
        order=["prob_oi_decel", "prob_oi_stable", "prob_oi_accel"],
        labels={
            "prob_oi_decel": 0.0,
            "prob_oi_stable": 1.0,
            "prob_oi_accel": 2.0,
        },
    )
    out = pd.concat([out, _confidence_and_entropy(probs, "ms_oi")], axis=1)
    return out


def calculate_ms_oi_state(oi_delta_1h: pd.Series, window: int = 288) -> pd.Series:
    return calculate_ms_oi_probabilities(oi_delta_1h, window=window)["ms_oi_state"]


def calculate_ms_funding_probabilities(
    funding_rate_bps: pd.Series,
    window: int = 96,
    window_long: int = 8640,
    abs_floor_bps: float = 1.0,
    persistence_multiplier: float = 1.50,
    extreme_multiplier: float = 1.50,
) -> pd.DataFrame:
    """
    Funding Dimension:
    0: NEUTRAL
    1: PERSISTENT
    2: EXTREME
    """
    min_p_short = min(window, max(12, window // 8))
    min_p_long = min(window_long, 288)

    abs_mean = trailing_mean(funding_rate_bps, window=window, lag=1, min_periods=min_p_short).abs()
    p_ext = trailing_quantile(abs_mean, window=window_long, q=0.90, lag=1, min_periods=min_p_long)
    p_65 = trailing_quantile(abs_mean, window=window_long, q=0.65, lag=1, min_periods=min_p_long)

    def _sign_consist(x: np.ndarray) -> float:
        if len(x) == 0:
            return 0.0
        pos = np.sum(x > 0)
        neg = np.sum(x < 0)
        return float(max(pos, neg) / len(x))

    consistency = (
        funding_rate_bps.rolling(window=window, min_periods=min_p_short)
        .apply(_sign_consist, raw=True)
        .shift(1)
    )
    baseline_65 = p_65.clip(lower=abs_floor_bps) * max(float(persistence_multiplier), 1.0)
    baseline_ext = p_ext.clip(lower=abs_floor_bps) * max(float(extreme_multiplier), 1.0)
    valid = funding_rate_bps.notna() & p_65.notna() & p_ext.notna() & consistency.notna()

    neutral_score = -((abs_mean / baseline_65).fillna(0.0))
    persistent_score = ((consistency - 0.80) / 0.10).fillna(-5.0) + (
        (abs_mean / baseline_65) - 1.0
    ).fillna(-5.0)
    extreme_score = (((abs_mean / baseline_ext) - 1.0) / 0.20).fillna(-5.0)
    persistent_flag = (consistency >= 0.80) & (abs_mean > baseline_65)
    extreme_flag = abs_mean > baseline_ext
    persistent_score = (persistent_score + persistent_flag.astype(float) * 2.0).where(
        persistent_flag, -5.0
    )
    extreme_score = (extreme_score + extreme_flag.astype(float) * 4.0).where(extreme_flag, -5.0)

    probs = _softmax_from_scores(
        {
            "prob_funding_neutral": neutral_score,
            "prob_funding_persistent": persistent_score,
            "prob_funding_extreme": extreme_score,
        }
    )
    if (~valid).any():
        probs.loc[~valid, :] = [1.0, 0.0, 0.0]
    probs.loc[funding_rate_bps.isna(), :] = np.nan

    out = probs.copy()
    out["ms_funding_state"] = _state_from_probabilities(
        out,
        order=["prob_funding_neutral", "prob_funding_persistent", "prob_funding_extreme"],
        labels={
            "prob_funding_neutral": 0.0,
            "prob_funding_persistent": 1.0,
            "prob_funding_extreme": 2.0,
        },
    )
    out = pd.concat([out, _confidence_and_entropy(probs, "ms_funding")], axis=1)
    return out


def calculate_ms_funding_state(
    funding_rate_bps: pd.Series,
    window: int = 96,
    window_long: int = 8640,
    abs_floor_bps: float = 1.0,
    persistence_multiplier: float = 1.50,
    extreme_multiplier: float = 1.50,
) -> pd.Series:
    return calculate_ms_funding_probabilities(
        funding_rate_bps,
        window=window,
        window_long=window_long,
        abs_floor_bps=abs_floor_bps,
        persistence_multiplier=persistence_multiplier,
        extreme_multiplier=extreme_multiplier,
    )["ms_funding_state"]


def calculate_ms_trend_probabilities(
    trend_return: pd.Series,
    rv: pd.Series | None = None,
    window_long: int = 8640,
) -> pd.DataFrame:
    """
    Trend Dimension:
    0: CHOP
    1: BULL
    2: BEAR
    """
    if rv is None:
        rv = trend_return.rolling(window=96, min_periods=12).std()

    trend_score = trend_return / (rv + 1e-6)
    min_p_long = min(window_long, 288)
    q70 = trailing_quantile(trend_score, window=window_long, q=0.70, lag=1, min_periods=min_p_long)
    q30 = trailing_quantile(trend_score, window=window_long, q=0.30, lag=1, min_periods=min_p_long)
    midpoint = (q70 + q30) / 2.0
    span = (q70 - q30).abs().replace(0.0, np.nan)
    valid = trend_return.notna() & q70.notna() & q30.notna()

    bull_score = ((trend_score - q70) / (span + 1e-6)).fillna(-5.0)
    bear_score = ((q30 - trend_score) / (span + 1e-6)).fillna(-5.0)
    chop_score = (1.0 - ((trend_score - midpoint).abs() / (span + 1e-6))).fillna(1.0)

    probs = _softmax_from_scores(
        {
            "prob_trend_chop": chop_score,
            "prob_trend_bull": bull_score,
            "prob_trend_bear": bear_score,
        }
    )
    if (~valid).any():
        probs.loc[~valid, :] = [1.0, 0.0, 0.0]
    probs.loc[trend_return.isna(), :] = np.nan

    out = probs.copy()
    out["ms_trend_state"] = _state_from_probabilities(
        out,
        order=["prob_trend_chop", "prob_trend_bull", "prob_trend_bear"],
        labels={
            "prob_trend_chop": 0.0,
            "prob_trend_bull": 1.0,
            "prob_trend_bear": 2.0,
        },
    )
    out = pd.concat([out, _confidence_and_entropy(probs, "ms_trend")], axis=1)
    return out


def calculate_ms_trend_state(
    trend_return: pd.Series,
    rv: pd.Series | None = None,
    window_long: int = 8640,
) -> pd.Series:
    return calculate_ms_trend_probabilities(trend_return, rv=rv, window_long=window_long)[
        "ms_trend_state"
    ]


def calculate_ms_spread_probabilities(spread_z: pd.Series) -> pd.DataFrame:
    """
    Spread Dimension:
    0: TIGHT
    1: WIDE
    """
    spread_z = pd.to_numeric(spread_z, errors="coerce").astype(float)
    wide = _sigmoid(spread_z - 0.5, scale=0.35)
    tight = 1.0 - wide
    probs = pd.DataFrame(
        {
            "prob_spread_tight": tight,
            "prob_spread_wide": wide,
        }
    )
    probs.loc[spread_z.isna(), :] = np.nan
    out = probs.copy()
    out["ms_spread_state"] = _state_from_probabilities(
        out,
        order=["prob_spread_tight", "prob_spread_wide"],
        labels={
            "prob_spread_tight": 0.0,
            "prob_spread_wide": 1.0,
        },
    )
    out = pd.concat([out, _confidence_and_entropy(probs, "ms_spread")], axis=1)
    return out


def calculate_ms_spread_state(spread_z: pd.Series) -> pd.Series:
    return calculate_ms_spread_probabilities(spread_z)["ms_spread_state"]


def encode_context_state_code(
    vol: pd.Series,
    liq: pd.Series,
    oi: pd.Series,
    fnd: pd.Series,
    trend: pd.Series,
    spread: pd.Series,
) -> pd.Series:
    """
    Generate ms_context_state_code as a unique permutation.
    Format: VLOFTS (Vol, Liq, OI, Funding, Trend, Spread)
    """
    code = (
        vol.fillna(0).values * 100000
        + liq.fillna(0).values * 10000
        + oi.fillna(0).values * 1000
        + fnd.fillna(0).values * 100
        + trend.fillna(0).values * 10
        + spread.fillna(0).values * 1
    )
    return pd.Series(code, index=vol.index, dtype=float)


def calculate_ms_cross_asset_probabilities(
    correlation: pd.Series,
    relative_vol_pct: pd.Series,
) -> pd.DataFrame:
    """
    Cross-Asset Dimension:

    Correlation (BTC/ETH proxy):
    0: LOW (fragmented discovery)
    1: MID (normal coupling)
    2: HIGH (index-driven regime)

    Relative Vol (Asset vs Benchmark):
    0: LOW (idiosyncratic compression)
    1: NORMAL (beta-matched)
    2: HIGH (idiosyncratic expansion)
    """
    # Correlation probabilities
    corr_probs = _proximity_softmax(
        correlation,
        centers={
            "prob_corr_low": 0.15,
            "prob_corr_mid": 0.50,
            "prob_corr_high": 0.85,
        },
        scale=0.2,
    )

    # Relative Vol probabilities (percentile rank 0-100)
    rel_vol_probs = _proximity_softmax(
        relative_vol_pct,
        centers={
            "prob_rel_vol_low": 10.0,
            "prob_rel_vol_normal": 50.0,
            "prob_rel_vol_high": 90.0,
        },
        scale=20.0,
    )

    out = pd.concat([corr_probs, rel_vol_probs], axis=1)

    out["ms_corr_state"] = _state_from_probabilities(
        out,
        order=["prob_corr_low", "prob_corr_mid", "prob_corr_high"],
        labels={"prob_corr_low": 0.0, "prob_corr_mid": 1.0, "prob_corr_high": 2.0}
    )

    out["ms_rel_vol_state"] = _state_from_probabilities(
        out,
        order=["prob_rel_vol_low", "prob_rel_vol_normal", "prob_rel_vol_high"],
        labels={"prob_rel_vol_low": 0.0, "prob_rel_vol_normal": 1.0, "prob_rel_vol_high": 2.0}
    )

    out = pd.concat([out, _confidence_and_entropy(corr_probs, "ms_corr")], axis=1)
    out = pd.concat([out, _confidence_and_entropy(rel_vol_probs, "ms_rel_vol")], axis=1)

    return out
