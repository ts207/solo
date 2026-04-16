import numpy as np
import pandas as pd

from project.contracts.temporal_contracts import TemporalContract
from project.core.causal_primitives import (
    trailing_quantile,
    trailing_mean,
    trailing_std,
    trailing_median,
    trailing_percentile_rank,
)

# --- Temporal Contract ---

TEMPORAL_CONTRACT = TemporalContract(
    name="thresholding_primitives",
    output_mode="point_feature",
    observation_clock="bar_close",
    decision_lag_bars=1,
    lookback_bars=None,  # Variable per caller
    uses_current_observation=False,
    calibration_mode="rolling",
    fit_scope="streaming",
    approved_primitives=("trailing_quantile", "trailing_mean", "trailing_std", "trailing_median"),
    notes="Official PIT-safe primitive library. Default lag is 1.",
)


def _series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype(float)


def rolling_mean_std_zscore(
    series: pd.Series,
    *,
    window: int,
    min_periods: int | None = None,
    shift: int = 1,
) -> pd.Series:
    """
    Compute Z-score of current value [t] relative to rolling window ending at [t-shift].
    PIT-safe by default (shift=1).
    """
    s = _series(series)
    mean = trailing_mean(s, window=window, lag=shift, min_periods=min_periods)
    std = trailing_std(s, window=window, lag=shift, min_periods=min_periods).replace(0.0, np.nan)
    return (s - mean) / std


def rolling_robust_zscore(
    series: pd.Series,
    *,
    window: int,
    min_periods: int | None = None,
    shift: int = 1,
) -> pd.Series:
    """
    Compute Robust Z-score (using MAD) of current value [t] relative to window [t-window-shift : t-shift].
    PIT-safe by default (shift=1).
    """
    s = _series(series)
    median = trailing_median(s, window=window, lag=shift, min_periods=min_periods)

    # MAD also needs to be computed on the shifted baseline
    baseline = s.shift(int(shift)) if shift else s
    rolling = baseline.rolling(window=window, min_periods=min_periods or window)

    def _mad(values: np.ndarray) -> float:
        valid = values[np.isfinite(values)]
        if len(valid) == 0:
            return np.nan
        med = np.median(valid)
        return float(np.median(np.abs(valid - med)))

    mad = rolling.apply(_mad, raw=True)
    std = baseline.rolling(window=window, min_periods=min_periods or window).std()
    denom = (1.4826 * mad).where(mad > 0.0, std)
    denom = denom.where(denom > 0.0, 1e-12)
    return (s - median) / denom


def rolling_quantile_threshold(
    series: pd.Series,
    *,
    window: int,
    quantile: float,
    min_periods: int | None = None,
    shift: int = 1,
) -> pd.Series:
    """
    Compute rolling quantile threshold. PIT-safe by default (shift=1).
    """
    return trailing_quantile(series, window=window, q=quantile, lag=shift, min_periods=min_periods)


def ewma_zscore(
    series: pd.Series,
    *,
    span: int,
    min_periods: int | None = None,
    shift: int = 1,
) -> pd.Series:
    s = _series(series)
    baseline = s.shift(int(shift)) if shift else s
    mean = baseline.ewm(span=span, adjust=False, min_periods=min_periods or 1).mean()
    var = baseline.ewm(span=span, adjust=False, min_periods=min_periods or 1).var()
    std = np.sqrt(var).replace(0.0, np.nan)
    return (s - mean) / std


def percentile_rank(
    series: pd.Series,
    *,
    window: int,
    min_periods: int | None = None,
    shift: int = 1,
    scale: float = 100.0,
) -> pd.Series:
    s = _series(series)
    mp = min_periods or window
    out = pd.Series(np.nan, index=s.index, dtype=float)
    hist_offset = int(shift)
    for idx in range(len(s)):
        current = s.iloc[idx]
        if not np.isfinite(current):
            continue
        hist_end = idx - hist_offset
        if hist_end < 0:
            continue
        hist_start = max(0, hist_end - window + 1)
        hist = s.iloc[hist_start : hist_end + 1]
        valid = hist[np.isfinite(hist)]
        if len(valid) < mp:
            continue
        out.iloc[idx] = float((valid <= current).sum() / len(valid) * scale)
    return out


def rolling_percentile_rank(
    series: pd.Series,
    *,
    window: int,
    min_periods: int | None = None,
    shift: int = 1,
    scale: float = 1.0,
) -> pd.Series:
    """
    Compute rolling percentile rank. PIT-safe by default (shift=1).
    """
    return (
        trailing_percentile_rank(series, window=window, lag=shift, min_periods=min_periods) * scale
    )


def percentile_rank_historical(
    series: pd.Series,
    *,
    window: int,
    min_periods: int | None = None,
    scale: float = 100.0,
) -> pd.Series:
    return percentile_rank(series, window=window, min_periods=min_periods, shift=1, scale=scale)


def state_conditioned_threshold(
    base_threshold: float | pd.Series,
    state: pd.Series,
    *,
    multipliers: dict[str, float] | None = None,
    default_multiplier: float = 1.0,
) -> pd.Series:
    state_series = state.astype(str).str.lower()
    mult = pd.Series(default_multiplier, index=state.index, dtype=float)
    for key, value in (multipliers or {}).items():
        mult = mult.where(state_series != str(key).lower(), float(value))
    if isinstance(base_threshold, pd.Series):
        base = _series(base_threshold)
    else:
        base = pd.Series(float(base_threshold), index=state.index, dtype=float)
    return base * mult


def rolling_vol_regime_factor(
    rv: pd.Series,
    *,
    window: int = 2880,
    min_periods: int | None = None,
    shift: int = 1,
) -> pd.Series:
    """Computes a volatility regime scaling factor based on current RV vs its rolling mean."""
    s = _series(rv)
    mp = min_periods or window // 4
    baseline = s.shift(int(shift)) if shift else s
    rv_mean = baseline.rolling(window=window, min_periods=mp).mean().replace(0.0, np.nan)
    return (s / rv_mean).fillna(1.0)


def dynamic_quantile_floor(
    series: pd.Series,
    *,
    window: int = 2880,
    quantile: float = 0.95,
    floor: float | pd.Series = 2.0,
    min_periods: int | None = None,
    shift: int = 1,
) -> pd.Series:
    """Computes a rolling quantile threshold with a hard floor."""
    q_th = rolling_quantile_threshold(
        series, window=window, quantile=quantile, min_periods=min_periods, shift=shift
    )

    if isinstance(floor, pd.Series):
        floor_series = floor.reindex(q_th.index).astype(float)
        return q_th.where(q_th >= floor_series, floor_series)

    return q_th.where(q_th >= float(floor), float(floor))
