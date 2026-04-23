from __future__ import annotations

import math

import numpy as np
import pandas as pd


def compute_return_autocorrelation(returns: pd.Series, lag: int = 1) -> float:
    series = pd.Series(returns, copy=False).astype(float).dropna()
    if len(series) <= lag:
        return float("nan")
    return float(series.autocorr(lag=lag))


def compute_variance_ratio(returns: pd.Series, lag: int = 2) -> float:
    series = pd.Series(returns, copy=False).astype(float).dropna()
    if lag < 1:
        raise ValueError("lag must be >= 1")
    if len(series) <= lag:
        return float("nan")

    one_step_var = float(series.var(ddof=1))
    if not math.isfinite(one_step_var) or one_step_var == 0.0:
        return float("nan")

    multi_step = series.rolling(window=lag).sum().dropna()
    multi_step_var = float(multi_step.var(ddof=1))
    if not math.isfinite(multi_step_var):
        return float("nan")

    return multi_step_var / (lag * one_step_var)


def compute_hurst_exponent(
    returns: pd.Series,
    *,
    min_lag: int = 2,
    max_lag: int = 20,
) -> float:
    series = pd.Series(returns, copy=False).astype(float).dropna()
    if len(series) <= max_lag + 1:
        return float("nan")
    if min_lag < 2 or max_lag <= min_lag:
        raise ValueError("Require 2 <= min_lag < max_lag")

    walk = series.cumsum().to_numpy(dtype=float)
    lags = np.arange(min_lag, max_lag + 1, dtype=int)
    tau: list[float] = []
    valid_lags: list[int] = []
    for lag in lags:
        diffs = walk[lag:] - walk[:-lag]
        sigma = float(np.std(diffs, ddof=1))
        if math.isfinite(sigma) and sigma > 0.0:
            tau.append(sigma)
            valid_lags.append(int(lag))

    if len(valid_lags) < 2:
        return float("nan")

    slope, _ = np.polyfit(np.log(valid_lags), np.log(tau), 1)
    return float(slope)


def build_efficiency_report(returns: pd.Series, lag: int = 2) -> dict[str, float]:
    clean = pd.Series(returns, copy=False).astype(float).dropna()
    return {
        "observations": float(len(clean)),
        "variance_ratio": float(compute_variance_ratio(clean, lag=lag)),
        "hurst_exponent": float(compute_hurst_exponent(clean)),
        "return_autocorr": float(compute_return_autocorrelation(clean, lag=1)),
    }
