from __future__ import annotations

from typing import cast

import numpy as np
import pandas as pd


def trailing_quantile(
    series: pd.Series,
    *,
    window: int,
    q: float,
    lag: int = 1,
    min_periods: int | None = None,
) -> pd.Series:
    """
    Compute rolling quantile with explicit lag to ensure PIT safety.

    Parameters
    ----------
    series : pd.Series
        Input numeric series.
    window : int
        Rolling window size.
    q : float
        Quantile to compute [0, 1].
    lag : int
        Number of bars to shift the result. Default 1 (causal).
        lag=0 would include the current bar in the result's 'current' position
        but the rolling window itself always includes the current bar of the
        underlying rolling object. Shifting ensures the value at 't' only
        uses data from <= t-lag.
    min_periods : int | None
        Minimum observations in window. Defaults to window.
    """
    if min_periods is None:
        min_periods = window

    rolled = cast(pd.Series, series.rolling(window=window, min_periods=min_periods).quantile(q))
    if lag > 0:
        return cast(pd.Series, rolled.shift(lag))
    return rolled


def trailing_mean(
    series: pd.Series,
    *,
    window: int,
    lag: int = 1,
    min_periods: int | None = None,
) -> pd.Series:
    if min_periods is None:
        min_periods = window
    rolled = cast(pd.Series, series.rolling(window=window, min_periods=min_periods).mean())
    if lag > 0:
        return cast(pd.Series, rolled.shift(lag))
    return rolled


def trailing_std(
    series: pd.Series,
    *,
    window: int,
    lag: int = 1,
    min_periods: int | None = None,
) -> pd.Series:
    if min_periods is None:
        min_periods = window
    rolled = cast(pd.Series, series.rolling(window=window, min_periods=min_periods).std())
    if lag > 0:
        return cast(pd.Series, rolled.shift(lag))
    return rolled


def trailing_median(
    series: pd.Series,
    *,
    window: int,
    lag: int = 1,
    min_periods: int | None = None,
) -> pd.Series:
    if min_periods is None:
        min_periods = window
    rolled = cast(pd.Series, series.rolling(window=window, min_periods=min_periods).median())
    if lag > 0:
        return cast(pd.Series, rolled.shift(lag))
    return rolled


def trailing_percentile_rank(
    series: pd.Series,
    *,
    window: int,
    lag: int = 1,
    min_periods: int | None = None,
) -> pd.Series:
    """
    Compute rolling percentile rank of the current value relative to historical window.
    Strictly PIT: rank of series[t] relative to series[t-window-lag : t-lag].
    """
    if min_periods is None:
        min_periods = window

    def _rank(x):
        if len(x) < 2:
            return np.nan
        # Last element is the one we want to rank (the 'current' one in the window)
        # But if we want to be strictly PIT, we should rank the element at t
        # against the window ending at t-1.
        # Implementation-wise: rolling(window+1).apply(...)
        # The window includes the current point.
        current = x[-1]
        past = x[:-1]
        if np.isnan(current) or np.all(np.isnan(past)):
            return np.nan
        return (past < current).sum() / len(past)

    # We use window + 1 because the apply function receives the current point
    # and we want 'window' points of history.
    rolled = cast(pd.Series, series.rolling(window=window + 1, min_periods=min_periods + 1).apply(_rank, raw=True))
    if lag > 0:
        return cast(pd.Series, rolled.shift(lag))
    return rolled

