from __future__ import annotations

import pandas as pd


def lagged_rolling_quantile(
    series: pd.Series,
    *,
    window: int,
    quantile: float,
    min_periods: int | None = None,
    lag: int = 1,
) -> pd.Series:
    effective_min_periods = window if min_periods is None else min_periods
    return (
        series.rolling(window=window, min_periods=effective_min_periods)
        .quantile(quantile)
        .shift(lag)
    )
