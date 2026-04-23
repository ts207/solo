import numpy as np
import pandas as pd
from project.contracts.temporal_contracts import TemporalContract
from project.core.causal_primitives import trailing_percentile_rank

# --- Temporal Contract ---

TEMPORAL_CONTRACT = TemporalContract(
    name="vol_regime",
    output_mode="point_feature",
    observation_clock="bar_close",
    decision_lag_bars=1,
    lookback_bars=1440,
    uses_current_observation=False,
    calibration_mode="rolling",
    fit_scope="streaming",
    approved_primitives=("trailing_percentile_rank"),
    notes="Rolling realized volatility percentile ranks.",
)


def calculate_rv_percentile_24h(
    close_series: pd.Series, window: int = 60, lookback: int = 1440
) -> pd.Series:
    log_ret = np.log(close_series / close_series.shift(1))
    rv = log_ret.rolling(window).std()
    # Use canonical PIT-safe rank (lagged)
    return trailing_percentile_rank(rv, window=lookback, lag=1)
