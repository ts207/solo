
import numpy as np
import pandas as pd

from project.contracts.temporal_contracts import TemporalContract

# --- Temporal Contract ---

TEMPORAL_CONTRACT = TemporalContract(
    name="microstructure",
    output_mode="point_feature",
    observation_clock="bar_close",
    decision_lag_bars=1,
    lookback_bars=24,
    uses_current_observation=False,
    calibration_mode="rolling",
    fit_scope="streaming",
    approved_primitives=("trailing_mean"),
    notes="Microstructure indicators (Roll, Amihud, Kyle, VPIN). All lagged.",
)


def calculate_roll(close_series: pd.Series, window: int = 24) -> pd.Series:
    """
    Standard Roll measure (raw units).
    """
    diff = close_series.diff()
    # Lagged covariance to ensure PIT
    cov = diff.rolling(window).cov(diff.shift(1))
    return 2 * np.sqrt(np.maximum(0, -cov))


def calculate_roll_spread_bps(close: pd.Series, window: int = 24) -> pd.Series:
    """
    Roll Spread (bps) = 2 * sqrt(-cov(dp_t, dp_{t-1})) / price * 10000
    Matches spec: roll_spread_bps
    """
    diff = close.diff()
    # Lagged covariance
    cov = diff.rolling(window).cov(diff.shift(1))
    roll_spread = 2 * np.sqrt(np.maximum(0, -cov)) / close.shift(1) * 10000
    return roll_spread


def calculate_amihud_illiquidity(
    close: pd.Series, volume: pd.Series, window: int = 24
) -> pd.Series:
    """
    Amihud Illiquidity = avg(|return| / dollar_volume)
    Matches spec: amihud_illiquidity
    """
    log_ret = np.log(close / close.shift(1))
    dollar_vol = close * volume
    # Use replacement of 0 with NaN to avoid division by zero and propagate NaNs correctly
    illiq = log_ret.abs() / (dollar_vol.replace(0.0, np.nan))
    return illiq.rolling(window, min_periods=1).mean().shift(1)


def calculate_kyle_lambda(
    close: pd.Series, buy_vol: pd.Series, sell_vol: pd.Series, window: int = 24
) -> pd.Series:
    """
    Kyle's Lambda: price change = lambda * net_order_flow
    Uses rolling regression logic.
    """
    price_change = close.diff()
    net_flow = buy_vol - sell_vol

    # exy = E(XY) -> lagged
    exy = (net_flow * price_change).rolling(window).mean().shift(1)
    ex = net_flow.rolling(window).mean().shift(1)
    ey = price_change.rolling(window).mean().shift(1)
    ex2 = (net_flow**2).rolling(window).mean().shift(1)

    num = exy - (ex * ey)
    denom = ex2 - (ex**2)

    # Use a variance floor to avoid near-zero denominators blowing up lambda.
    # Values within machine-noise of zero are treated as missing rather than
    # producing astronomically large estimates that corrupt event detectors.
    safe_denom = denom.where(denom.abs() > 1e-8, np.nan)
    lambdas = (num / safe_denom).clip(-1e6, 1e6)
    return lambdas.reindex(close.index)


def calculate_vpin_score(volume: pd.Series, buy_volume: pd.Series, window: int = 50) -> pd.Series:
    """
    VPIN score using rolling volume windows.
    Matches spec: vpin_score
    """
    sell_volume = volume - buy_volume
    oi = (buy_volume - sell_volume).abs()

    # VPIN = sum|V_buy - V_sell| / Total_Volume over window
    # Lagged to ensure PIT
    vpin = oi.rolling(window, min_periods=1).sum() / volume.rolling(window, min_periods=1).sum().replace(0.0, np.nan)
    return vpin.shift(1)


def calculate_imbalance(buy_vol: pd.Series, sell_vol: pd.Series, window: int = 24) -> pd.Series:
    """
    Rolling orderbook/orderflow imbalance: (Buy - Sell) / (Buy + Sell).
    Lagged to ensure PIT.
    """
    total = buy_vol + sell_vol
    imb = (buy_vol - sell_vol) / total.replace(0.0, np.nan)
    return imb.rolling(window, min_periods=min(window, 4)).mean().shift(1).fillna(0.0)
