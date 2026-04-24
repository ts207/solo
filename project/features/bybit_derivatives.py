from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


def derive_realized_volatility(
    ohlcv_df: pd.DataFrame, window: int = 20, annualize_factor: float = np.sqrt(365 * 288)
) -> pd.Series:
    """
    Calculate rolling realized volatility from 5m log returns.
    288 = number of 5m bars in a day.
    """
    returns = np.log(ohlcv_df["close"] / ohlcv_df["close"].shift(1))
    vol = returns.rolling(window=window).std() * annualize_factor
    return vol.rename(f"rv_5m_{window}")


def derive_basis_premium(
    mark_price_df: pd.DataFrame, index_price_df: pd.DataFrame
) -> pd.Series:
    """
    Calculate basis/premium in bps between 5m-aligned mark price and index price.
    """
    # Ensure alignment on timestamp
    merged = pd.merge_asof(
        mark_price_df.sort_values("timestamp"),
        index_price_df.sort_values("timestamp"),
        on="timestamp",
        suffixes=("_mark", "_index"),
    )
    basis_bps = (merged["mark_price"] / merged["index_price"] - 1) * 10000
    return pd.Series(basis_bps.values, index=merged["timestamp"], name="basis_bps")


def derive_funding_slope(funding_df: pd.DataFrame) -> pd.Series:
    """
    Calculate the rate of change (slope) of funding rates.
    Aligned to the funding update frequency (usually 8h), but can be broadcast to 5m.
    """
    df = funding_df.sort_values("timestamp").copy()
    # Change in funding rate per hour
    df["funding_slope"] = df["funding_rate"].diff() / df["timestamp"].diff().dt.total_seconds() * 3600
    return df.set_index("timestamp")["funding_slope"]


def derive_oi_features(oi_df: pd.DataFrame, window: int = 12) -> pd.DataFrame:
    """
    Calculate 5m OI level, delta, and acceleration.
    Assumes oi_df is already at 5m resolution (Bybit OI history is usually 5m).
    """
    df = oi_df.sort_values("timestamp").copy()
    df = df.set_index("timestamp")

    # Delta (1-period change at 5m)
    df["oi_delta"] = df["open_interest"].diff()
    df["oi_delta_pct"] = df["open_interest"].pct_change()

    # Acceleration (Change in delta)
    df["oi_acceleration"] = df["oi_delta"].diff()

    # Rolling level relative to window (Z-score)
    df["oi_relative_level"] = (
        df["open_interest"] - df["open_interest"].rolling(window).mean()
    ) / (df["open_interest"].rolling(window).std() + 1e-9)

    return df[["oi_delta", "oi_delta_pct", "oi_acceleration", "oi_relative_level"]]


def derive_trend_regime(ohlcv_df: pd.DataFrame, fast_period: int = 20, slow_period: int = 50) -> pd.Series:
    """
    Identify trend vs chop regime using 5m EMA crossover.
    """
    df = ohlcv_df.sort_values("timestamp").copy()
    df = df.set_index("timestamp")

    ema_fast = df["close"].ewm(span=fast_period).mean()
    ema_slow = df["close"].ewm(span=slow_period).mean()

    # Trend strength in bps
    diff = (ema_fast - ema_slow) / ema_slow * 10000

    regime = pd.Series("CHOP", index=df.index)
    regime[diff > 10] = "BULL_TREND"
    regime[diff < -10] = "BEAR_TREND"

    return regime.rename("trend_regime")


def derive_liquidity_stress_proxy(
    ticker_df: pd.DataFrame, window: int = 60
) -> pd.Series:
    """
    Proxy for liquidity stress using 5m-resampled bid/ask spread and top-of-book size.
    """
    df = ticker_df.sort_values("timestamp").copy()
    df["spread_bps"] = (df["best_ask_price"] / df["best_bid_price"] - 1) * 10000
    df["avg_top_size"] = (df["best_bid_qty"] + df["best_ask_qty"]) / 2

    # Raw stress = Spread / Size
    raw_stress = df["spread_bps"] / (df["avg_top_size"] + 1e-9)

    # Rolling Z-score
    stress_proxy = (raw_stress - raw_stress.rolling(window).mean()) / (raw_stress.rolling(window).std() + 1e-9)

    return pd.Series(stress_proxy.values, index=df["timestamp"], name="liquidity_stress_proxy")


def build_bybit_derivatives_feature_set(
    ohlcv_5m: pd.DataFrame,
    mark_price_5m: pd.DataFrame,
    index_price_5m: pd.DataFrame,
    funding_df: pd.DataFrame,
    oi_df: pd.DataFrame,
    ticker_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Joins all Bybit derivatives data into a canonical 5m feature frame.
    """
    # 1. Base OHLCV
    base = ohlcv_5m.sort_values("timestamp").set_index("timestamp")

    # 2. RV features — compute on base directly to avoid integer-vs-timestamp index mismatch
    _returns = np.log(base["close"] / base["close"].shift(1))
    _ann = np.sqrt(365 * 288)
    base["rv_5m_20"] = _returns.rolling(window=20).std() * _ann
    base["rv_5m_60"] = _returns.rolling(window=60).std() * _ann
    base["rv_96"] = _returns.rolling(window=96).std() * _ann

    # 3. Regime — derive_trend_regime sets its own timestamp index, aligns correctly
    base["trend_regime"] = derive_trend_regime(ohlcv_5m)

    # 4. Mark/Index/Basis
    base["close_perp"] = mark_price_5m.sort_values("timestamp").set_index("timestamp")["mark_price"]
    base["close_spot"] = index_price_5m.sort_values("timestamp").set_index("timestamp")["index_price"]
    basis = derive_basis_premium(mark_price_5m, index_price_5m)
    # merge_asof requires DataFrame on the right side (not Series)
    base = pd.merge_asof(base, basis.to_frame(), left_index=True, right_index=True)

    # 5. Funding
    f_slope = derive_funding_slope(funding_df)
    f_rate = funding_df.sort_values("timestamp").set_index("timestamp")["funding_rate"]
    base = pd.merge_asof(base, f_rate.to_frame(), left_index=True, right_index=True)
    base = pd.merge_asof(base, f_slope.to_frame(), left_index=True, right_index=True)

    # 6. Open Interest
    oi_feats = derive_oi_features(oi_df)
    oi_val = oi_df.sort_values("timestamp").set_index("timestamp")["open_interest"]
    base = pd.merge_asof(base, oi_val.to_frame(), left_index=True, right_index=True)
    base = pd.merge_asof(base, oi_feats, left_index=True, right_index=True)

    # 7. Liquidity Stress (if available)
    if ticker_df is not None:
        stress = derive_liquidity_stress_proxy(ticker_df)
        base = pd.merge_asof(base, stress, left_index=True, right_index=True)
    else:
        base["liquidity_stress_proxy"] = np.nan

    # Fill small gaps in broadcasted funding/OI if necessary
    base = base.ffill().dropna(subset=["close"])

    return base.reset_index()
