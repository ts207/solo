from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Tuple


def safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denom = denominator.replace(0.0, np.nan)
    return (numerator / denom).replace([np.inf, -np.inf], np.nan)


def rolling_quantile(series: pd.Series, window: int, q: float) -> pd.Series:
    return series.rolling(window, min_periods=1).quantile(q)


def build_signal_frame(merged: pd.DataFrame) -> pd.DataFrame:
    """
    Augments the merged features/bars frame with derived runtime signals needed for interpretation.
    """
    frame = merged.copy()
    timestamp = pd.to_datetime(
        frame.get("timestamp", pd.Series(pd.NaT, index=frame.index)),
        utc=True,
        errors="coerce",
    )
    frame["timestamp"] = timestamp
    frame["session_hour_utc"] = timestamp.dt.hour.astype(float)

    close = pd.to_numeric(frame.get("close", pd.Series(np.nan, index=frame.index)), errors="coerce")
    frame["close"] = close

    volume = pd.to_numeric(
        frame.get("volume", pd.Series(np.nan, index=frame.index)), errors="coerce"
    )
    volume_quote_fallback = (volume * close).replace([np.inf, -np.inf], np.nan)
    if "quote_volume" in frame.columns:
        quote_volume = pd.to_numeric(frame.get("quote_volume"), errors="coerce")
        quote_volume = quote_volume.where(quote_volume.notna(), volume_quote_fallback)
    else:
        quote_volume = volume_quote_fallback
    frame["quote_volume"] = quote_volume

    if "spread_bps" in frame.columns:
        spread_bps = pd.to_numeric(frame.get("spread_bps"), errors="coerce")
        if "basis_bps" in frame.columns:
            basis_spread = pd.to_numeric(frame.get("basis_bps"), errors="coerce")
            spread_bps = spread_bps.where(spread_bps.notna(), basis_spread)
    elif "basis_bps" in frame.columns:
        spread_bps = pd.to_numeric(frame.get("basis_bps"), errors="coerce")
    else:
        spread_bps = pd.Series(np.nan, index=frame.index, dtype=float)
    frame["spread_bps"] = spread_bps
    frame["spread_abs"] = spread_bps.abs()

    if "funding_rate_scaled" in frame.columns:
        funding_rate = pd.to_numeric(frame.get("funding_rate_scaled"), errors="coerce")
        funding_available = funding_rate.notna()
    else:
        funding_rate = pd.Series(0.0, index=frame.index, dtype=float)
        funding_available = pd.Series(False, index=frame.index, dtype=bool)
    frame["funding_rate_scaled"] = funding_rate
    frame["funding_rate_scaled_available"] = funding_available.astype(bool)
    frame["funding_bps_abs"] = (funding_rate * 10_000.0).abs()

    ret_1 = close.pct_change().replace([np.inf, -np.inf], np.nan).fillna(0.0)
    ret_4 = close.pct_change(4).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    frame["ret_1"] = ret_1
    frame["ret_4"] = ret_4
    frame["abs_ret_1"] = ret_1.abs()
    frame["abs_ret_4"] = ret_4.abs()
    trend_96 = safe_divide(close, close.shift(96)) - 1.0
    bull_bear_flag = np.sign(trend_96).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    frame["bull_bear_flag"] = bull_bear_flag.astype(float)

    volume_median = quote_volume.rolling(96, min_periods=1).median()
    frame["volume_ratio"] = safe_divide(quote_volume, volume_median)

    if "range_96" in frame.columns:
        range_num = pd.to_numeric(frame.get("range_96"), errors="coerce").abs()
    else:
        high_96 = pd.to_numeric(
            frame.get("high_96", pd.Series(np.nan, index=frame.index)), errors="coerce"
        )
        low_96 = pd.to_numeric(
            frame.get("low_96", pd.Series(np.nan, index=frame.index)), errors="coerce"
        )
        range_num = (high_96 - low_96).abs()

    if "range_med_480" in frame.columns:
        range_den = pd.to_numeric(frame.get("range_med_480"), errors="coerce").abs()
    else:
        range_den = range_num.rolling(480, min_periods=1).median()

    range_ratio = safe_divide(range_num, range_den)
    frame["range_ratio"] = range_ratio
    vol_mean = range_ratio.rolling(96, min_periods=96).mean()
    vol_std = range_ratio.rolling(96, min_periods=96).std().replace(0.0, np.nan)
    frame["vol_z"] = ((range_ratio - vol_mean) / vol_std).replace([np.inf, -np.inf], np.nan)
    realized_vol = ret_1.abs().rolling(96, min_periods=1).mean()
    vol_q33 = realized_vol.rolling(480, min_periods=1).quantile(1.0 / 3.0)
    vol_q66 = realized_vol.rolling(480, min_periods=1).quantile(2.0 / 3.0)
    vol_regime_code = pd.Series(1.0, index=frame.index, dtype=float)
    vol_regime_code = vol_regime_code.mask(realized_vol <= vol_q33, 0.0)
    vol_regime_code = vol_regime_code.mask(realized_vol >= vol_q66, 2.0)
    frame["vol_regime_code"] = vol_regime_code.fillna(1.0)

    frame["spread_q75"] = rolling_quantile(frame["spread_abs"], 96, 0.75)
    frame["abs_ret_q75"] = rolling_quantile(frame["abs_ret_1"], 96, 0.75)
    frame["abs_ret4_q90"] = rolling_quantile(frame["abs_ret_4"], 96, 0.90)
    frame["range_ratio_q25"] = rolling_quantile(frame["range_ratio"], 96, 0.25)

    for col in ["direction_score", "signed_edge", "forward_return_h"]:
        frame[col] = pd.to_numeric(
            frame.get(col, pd.Series(np.nan, index=frame.index)), errors="coerce"
        )

    return frame
