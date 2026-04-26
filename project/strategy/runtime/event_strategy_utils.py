from __future__ import annotations

import numpy as np
import pandas as pd

from project.core.validation import ensure_utc_timestamp


def to_numeric(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)


def prepare_frame(bars: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    bars_cp = bars.copy()
    feats_cp = features.copy()
    bars_cp["timestamp"] = pd.to_datetime(bars_cp["timestamp"], utc=True)
    feats_cp["timestamp"] = pd.to_datetime(feats_cp["timestamp"], utc=True)
    ensure_utc_timestamp(bars_cp["timestamp"], "timestamp")
    ensure_utc_timestamp(feats_cp["timestamp"], "timestamp")

    feature_base = feats_cp.drop(
        columns=["open", "high", "low", "close", "volume"], errors="ignore"
    )
    merged = (
        bars_cp[["timestamp", "open", "high", "low", "close", "volume"]]
        .merge(feature_base, on="timestamp", how="left")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    return merged


def finalize_positions(
    merged: pd.DataFrame,
    positions: list[int],
    signal_events: list[dict[str, object]],
    strategy_id: str,
    family: str,
    params: dict[str, object],
) -> pd.Series:
    out = pd.Series(positions, index=merged["timestamp"], name="position").astype(int)
    out.attrs["signal_events"] = signal_events
    out.attrs["strategy_metadata"] = {
        "family": family,
        "strategy_id": strategy_id,
        "key_params": params,
    }
    return out


def rolling_z(series: pd.Series, window: int, min_periods: int) -> pd.Series:
    mean = series.rolling(window=window, min_periods=min_periods).mean()
    std = series.rolling(window=window, min_periods=min_periods).std().replace(0.0, np.nan)
    return ((series - mean) / std).replace([np.inf, -np.inf], np.nan).fillna(0.0)
