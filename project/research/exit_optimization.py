from __future__ import annotations

import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd

_LOG = logging.getLogger(__name__)

_HORIZON_COLUMN_TEMPLATES = (
    "return_h{h}",
    "ret_h{h}",
    "ret_{h}",
    "pnl_h{h}",
    "pnl_{h}",
    "expectancy_h{h}",
    "future_return_{h}",
)


def _coerce_numeric_series(values: object) -> pd.Series:
    if isinstance(values, pd.Series):
        return pd.to_numeric(values, errors="coerce")
    if isinstance(values, (list, tuple, np.ndarray)):
        return pd.to_numeric(pd.Series(list(values)), errors="coerce")
    if values is None:
        return pd.Series(dtype=float)
    try:
        return pd.to_numeric(pd.Series([values]), errors="coerce")
    except Exception:
        return pd.Series(dtype=float)


def _extract_horizon_returns(episodes: pd.DataFrame, horizon: int) -> pd.Series:
    for template in _HORIZON_COLUMN_TEMPLATES:
        col = template.format(h=horizon)
        if col in episodes.columns:
            return pd.to_numeric(episodes[col], errors="coerce").dropna()

    sequence_columns = [
        "future_returns",
        "forward_returns",
        "returns_path",
        "pnl_path",
        "episode_returns",
    ]
    for col in sequence_columns:
        if col not in episodes.columns:
            continue
        values: List[float] = []
        for entry in episodes[col].dropna():
            if isinstance(entry, str):
                try:
                    entry = pd.read_json(entry, typ="series")
                except Exception:
                    continue
            seq = _coerce_numeric_series(entry).dropna()
            if len(seq) >= horizon:
                values.append(float(seq.iloc[:horizon].sum()))
        return pd.Series(values, dtype=float)

    if {"entry_price", "exit_price", "holding_bars"}.issubset(episodes.columns):
        subset = episodes.loc[pd.to_numeric(episodes["holding_bars"], errors="coerce") == horizon]
        if not subset.empty:
            entry = pd.to_numeric(subset["entry_price"], errors="coerce")
            exit_ = pd.to_numeric(subset["exit_price"], errors="coerce")
            ret = (exit_ - entry) / entry.replace(0.0, np.nan)
            return ret.replace([np.inf, -np.inf], np.nan).dropna()

    return pd.Series(dtype=float)


def optimize_exit_horizon(
    episodes: pd.DataFrame,
    max_horizon: int = 192,
) -> Dict[str, Any]:
    """
    Find the fixed holding horizon with the highest realized mean expectancy.
    """
    if episodes.empty:
        return {"optimal_horizon": 96, "max_expectancy": 0.0, "evaluated_horizons": 0}

    horizon_scores: List[tuple[int, float, int]] = []
    for horizon in range(4, max_horizon + 1, 4):
        realized = _extract_horizon_returns(episodes, horizon)
        if realized.empty:
            continue
        horizon_scores.append((horizon, float(realized.mean()), int(realized.count())))

    if not horizon_scores:
        return {"optimal_horizon": 96, "max_expectancy": 0.0, "evaluated_horizons": 0}

    optimal_horizon, max_expectancy, sample_count = max(
        horizon_scores,
        key=lambda item: (item[1], item[2], -item[0]),
    )
    return {
        "optimal_horizon": int(optimal_horizon),
        "max_expectancy": float(max_expectancy),
        "sample_count": int(sample_count),
        "evaluated_horizons": int(len(horizon_scores)),
    }


def analyze_exit_efficiency(
    trades: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Analyze whether realized exits capture available move efficiently.
    """
    if trades.empty:
        return {}

    exit_return = pd.Series(dtype=float)
    if "exit_return" in trades.columns:
        exit_return = pd.to_numeric(trades["exit_return"], errors="coerce")
    elif {"entry_price", "exit_price"}.issubset(trades.columns):
        entry = pd.to_numeric(trades["entry_price"], errors="coerce")
        exit_ = pd.to_numeric(trades["exit_price"], errors="coerce")
        direction = pd.to_numeric(trades.get("direction", 1.0), errors="coerce").fillna(1.0)
        exit_return = ((exit_ - entry) / entry.replace(0.0, np.nan)) * direction

    max_favorable = pd.Series(dtype=float)
    if "max_favorable_return" in trades.columns:
        max_favorable = pd.to_numeric(trades["max_favorable_return"], errors="coerce")
    elif {"entry_price", "max_favorable_price"}.issubset(trades.columns):
        entry = pd.to_numeric(trades["entry_price"], errors="coerce")
        mfe_price = pd.to_numeric(trades["max_favorable_price"], errors="coerce")
        direction = pd.to_numeric(trades.get("direction", 1.0), errors="coerce").fillna(1.0)
        max_favorable = ((mfe_price - entry) / entry.replace(0.0, np.nan)) * direction

    post_exit_return = pd.Series(dtype=float)
    if "post_exit_return" in trades.columns:
        post_exit_return = pd.to_numeric(trades["post_exit_return"], errors="coerce")
    elif {"exit_price", "max_post_exit_price"}.issubset(trades.columns):
        exit_ = pd.to_numeric(trades["exit_price"], errors="coerce")
        post = pd.to_numeric(trades["max_post_exit_price"], errors="coerce")
        direction = pd.to_numeric(trades.get("direction", 1.0), errors="coerce").fillna(1.0)
        post_exit_return = ((post - exit_) / exit_.replace(0.0, np.nan)) * direction

    df = pd.DataFrame(
        {
            "exit_return": exit_return,
            "max_favorable": max_favorable,
            "post_exit_return": post_exit_return,
        }
    ).replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["exit_return"])
    if df.empty:
        return {}

    denom = df["max_favorable"].where(df["max_favorable"].abs() > 1e-12)
    efficiency = (df["exit_return"] / denom).clip(lower=0.0, upper=1.5)
    early_exit_rate = float((df["post_exit_return"].fillna(0.0) > 0.0).mean())
    late_exit_rate = float(
        ((df["exit_return"] < 0.0) & (df["post_exit_return"].fillna(0.0) <= 0.0)).mean()
    )

    return {
        "avg_exit_efficiency": float(efficiency.dropna().mean())
        if efficiency.notna().any()
        else 0.0,
        "median_exit_efficiency": float(efficiency.dropna().median())
        if efficiency.notna().any()
        else 0.0,
        "early_exit_rate": early_exit_rate,
        "late_exit_rate": late_exit_rate,
        "n_trades": int(len(df)),
    }
