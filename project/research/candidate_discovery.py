from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple

from project.research.research_core import (
    normalize_research_dataframe,
    load_research_features,
    sparsify_event_mask,
    rolling_z_score,
    safe_float,
    safe_int,
)


def discover_candidates_for_event(
    features_df: pd.DataFrame,
    event_type: str,
    *,
    min_spacing: int = 6,
    horizon_bars: int = 24,
    min_expectancy_bps: float = 2.0,
) -> pd.DataFrame:
    """
    Business logic for isolating and characterizing event-driven candidates.

    Args:
        features_df: DataFrame with research features and event masks.
        event_type: The type of event to isolate (e.g., 'liquidations', 'vol_shock').
        min_spacing: Minimum bars between isolated events.
        horizon_bars: Forecast horizon for returns calculation.
        min_expectancy_bps: Initial gating threshold for mean forward return.

    Returns:
        DataFrame of candidate events with forward returns and initial gating flags.
    """
    df = normalize_research_dataframe(features_df)
    if df.empty:
        return pd.DataFrame()

    # 1. Compute forward returns
    df["forward_ret"] = compute_forward_returns(df, horizon_bars)

    # 2. Event isolation
    # Look for a mask column: event_{event_type} or event_mask
    mask_col = f"event_{event_type.lower()}"
    if mask_col not in df.columns:
        # If specific mask not found, check if 'event_type' column exists and filter it
        if "event_type" in df.columns:
            mask = df["event_type"] == event_type
        else:
            # Fallback to any column starting with 'event_'
            event_cols = [c for c in df.columns if c.startswith("event_")]
            mask_col = event_cols[0] if event_cols else None
            mask = (
                df[mask_col].fillna(False).astype(bool)
                if mask_col
                else pd.Series(False, index=df.index)
            )
    else:
        mask = df[mask_col].fillna(False).astype(bool)

    candidates = apply_event_mask(df, mask, min_spacing=min_spacing)

    if candidates.empty:
        return pd.DataFrame()

    # 3. Initial gating - QUARANTINED in Phase 2
    # Only keep candidates where we have forward returns
    candidates = candidates.dropna(subset=["forward_ret"]).copy()

    # Simple expectancy gate is too weak and creates false-positive channels.
    # We now defer to richer split-aware services for all statistical gating.
    # mean_ret_bps = candidates["forward_ret"].mean() * 10000.0
    # candidates["gate_initial_expectancy"] = mean_ret_bps >= min_expectancy_bps
    # candidates["expectancy_bps"] = mean_ret_bps

    return candidates


def apply_event_mask(df: pd.DataFrame, mask: pd.Series, min_spacing: int = 6) -> pd.DataFrame:
    idxs = sparsify_event_mask(mask, min_spacing)
    return df.iloc[idxs].copy()


def compute_forward_returns(df: pd.DataFrame, horizon: int) -> pd.Series:
    """
    Calculates forward log returns over a fixed horizon.
    """
    if "close" not in df.columns:
        return pd.Series(np.nan, index=df.index)
    log_close = np.log(df["close"])
    return log_close.shift(-horizon) - log_close


def calculate_expectancy_stats(
    returns: pd.Series,
    *,
    min_ess: float = 150.0,
) -> Dict[str, Any]:
    """
    Calculates mean, std, t-stat, and effective sample size.
    """
    clean = returns.dropna()
    n = len(clean)
    if n < 2:
        return {"mean": 0.0, "std": 0.0, "t_stat": 0.0, "ess": 0.0}

    mean = clean.mean()
    std = clean.std()
    t_stat = mean / (std / np.sqrt(n)) if std > 0 else 0.0

    return {
        "mean": float(mean),
        "std": float(std),
        "t_stat": float(t_stat),
        "ess": float(n),
    }
