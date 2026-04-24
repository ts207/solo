from __future__ import annotations

from typing import Any, Mapping

import numpy as np
import pandas as pd

from project.events.thresholding import percentile_rank_historical


def run_length(mask: pd.Series) -> pd.Series:
    """Compute run lengths of True values in a boolean series."""
    out = []
    streak = 0
    for flag in mask.fillna(False).astype(bool).tolist():
        streak = streak + 1 if flag else 0
        out.append(streak)
    return pd.Series(out, index=mask.index)


def prepare_funding_persistence_features(
    df: pd.DataFrame,
    funding_signed: pd.Series,
    defaults: Mapping[str, Any],
    params: Mapping[str, Any],
) -> dict[str, pd.Series]:
    """Extracted feature preparation for FundingPersistenceDetector."""
    f_pct = pd.to_numeric(df["funding_abs_pct"], errors="coerce").astype(float)
    f_abs = pd.to_numeric(df["funding_abs"], errors="coerce").astype(float)

    accel_pct = float(params.get("accel_pct", defaults["accel_pct"]))
    accel_lookback = int(params.get("accel_lookback", 12))
    persistence_pct = float(params.get("persistence_pct", defaults["persistence_pct"]))
    persistence_bars = int(params.get("persistence_bars", 8))
    threshold_window = int(params.get("threshold_window", 2880))

    accel = f_abs - f_abs.shift(accel_lookback)
    accel = accel.where(accel > 0.0)
    accel_rank = percentile_rank_historical(
        accel, window=threshold_window, min_periods=max(1, min(threshold_window, max(24, accel_lookback)))
    )
    accel_raw = ((accel_rank >= accel_pct) & (accel_rank.shift(1) < accel_pct)).fillna(False)

    fp_active = pd.to_numeric(
        df.get("fp_active", pd.Series(np.nan, index=df.index)), errors="coerce"
    )
    fp_age_bars = pd.to_numeric(
        df.get("fp_age_bars", pd.Series(np.nan, index=df.index)), errors="coerce"
    )
    fp_severity = pd.to_numeric(
        df.get("fp_severity", pd.Series(np.nan, index=df.index)), errors="coerce"
    )

    if fp_active.notna().any():
        persistence_raw = (fp_active.fillna(0.0) > 0).astype(bool)
        run_len = (fp_age_bars.fillna(0.0) + max(persistence_bars - 1, 0)).astype(float)
        persistence_intensity = fp_severity.fillna(0.0).clip(lower=0.0)
    else:
        high = (f_pct >= persistence_pct).fillna(False)
        run_len = run_length(high)
        persistence_raw = (high & (run_len == persistence_bars)).fillna(False)
        persistence_intensity = (run_len / max(persistence_bars, 1)).clip(lower=0.0)

    subtype = pd.Series("none", index=df.index, dtype="object")
    subtype = subtype.where(~accel_raw, "acceleration")
    subtype = subtype.where(~persistence_raw, "persistence")
    accel_intensity = (accel_rank / 100.0).clip(lower=0.0)
    signal_intensity = pd.Series(0.0, index=df.index, dtype=float)
    signal_intensity = signal_intensity.where(~accel_raw, accel_intensity.fillna(0.0))
    signal_intensity = signal_intensity.where(~persistence_raw, persistence_intensity.fillna(0.0))

    return {
        "funding_abs_pct": f_pct,
        "funding_abs": f_abs,
        "funding_signed": funding_signed,
        "run_len": run_len.astype(float),
        "accel_rank": accel_rank.fillna(0.0),
        "signal_intensity": signal_intensity.clip(lower=0.0),
        "subtype": subtype,
        "mask": (accel_raw | persistence_raw).fillna(False),
    }


def prepare_funding_normalization_features(
    df: pd.DataFrame,
    funding_signed: pd.Series,
    defaults: Mapping[str, Any],
    params: Mapping[str, Any],
) -> dict[str, pd.Series]:
    """Extracted feature preparation for FundingNormalizationDetector."""
    f_pct = pd.to_numeric(df["funding_abs_pct"], errors="coerce").astype(float)
    f_abs = pd.to_numeric(df["funding_abs"], errors="coerce").astype(float)

    extreme_pct = float(params.get("extreme_pct", defaults["extreme_pct"]))
    normalization_pct = float(params.get("normalization_pct", defaults["normalization_pct"]))
    normalization_lookback = int(
        params.get("normalization_lookback", defaults["normalization_lookback"])
    )
    min_prior_extreme_abs = float(
        params.get("min_prior_extreme_abs", defaults["min_prior_extreme_abs"])
    )

    recent_extreme = (
        (f_pct.shift(1) >= extreme_pct)
        .rolling(window=normalization_lookback, min_periods=1)
        .max()
        .fillna(0)
        .astype(bool)
    )
    prior_extreme_pct = (
        f_pct.shift(1)
        .where(f_pct.shift(1) >= extreme_pct)
        .rolling(window=normalization_lookback, min_periods=1)
        .max()
        .fillna(0.0)
    )
    prior_extreme_abs = (
        f_abs.shift(1)
        .where(f_pct.shift(1) >= extreme_pct)
        .rolling(window=normalization_lookback, min_periods=1)
        .max()
        .fillna(0.0)
    )
    mask = (
        (f_pct <= normalization_pct)
        & (f_pct.shift(1) > normalization_pct)
        & recent_extreme
        & (prior_extreme_abs >= min_prior_extreme_abs)
    ).fillna(False)
    release_intensity = ((prior_extreme_pct - f_pct).clip(lower=0.0) / 100.0).fillna(0.0)

    return {
        "funding_abs_pct": f_pct,
        "funding_abs": f_abs,
        "funding_signed": funding_signed,
        "prior_extreme_pct": prior_extreme_pct,
        "prior_extreme_abs": prior_extreme_abs,
        "signal_intensity": release_intensity,
        "mask": mask,
    }
