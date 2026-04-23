from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Any


def is_finite_scalar(x: Any) -> bool:
    """Returns True only if x is a finite number."""
    try:
        f = float(x)
        return np.isfinite(f)
    except (TypeError, ValueError):
        return False


def finite_ge(x: Any, threshold: float) -> bool:
    """Returns True only if x is finite and >= threshold."""
    try:
        f = float(x)
        if not np.isfinite(f):
            return False
        return f >= float(threshold)
    except (TypeError, ValueError):
        return False


def finite_le(x: Any, threshold: float) -> bool:
    """Returns True only if x is finite and <= threshold."""
    try:
        f = float(x)
        if not np.isfinite(f):
            return False
        return f <= float(threshold)
    except (TypeError, ValueError):
        return False


def fail_closed_bool(x: Any) -> bool:
    """Returns False if x is missing, NaN, or not truthy."""
    if isinstance(x, bool):
        return x
    if x is None:
        return False
    if isinstance(x, (int, float)):
        try:
            f = float(x)
            if not np.isfinite(f):
                return False
            return bool(int(f))
        except (TypeError, ValueError):
            return False
    s = str(x).strip().lower()
    return s in {"1", "true", "t", "yes", "y", "on", "pass"}


def nanmedian_or_nan(series: pd.Series | list) -> float:
    """Returns NaN if the series is empty or all NaN, otherwise returns median."""
    if isinstance(series, list):
        series = pd.Series(series)
    if series.empty:
        return np.nan
    res = series.median()
    return float(res) if pd.notna(res) else np.nan


def nanmax_or_nan(series: pd.Series | list) -> float:
    """Returns NaN if the series is empty or all NaN, otherwise returns max."""
    if isinstance(series, list):
        series = pd.Series(series)
    if series.empty:
        return np.nan
    res = series.max()
    return float(res) if pd.notna(res) else np.nan


def required_columns(df: pd.DataFrame, cols: list[str], stage_name: str) -> None:
    """Raises ValueError if any required column is missing from the DataFrame."""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"[{stage_name}] missing required columns: {missing}")


def coerce_numeric_nan(val: Any) -> float:
    """Coerces to float, returns NaN if not finite or invalid."""
    try:
        f = float(val)
        return f if np.isfinite(f) else np.nan
    except (TypeError, ValueError):
        return np.nan


def bool_gate(value: Any) -> bool:
    """Alias for fail_closed_bool, returning False on missing/invalid."""
    return fail_closed_bool(value)
