from __future__ import annotations

from collections.abc import Iterable
from typing import Any, cast

import numpy as np
import pandas as pd

# Consts for funding and state validation
FUNDING_MAX_ABS = 0.05
FUNDING_SCALE_CANDIDATES = (1.0, 0.01, 0.0001)
_KNOWN_DECIMAL_FUNDING_SOURCES = {"archive_monthly", "archive_daily", "api", "bybit_v5"}
FUNDING_SCALE_NAME_TO_MULTIPLIER = {
    "decimal": 1.0,
    "percent": 0.01,
    "bps": 0.0001,
}

_ALLOWED_SIZING_CURVES = {"linear", "sqrt", "flat"}
_STRATEGY_FAMILY_KEYS = {
    "Carry": {
        "funding_percentile_entry_min",
        "funding_percentile_entry_max",
        "normalization_exit_percentile",
        "normalization_exit_consecutive_bars",
        "sizing_curve",
    },
    "MeanReversion": {
        "zscore_entry_abs",
        "extension_entry_abs",
        "reversion_target_zscore",
        "stop_zscore_abs",
    },
    "Spread": {
        "spread_zscore_entry_abs",
        "dislocation_threshold_bps",
        "convergence_target_zscore",
        "max_hold_bars",
    },
}


def assert_ohlcv_schema(df: pd.DataFrame) -> None:
    """
    Validate that OHLCV data has expected columns and numeric types.
    """
    required = ["timestamp", "open", "high", "low", "close", "volume"]
    validate_columns(df, required)
    for col in ["open", "high", "low", "close", "volume"]:
        if not pd.api.types.is_numeric_dtype(df[col]):
            raise ValueError(f"{col} must be numeric")


def assert_ohlcv_geometry(df: pd.DataFrame) -> None:
    """
    Validate OHLCV geometric constraints on non-null rows:
    - All prices must be > 0
    - high >= open, high >= close, high >= low
    - low <= open, low <= close
    Raises ValueError on the first violation found.
    """
    price_cols = ["open", "high", "low", "close"]
    validate_columns(df, price_cols)

    o = pd.to_numeric(df["open"], errors="coerce")
    h = pd.to_numeric(df["high"], errors="coerce")
    l = pd.to_numeric(df["low"], errors="coerce")
    c = pd.to_numeric(df["close"], errors="coerce")

    for name, series in [("open", o), ("high", h), ("low", l), ("close", c)]:
        non_null = series.dropna()
        if (non_null <= 0).any():
            raise ValueError(f"{name} contains non-positive values")

    checks = [
        (h.notna() & o.notna() & (h < o), "high < open"),
        (h.notna() & c.notna() & (h < c), "high < close"),
        (h.notna() & l.notna() & (h < l), "high < low"),
        (l.notna() & o.notna() & (l > o), "low > open"),
        (l.notna() & c.notna() & (l > c), "low > close"),
    ]
    for mask, label in checks:
        count = int(mask.sum())
        if count:
            raise ValueError(f"OHLCV geometry violation: {label} in {count} row(s)")


def filter_ohlcv_geometry_violations(
    df: pd.DataFrame, label: str = ""
) -> tuple[pd.DataFrame, int]:
    """
    Drop rows that violate OHLCV geometric constraints or have non-positive prices.
    Returns (clean_df, dropped_count). Intended for ingest-time soft filtering.
    """
    price_cols = [c for c in ["open", "high", "low", "close"] if c in df.columns]
    if not price_cols:
        return df, 0

    valid = pd.Series(True, index=df.index)

    for col in price_cols:
        s = cast(pd.Series, pd.to_numeric(df[col], errors="coerce"))
        valid &= cast(pd.Series, s.isna() | (s > 0))

    if all(c in df.columns for c in ["open", "high", "low", "close"]):
        o = cast(pd.Series, pd.to_numeric(df["open"], errors="coerce"))
        h = cast(pd.Series, pd.to_numeric(df["high"], errors="coerce"))
        lv = cast(pd.Series, pd.to_numeric(df["low"], errors="coerce"))
        c = cast(pd.Series, pd.to_numeric(df["close"], errors="coerce"))
        valid &= cast(pd.Series, h.isna() | o.isna() | (h >= o))
        valid &= cast(pd.Series, h.isna() | c.isna() | (h >= c))
        valid &= cast(pd.Series, h.isna() | lv.isna() | (h >= lv))
        valid &= cast(pd.Series, lv.isna() | o.isna() | (lv <= o))
        valid &= cast(pd.Series, lv.isna() | c.isna() | (lv <= c))

    dropped = int((~valid).sum())
    return df[valid].copy(), dropped


def assert_monotonic_utc_timestamp(df: pd.DataFrame, col: str = "timestamp") -> None:
    """
    Ensure timestamp column is tz-aware UTC, monotonic increasing, and unique.
    """
    validate_columns(df, [col])
    series = df[col]
    if bool(series.isna().any()):
        raise ValueError(f"{col} contains nulls")
    ensure_utc_timestamp(series, col)
    if bool(series.duplicated().any()):
        raise ValueError(f"{col} contains duplicate timestamps")
    if not series.is_monotonic_increasing:
        raise ValueError(f"{col} must be monotonic increasing")


def infer_and_apply_funding_scale(
    df: pd.DataFrame,
    col: str = "funding_rate",
    source_col: str = "source",
    explicit_scale: float | None = None,
) -> tuple[pd.DataFrame, float, float]:
    """
    Infer funding rate scale and add funding_rate_scaled column.
    Returns (scaled_frame, scale_multiplier, confidence).
    """
    validate_columns(df, [col])
    series = pd.to_numeric(df[col], errors="coerce")
    non_null = series.dropna()
    if non_null.empty:
        raise ValueError("No funding values available to infer scale")

    max_abs = float(non_null.abs().max())
    scale_used = None
    confidence = 0.0

    # Source-aware strict path: known Binance ingest sources are already decimal.
    if source_col in df.columns:
        source_values = {str(v).strip().lower() for v in df[source_col].dropna().unique().tolist()}
        if source_values and source_values.issubset(_KNOWN_DECIMAL_FUNDING_SOURCES):
            if max_abs > FUNDING_MAX_ABS:
                raise ValueError(
                    "Known source funding rates must already be decimal. "
                    f"Observed max_abs={max_abs} exceeds {FUNDING_MAX_ABS:.4f}."
                )
            scale_used = 1.0
            confidence = 1.0

    if explicit_scale is not None:
        scale_used = float(explicit_scale)
        confidence = 1.0

    if scale_used is None:
        valid_scales: list[float] = []
        for scale in FUNDING_SCALE_CANDIDATES:
            if max_abs * scale <= FUNDING_MAX_ABS:
                valid_scales.append(float(scale))

        if not valid_scales:
            raise ValueError(f"Unable to infer funding scale; max_abs={max_abs}")

        scale_used = valid_scales[0]
        if len(valid_scales) == 1:
            confidence = 1.0
        else:
            # Ambiguous when multiple candidates satisfy the sanity bound.
            # Confidence increases only when inferred scale is very close to the bound.
            cap_utilization = min(1.0, (max_abs * scale_used) / FUNDING_MAX_ABS)
            confidence = float(0.5 + (0.49 * cap_utilization))

    out = df.copy()
    out["funding_rate_scaled"] = series * scale_used
    return out, float(scale_used), float(confidence)


def assert_funding_sane(df: pd.DataFrame, col: str = "funding_rate_scaled") -> None:
    """
    Ensure scaled funding rates are within plausible bounds.
    """
    validate_columns(df, [col])
    series = pd.to_numeric(df[col], errors="coerce")
    non_null = series.dropna()
    if non_null.empty:
        raise ValueError("No funding values to validate")
    max_abs = float(non_null.abs().max())
    if max_abs > FUNDING_MAX_ABS:
        raise ValueError(f"Funding rate exceeds {FUNDING_MAX_ABS:.4f}; max_abs={max_abs}")


def assert_funding_event_grid(
    df: pd.DataFrame, col: str = "timestamp", expected_hours: int = 8
) -> None:
    """
    Validate that funding events lie on the expected hourly grid.
    """
    validate_columns(df, [col])
    assert_monotonic_utc_timestamp(df, col)
    series = df[col]

    if (
        series.dt.minute.ne(0).any()
        or series.dt.second.ne(0).any()
        or series.dt.microsecond.ne(0).any()
    ):
        raise ValueError("Funding timestamps must be on the hour")
    if (series.dt.hour % expected_hours != 0).any():
        raise ValueError(f"Funding timestamps must align to {expected_hours}h grid")

    diffs = series.sort_values().diff().dropna()
    if not diffs.empty:
        hours = diffs.dt.total_seconds() / 3600.0
        multiples = hours / float(expected_hours)
        if not np.allclose(multiples, np.round(multiples), atol=1e-6):
            raise ValueError("Funding timestamps must be spaced on the expected grid")


def is_constant_series(series: pd.Series) -> bool:
    """
    Return True if non-null values are constant (std == 0).
    """
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return False
    std = float(values.std(ddof=0))
    return bool(np.isclose(std, 0.0))


def coerce_timestamps_to_hour(df: pd.DataFrame, col: str = "timestamp") -> tuple[pd.DataFrame, int]:
    """
    Round timestamps to the nearest hour and return adjusted row count.
    """
    validate_columns(df, [col])
    series = df[col]
    if series.isna().any():
        raise ValueError(f"{col} contains nulls")
    ensure_utc_timestamp(series, col)
    rounded = series.dt.round("h")
    adjusted = int((series != rounded).sum())
    out = df.copy()
    out[col] = rounded
    return out, adjusted


def ensure_utc_timestamp(series: pd.Series, name: str) -> pd.Series:
    """
    Validate that a pandas Series of timestamps is timezone-aware UTC.
    """
    if not isinstance(series.dtype, pd.DatetimeTZDtype):
        raise ValueError(f"{name} must be timezone-aware UTC")
    if str(series.dt.tz) != "UTC":
        raise ValueError(f"{name} must be UTC")
    return series


def ts_ns_utc(series: pd.Series, *, allow_nat: bool = False) -> pd.Series:
    """
    Convert a series to datetime64[ns, UTC] with strict validation.
    """
    ts = pd.to_datetime(series, utc=True, errors="coerce")
    if not allow_nat and ts.isna().any():
        raise ValueError("Timestamp series contains NaT or unparseable values")
    return ts.dt.as_unit("ns")


def coerce_to_ns_int(series: pd.Series) -> pd.Series:
    """
    Heuristically convert arbitrary timestamps (ms ints, ns ints, strings, datetimes)
    to int64 epoch nanoseconds.
    """
    if series.empty:
        return pd.Series(dtype="int64")

    if pd.api.types.is_datetime64_any_dtype(series):
        return series.astype("int64")

    s_num = pd.to_numeric(series, errors="coerce")

    if s_num.isna().all():
        # Fallback to string parsing
        ts = pd.to_datetime(series, utc=True, errors="coerce")
        # Replace NaT (-9223372036854775808) with 0 or drop, but astype("int64") does NaT -> MIN_INT
        return ts.astype("int64")

    med = s_num.median()
    if pd.isna(med):
        return s_num.fillna(0).astype("int64")

    # Heuristic: 1e12 is ~2001 in ms. 1e15 is ~1970 days in micro.
    # Current time in ms is ~1.7e12. In ns is ~1.7e18.
    if med < 1e14:
        # Treat as ms
        ts = pd.to_datetime(s_num, unit="ms", utc=True, errors="coerce")
    else:
        # Treat as ns
        # If it's already ns ints, passing to pd.to_datetime with unit="ns" is safe
        ts = pd.to_datetime(s_num, unit="ns", utc=True, errors="coerce")

    return ts.astype("int64")


def validate_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    """
    Ensure that a DataFrame contains the required columns.
    """
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")


def strategy_family_allowed_keys(family: str) -> set[str]:
    return set(_STRATEGY_FAMILY_KEYS.get(family, set()))


def validate_strategy_family_params(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw = config.get("strategy_family_params", {})
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("strategy_family_params must be a mapping of family -> params")

    out: dict[str, dict[str, Any]] = {}
    for family in ["Carry", "MeanReversion", "Spread"]:
        params = raw.get(family, {})
        if params is None:
            continue
        if not isinstance(params, dict):
            raise ValueError(f"strategy_family_params.{family} must be a mapping")
        unknown = sorted(set(params) - _STRATEGY_FAMILY_KEYS[family])
        if unknown:
            raise ValueError(f"strategy_family_params.{family} has unsupported keys: {unknown}")

        norm = dict(params)
        if family == "Carry":
            for key in [
                "funding_percentile_entry_min",
                "funding_percentile_entry_max",
                "normalization_exit_percentile",
            ]:
                if key in norm:
                    value = float(norm[key])
                    if value < 0.0 or value > 100.0:
                        raise ValueError(f"strategy_family_params.Carry.{key} must be in [0, 100]")
                    norm[key] = value
            if (
                "funding_percentile_entry_min" in norm
                and "funding_percentile_entry_max" in norm
                and norm["funding_percentile_entry_min"] > norm["funding_percentile_entry_max"]
            ):
                raise ValueError(
                    "strategy_family_params.Carry.funding_percentile_entry_min must be <= "
                    "funding_percentile_entry_max"
                )
            if "normalization_exit_consecutive_bars" in norm:
                bars = int(norm["normalization_exit_consecutive_bars"])
                if bars < 1:
                    raise ValueError(
                        "strategy_family_params.Carry.normalization_exit_consecutive_bars must be >= 1"
                    )
                norm["normalization_exit_consecutive_bars"] = bars
            if "sizing_curve" in norm:
                curve = str(norm["sizing_curve"]).strip().lower()
                if curve not in _ALLOWED_SIZING_CURVES:
                    allowed = ", ".join(sorted(_ALLOWED_SIZING_CURVES))
                    raise ValueError(
                        f"strategy_family_params.Carry.sizing_curve must be one of: {allowed}"
                    )
                norm["sizing_curve"] = curve
            out[family] = norm

        elif family == "MeanReversion":
            for key in ["zscore_entry_abs", "extension_entry_abs"]:
                if key in norm:
                    value = float(norm[key])
                    if value <= 0.0:
                        raise ValueError(f"strategy_family_params.MeanReversion.{key} must be > 0")
                    norm[key] = value
            if "reversion_target_zscore" in norm:
                norm["reversion_target_zscore"] = float(norm["reversion_target_zscore"])
            if "stop_zscore_abs" in norm and norm["stop_zscore_abs"] is not None:
                stop_z = float(norm["stop_zscore_abs"])
                if stop_z <= 0.0:
                    raise ValueError(
                        "strategy_family_params.MeanReversion.stop_zscore_abs must be > 0 when set"
                    )
                norm["stop_zscore_abs"] = stop_z
            out[family] = norm

        elif family == "Spread":
            for key in ["spread_zscore_entry_abs", "convergence_target_zscore"]:
                if key in norm:
                    value = float(norm[key])
                    if key == "spread_zscore_entry_abs" and value <= 0.0:
                        raise ValueError(
                            "strategy_family_params.Spread.spread_zscore_entry_abs must be > 0"
                        )
                    norm[key] = value
            if "dislocation_threshold_bps" in norm:
                dislocation = float(norm["dislocation_threshold_bps"])
                if dislocation < 0.0:
                    raise ValueError(
                        "strategy_family_params.Spread.dislocation_threshold_bps must be >= 0"
                    )
                norm["dislocation_threshold_bps"] = dislocation
            if "max_hold_bars" in norm:
                hold = int(norm["max_hold_bars"])
                if hold < 1:
                    raise ValueError("strategy_family_params.Spread.max_hold_bars must be >= 1")
                norm["max_hold_bars"] = hold
            out[family] = norm

    return out
