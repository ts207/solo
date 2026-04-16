from __future__ import annotations

from typing import Any

import pandas as pd

from project.events.timestamps import compute_canonical_timestamps


class PITValidationError(ValueError):
    """Raised when milestone-2 point-in-time timing rules are violated."""


def _utc_ts(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        raise PITValidationError("timestamp value cannot be NaT")
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts


def validate_event_row_pit(
    row: dict[str, Any] | pd.Series,
    *,
    timeframe_minutes: int = 5,
    min_signal_delay_bars: int = 1,
) -> None:
    eval_ts = _utc_ts(row["eval_bar_ts"])
    detected_ts = _utc_ts(row["detected_ts"])
    signal_ts = _utc_ts(row["signal_ts"])

    expected = compute_canonical_timestamps(
        eval_ts,
        timeframe_minutes=timeframe_minutes,
        signal_delay_bars=min_signal_delay_bars,
    )
    if detected_ts != expected.detected_ts:
        raise PITValidationError(
            f"detected_ts must equal eval_bar_ts under milestone-2 policy; got {detected_ts} vs {expected.detected_ts}"
        )
    if signal_ts < expected.signal_ts:
        raise PITValidationError(
            f"signal_ts must be at least the next tradable bar after eval_bar_ts; got {signal_ts} < {expected.signal_ts}"
        )


def validate_event_frame_pit(
    events: pd.DataFrame,
    *,
    timeframe_minutes: int = 5,
    min_signal_delay_bars: int = 1,
) -> None:
    if events.empty:
        return
    required = {"eval_bar_ts", "detected_ts", "signal_ts"}
    missing = sorted(required.difference(events.columns))
    if missing:
        raise PITValidationError(f"events frame missing required PIT fields: {missing}")
    for _, row in events.iterrows():
        validate_event_row_pit(
            row,
            timeframe_minutes=timeframe_minutes,
            min_signal_delay_bars=min_signal_delay_bars,
        )


def assert_shifted_rolling_mean(
    feature: pd.Series,
    source: pd.Series,
    *,
    window: int,
    min_periods: int | None = None,
    atol: float = 1e-12,
) -> None:
    expected = source.rolling(window=window, min_periods=min_periods or window).mean().shift(1)
    _assert_series_close(feature, expected, atol=atol, label="shifted rolling mean")


def assert_shifted_rolling_quantile(
    feature: pd.Series,
    source: pd.Series,
    *,
    window: int,
    quantile: float,
    min_periods: int | None = None,
    atol: float = 1e-12,
) -> None:
    expected = (
        source.rolling(window=window, min_periods=min_periods or window).quantile(quantile).shift(1)
    )
    _assert_series_close(feature, expected, atol=atol, label="shifted rolling quantile")


def _assert_series_close(
    actual: pd.Series, expected: pd.Series, *, atol: float, label: str
) -> None:
    actual_num = pd.to_numeric(actual, errors="coerce")
    expected_num = pd.to_numeric(expected, errors="coerce")
    aligned = pd.concat([actual_num.rename("actual"), expected_num.rename("expected")], axis=1)
    comparable = aligned[aligned[["actual", "expected"]].notna().all(axis=1)]
    if comparable.empty:
        return
    diff = (comparable["actual"] - comparable["expected"]).abs()
    if bool((diff > atol).any()):
        first_bad = diff[diff > atol].index[0]
        raise PITValidationError(
            f"{label} does not match shifted historical computation at index {first_bad}: "
            f"actual={comparable.loc[first_bad, 'actual']} expected={comparable.loc[first_bad, 'expected']}"
        )
