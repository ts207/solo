from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import List

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SplitWindow:
    label: str
    start: pd.Timestamp
    end: pd.Timestamp

    def to_dict(self) -> dict:
        return {"label": self.label, "start": self.start.isoformat(), "end": self.end.isoformat()}


def _normalize_ts(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts


def build_time_splits(
    *,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    train_frac: float = 0.6,
    validation_frac: float = 0.2,
    embargo_days: int = 5,
) -> List[SplitWindow]:
    """
    Deterministic walk-forward windows with optional embargo between split boundaries.
    """
    start_ts = _normalize_ts(start)
    end_ts = _normalize_ts(end)
    if start_ts > end_ts:
        raise ValueError("start must be <= end")
    if not (0.0 < float(train_frac) < 1.0):
        raise ValueError("train_frac must be in (0,1)")
    if not (0.0 < float(validation_frac) < 1.0):
        raise ValueError("validation_frac must be in (0,1)")
    if float(train_frac + validation_frac) >= 1.0:
        raise ValueError("train_frac + validation_frac must be < 1")
    if int(embargo_days) < 0:
        raise ValueError("embargo_days must be >= 0")

    duration = end_ts - start_ts
    train_duration = duration * float(train_frac)
    validation_duration = duration * float(validation_frac)
    embargo = timedelta(days=int(embargo_days))

    train_start = start_ts
    train_end = train_start + train_duration

    validation_start = train_end + embargo
    validation_end = validation_start + validation_duration

    test_start = validation_end + embargo
    test_end = end_ts

    windows: List[SplitWindow] = []
    if train_start <= train_end:
        windows.append(SplitWindow("train", train_start, train_end))
    if validation_start <= validation_end:
        windows.append(SplitWindow("validation", validation_start, validation_end))
    if test_start <= test_end:
        windows.append(SplitWindow("test", test_start, test_end))

    if not windows:
        raise ValueError("No split windows produced for requested range/embargo")
    labels = [w.label for w in windows]
    if labels[0] != "train":
        raise ValueError("Split generation failed: missing train window")
    return windows


def build_time_splits_with_purge(
    *,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    train_frac: float = 0.6,
    validation_frac: float = 0.2,
    embargo_days: int = 5,
    purge_bars: int = 0,
    bar_duration_minutes: int = 5,
) -> List[SplitWindow]:
    """
    Like build_time_splits but trims the tail of each non-test window by purge_bars.
    Purge removes positions whose exit could overlap the embargo/next-split zone.
    purge_bars = max(horizon_bars, entry_lag_bars) + max_feature_lookback_bars.
    """
    if int(purge_bars) < 0:
        raise ValueError("purge_bars must be >= 0")
    windows = build_time_splits(
        start=start,
        end=end,
        train_frac=train_frac,
        validation_frac=validation_frac,
        embargo_days=embargo_days,
    )
    if int(purge_bars) == 0:
        return windows

    purge_delta = timedelta(minutes=int(purge_bars) * int(bar_duration_minutes))
    result: List[SplitWindow] = []
    for w in windows:
        if w.label == "test":
            result.append(w)
        else:
            new_end = w.end - purge_delta
            if new_end < w.start:
                raise ValueError(
                    f"purge_bars={purge_bars} with bar_duration_minutes={bar_duration_minutes} "
                    f"exceeds the {w.label} window length. Reduce purge_bars or extend the window."
                )
            result.append(SplitWindow(w.label, w.start, new_end))
    return result


def build_repeated_walk_forward_folds(
    *,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    n_folds: int = 5,
    train_frac: float = 0.5,
    validation_frac: float = 0.2,
    embargo_days: int = 5,
    purge_bars: int = 0,
    bar_duration_minutes: int = 5,
) -> List[List[SplitWindow]]:
    """
    R2: Create repeated temporal folds.
    Generates a list of walk-forward split sets by shifting the start time.
    """
    start_ts = _normalize_ts(start)
    end_ts = _normalize_ts(end)
    full_duration = end_ts - start_ts
    # Each fold uses a window of (train + val + test + 2*embargo)
    # For repeated folds, we shift the window by a small increment
    shift_delta = full_duration * 0.05  # 5% shift per fold

    all_folds = []
    for i in range(int(n_folds)):
        f_start = start_ts + i * shift_delta
        if f_start + (full_duration * 0.5) > end_ts:
            break  # Not enough data for more folds

        try:
            folds = build_time_splits_with_purge(
                start=f_start,
                end=end_ts,
                train_frac=train_frac,
                validation_frac=validation_frac,
                embargo_days=embargo_days,
                purge_bars=purge_bars,
                bar_duration_minutes=bar_duration_minutes,
            )
            all_folds.append(folds)
        except Exception:
            continue

    return all_folds


def build_walk_forward_split_labels(
    df: pd.DataFrame,
    *,
    time_col: str,
    train_frac: float = 0.6,
    validation_frac: float = 0.2,
    embargo_days: float = 7.0,
) -> pd.Series:
    """Assign deterministic time-ordered walk-forward labels (train/validation/test) using global timestamp cutoffs."""
    if df.empty:
        return pd.Series(dtype="object", index=df.index)

    out = pd.Series("", index=df.index, dtype="object")

    # Compute global timestamp boundaries across all symbols
    ts = pd.to_datetime(df[time_col], utc=True, errors="coerce")
    min_ts = ts.min()
    max_ts = ts.max()

    if pd.isna(min_ts) or pd.isna(max_ts):
        return out

    duration = max_ts - min_ts
    train_end = min_ts + duration * float(train_frac)
    val_end = min_ts + duration * float(train_frac + validation_frac)

    # Add proportional embargo around boundaries (Finding 90)
    embargo = (
        min(pd.Timedelta(days=float(embargo_days)), duration * 0.05)
        if duration.total_seconds() > 0
        else pd.Timedelta(0)
    )

    train_mask = ts < train_end
    val_mask = (ts >= train_end + embargo) & (ts < val_end)
    test_mask = ts >= val_end + embargo

    out.loc[train_mask] = "train"
    out.loc[val_mask] = "validation"
    out.loc[test_mask] = "test"

    return out
