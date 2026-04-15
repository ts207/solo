from __future__ import annotations

import logging
import re
from datetime import timedelta
from typing import Any, Iterable, List, Optional

import pandas as pd

from project.research.validation.schemas import ValidationSplit


DEFAULT_BAR_DURATION_MINUTES = 5
log = logging.getLogger(__name__)
_DEFAULT_SPLIT_SCHEME_ID = "WF_60_20_20"
_SPLIT_SCHEME_ALIASES = {
    "SMOKE_TVT": (0.6, 0.2),
    "WF_60_20_20": (0.6, 0.2),
    "TVT_60_20_20": (0.6, 0.2),
}


def normalize_timestamp(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tz is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def bars_to_timedelta(
    bars: int, *, bar_duration_minutes: int = DEFAULT_BAR_DURATION_MINUTES
) -> pd.Timedelta:
    return pd.Timedelta(minutes=max(0, int(bars)) * max(1, int(bar_duration_minutes)))


def resolve_split_scheme(split_scheme_id: str | None) -> tuple[str, float, float]:
    raw = (
        str(split_scheme_id or _DEFAULT_SPLIT_SCHEME_ID).strip().upper() or _DEFAULT_SPLIT_SCHEME_ID
    )
    if raw in _SPLIT_SCHEME_ALIASES:
        train_frac, validation_frac = _SPLIT_SCHEME_ALIASES[raw]
        return raw, float(train_frac), float(validation_frac)

    match = re.fullmatch(r"(?:WF|TVT)_(\d{1,2})_(\d{1,2})_(\d{1,2})", raw)
    if match:
        train_pct, validation_pct, test_pct = (int(token) for token in match.groups())
        total = train_pct + validation_pct + test_pct
        if total != 100:
            raise ValueError(f"split_scheme_id must sum to 100, got {raw!r}")
        return raw, float(train_pct) / 100.0, float(validation_pct) / 100.0

    raise ValueError(f"unsupported split_scheme_id: {split_scheme_id!r}")


def build_validation_splits(
    *,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    train_frac: float = 0.6,
    validation_frac: float = 0.2,
    embargo_bars: int = 0,
    purge_bars: int = 0,
    bar_duration_minutes: int = DEFAULT_BAR_DURATION_MINUTES,
) -> List[ValidationSplit]:
    start_ts = normalize_timestamp(start)
    end_ts = normalize_timestamp(end)
    if start_ts > end_ts:
        raise ValueError("start must be <= end")
    if not (0.0 < float(train_frac) < 1.0):
        raise ValueError("train_frac must be in (0,1)")
    if not (0.0 < float(validation_frac) < 1.0):
        raise ValueError("validation_frac must be in (0,1)")
    if float(train_frac) + float(validation_frac) >= 1.0:
        raise ValueError("train_frac + validation_frac must be < 1")

    duration = end_ts - start_ts
    embargo_delta = bars_to_timedelta(embargo_bars, bar_duration_minutes=bar_duration_minutes)
    purge_delta = bars_to_timedelta(purge_bars, bar_duration_minutes=bar_duration_minutes)

    train_start = start_ts
    train_end_nominal = train_start + duration * float(train_frac)
    train_end = train_end_nominal - purge_delta

    validation_start = train_end_nominal + embargo_delta
    validation_duration = duration * float(validation_frac)
    validation_end_nominal = validation_start + validation_duration
    validation_end = validation_end_nominal - purge_delta

    test_start = validation_end_nominal + embargo_delta
    test_end = end_ts

    windows: List[ValidationSplit] = []
    if train_end < train_start:
        raise ValueError("purge_bars trims train window below zero length")
    windows.append(
        ValidationSplit(
            label="train",
            start=train_start,
            end=train_end,
            purge_bars=int(purge_bars),
            embargo_bars=int(embargo_bars),
            bar_duration_minutes=int(bar_duration_minutes),
        )
    )
    if validation_end >= validation_start:
        windows.append(
            ValidationSplit(
                label="validation",
                start=validation_start,
                end=validation_end,
                purge_bars=int(purge_bars),
                embargo_bars=int(embargo_bars),
                bar_duration_minutes=int(bar_duration_minutes),
            )
        )
    if test_end >= test_start:
        windows.append(
            ValidationSplit(
                label="test",
                start=test_start,
                end=test_end,
                purge_bars=int(purge_bars),
                embargo_bars=int(embargo_bars),
                bar_duration_minutes=int(bar_duration_minutes),
            )
        )
    if not windows:
        raise ValueError("No validation windows produced")
    return windows


def assign_split_labels(
    df: pd.DataFrame,
    *,
    time_col: str,
    train_frac: float = 0.6,
    validation_frac: float = 0.2,
    embargo_bars: int = 0,
    purge_bars: int = 0,
    bar_duration_minutes: int = DEFAULT_BAR_DURATION_MINUTES,
    split_col: str = "split_label",
    event_window_start_col: str | None = None,
    event_window_end_col: str | None = None,
) -> pd.DataFrame:
    if df.empty or time_col not in df.columns:
        return df.copy()

    out = df.copy()
    ts = pd.to_datetime(out[time_col], utc=True, errors="coerce")
    out[time_col] = ts
    window_start = None
    window_end = None
    if event_window_start_col and event_window_start_col in out.columns:
        window_start = pd.to_datetime(out[event_window_start_col], utc=True, errors="coerce")
        out[event_window_start_col] = window_start
    if event_window_end_col and event_window_end_col in out.columns:
        window_end = pd.to_datetime(out[event_window_end_col], utc=True, errors="coerce")
        out[event_window_end_col] = window_end

    use_event_windows = window_start is not None and window_end is not None
    if use_event_windows:
        valid = ts.notna() & window_start.notna() & window_end.notna()
    else:
        valid = ts.notna()

    if valid.sum() < 2:
        out[split_col] = "train"
        out["non_promotable"] = True
        return out

    windows = build_validation_splits(
        start=ts[valid].min(),
        end=ts[valid].max(),
        train_frac=train_frac,
        validation_frac=validation_frac,
        embargo_bars=embargo_bars,
        purge_bars=purge_bars,
        bar_duration_minutes=bar_duration_minutes,
    )
    labels = pd.Series([pd.NA] * len(out), index=out.index, dtype="object")
    for window in windows:
        if use_event_windows:
            assert window_start is not None and window_end is not None
            mask = valid & (window_start >= window.start) & (window_end <= window.end)
        elif window.label == "test":
            mask = valid & (ts >= window.start) & (ts <= window.end)
        else:
            mask = valid & (ts >= window.start) & (ts < window.end)
        labels.loc[mask] = window.label

    excluded_mask = valid & labels.isna()
    if bool(excluded_mask.any()):
        logging.getLogger(__name__).debug(f"Dropping {excluded_mask.sum()} rows due to embargo/purge/invalid split.")
        out = out.loc[~excluded_mask].copy()
        labels = labels.loc[out.index]
    out[split_col] = labels.astype(str)
    out["split_plan_id"] = (
        f"TVT_{int(round(train_frac * 100))}_{int(round(validation_frac * 100))}_{100 - int(round((train_frac + validation_frac) * 100))}"
    )
    out["purge_bars_used"] = int(purge_bars)
    out["embargo_bars_used"] = int(embargo_bars)
    out["bar_duration_minutes"] = int(bar_duration_minutes)
    return out


def serialize_splits(splits: Iterable[ValidationSplit]) -> list[dict]:
    return [split.to_dict() for split in splits]

def build_repeated_walkforward_splits(
    timestamps: pd.Series | pd.DatetimeIndex | list,
    *,
    train_bars: int,
    validation_bars: int,
    test_bars: int,
    step_bars: int,
    min_folds: int,
    max_folds: int | None,
    purge_bars: int = 0,
    embargo_bars: int = 0,
    bar_duration_minutes: int = DEFAULT_BAR_DURATION_MINUTES,
) -> List[Any]:
    """Build a list of FoldDefinition objects for repeated walk-forward evaluation.

    Returns an empty list when fewer than *min_folds* valid folds can be
    constructed.  **Callers must check for an empty return** — a hypothesis
    evaluated against an empty fold list will produce NaN fold-stability scores
    with no other diagnostic, which can cause it to fail the fold-stability gate
    silently.  A WARNING is emitted here so the log trace is available.

    Purge/embargo implementation note
    -----------------------------------
    Purge and embargo are applied as *calendar-time deltas* (via
    ``bars_to_timedelta``), not as *row counts*.  In continuously-trading
    24/7 crypto markets this is approximately equivalent.  However, if the
    data contains gaps (exchange maintenance, collection outages), the actual
    number of rows excluded from training may be fewer than *purge_bars*,
    which can allow information leakage from near-event-time bars into the
    validation window.

    # TODO(purge-row-count): Implement an alternative row-index-based purge
    # mode (purge_mode='rows') that removes exactly *purge_bars* rows rather
    # than *purge_bars × bar_duration_minutes* of calendar time.  This is
    # the correct approach for data with irregular or sparse bar density.
    """
    ts = pd.to_datetime(timestamps).sort_values()
    if len(ts) == 0:
        return []

    from project.research.validation.schemas import FoldDefinition, ValidationSplit

    folds: List[FoldDefinition] = []
    
    total_bars = len(ts)
    if total_bars == 0:
        return folds

    embargo_delta = bars_to_timedelta(embargo_bars, bar_duration_minutes=bar_duration_minutes)
    purge_delta = bars_to_timedelta(purge_bars, bar_duration_minutes=bar_duration_minutes)

    fold_size = train_bars + validation_bars + test_bars
    start_idx = 0
    fold_id = 1
    
    while True:
        if start_idx + fold_size > total_bars:
            break
            
        train_start_ts = ts.iloc[start_idx]
        train_end_nominal_idx = start_idx + train_bars - 1
        train_end_nominal_ts = ts.iloc[train_end_nominal_idx]
        
        valid_start_idx = train_end_nominal_idx + 1
        valid_end_nominal_idx = valid_start_idx + validation_bars - 1
        valid_start_ts = ts.iloc[valid_start_idx]
        valid_end_nominal_ts = ts.iloc[valid_end_nominal_idx]
        
        test_start_idx = valid_end_nominal_idx + 1
        test_end_nominal_idx = test_start_idx + test_bars - 1
        test_start_ts = ts.iloc[test_start_idx]
        test_end_ts = ts.iloc[test_end_nominal_idx]
        
        train_end_ts = train_end_nominal_ts - purge_delta
        adjusted_valid_start = max(valid_start_ts, train_end_nominal_ts + embargo_delta)
        adjusted_valid_end = valid_end_nominal_ts - purge_delta
        adjusted_test_start = max(test_start_ts, valid_end_nominal_ts + embargo_delta)
        
        if train_end_ts < train_start_ts or adjusted_valid_end < adjusted_valid_start or test_end_ts < adjusted_test_start:
            start_idx += step_bars
            continue
            
        t_split = ValidationSplit(
            label="train",
            start=normalize_timestamp(train_start_ts),
            end=normalize_timestamp(train_end_ts),
            purge_bars=int(purge_bars),
            embargo_bars=int(embargo_bars),
            bar_duration_minutes=int(bar_duration_minutes)
        )
        
        v_split = ValidationSplit(
            label="validation",
            start=normalize_timestamp(adjusted_valid_start),
            end=normalize_timestamp(adjusted_valid_end),
            purge_bars=int(purge_bars),
            embargo_bars=int(embargo_bars),
            bar_duration_minutes=int(bar_duration_minutes)
        )
        
        test_split = ValidationSplit(
            label="test",
            start=normalize_timestamp(adjusted_test_start),
            end=normalize_timestamp(test_end_ts),
            purge_bars=int(purge_bars),
            embargo_bars=int(embargo_bars),
            bar_duration_minutes=int(bar_duration_minutes)
        )
        
        folds.append(FoldDefinition(
            fold_id=fold_id,
            train_split=t_split,
            validation_split=v_split,
            test_split=test_split
        ))
        
        fold_id += 1
        start_idx += step_bars
        
        if max_folds is not None and len(folds) >= max_folds:
            break
            
    if len(folds) < min_folds:
        log.warning(
            "build_repeated_walkforward_splits: could not satisfy min_folds=%d. "
            "Built %d valid fold(s) from %d total bars "
            "(fold_size=%d = train_bars=%d + validation_bars=%d + test_bars=%d, "
            "step_bars=%d). "
            "Increase the data window or reduce min_folds / fold_size. "
            "Returning empty fold list — hypothesis fold-stability scores will be NaN.",
            min_folds,
            len(folds),
            total_bars,
            train_bars + validation_bars + test_bars,
            train_bars,
            validation_bars,
            test_bars,
            step_bars,
        )
        return []
        
    return folds

