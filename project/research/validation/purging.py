from __future__ import annotations

import pandas as pd

from project.research.validation.splits import bars_to_timedelta


def compute_event_windows(
    events: pd.DataFrame,
    *,
    time_col: str = "enter_ts",
    horizon_bars: int | None = None,
    horizon_col: str | None = None,
    entry_lag_bars: int = 0,
    bar_duration_minutes: int = 5,
) -> pd.DataFrame:
    if events.empty or time_col not in events.columns:
        return events.copy()
    out = events.copy()
    start_ts = pd.to_datetime(out[time_col], utc=True, errors="coerce")
    out[time_col] = start_ts
    lag_delta = bars_to_timedelta(entry_lag_bars, bar_duration_minutes=bar_duration_minutes)
    if horizon_col and horizon_col in out.columns:
        horizons = pd.to_numeric(out[horizon_col], errors="coerce").fillna(0).astype(int)
    else:
        horizons = pd.Series(int(horizon_bars or 0), index=out.index, dtype=int)
    end_ts = (
        start_ts
        + lag_delta
        + horizons.apply(
            lambda v: bars_to_timedelta(int(v), bar_duration_minutes=bar_duration_minutes)
        )
    )
    out["event_window_start"] = start_ts
    out["event_window_end"] = end_ts
    return out


def purge_overlapping_events(
    events: pd.DataFrame,
    *,
    boundary_start: str | pd.Timestamp,
    boundary_end: str | pd.Timestamp,
    start_col: str = "event_window_start",
    end_col: str = "event_window_end",
) -> pd.DataFrame:
    if events.empty:
        return events.copy()
    out = events.copy()
    boundary_start_ts = pd.Timestamp(boundary_start)
    boundary_end_ts = pd.Timestamp(boundary_end)
    starts = pd.to_datetime(out[start_col], utc=True, errors="coerce")
    ends = pd.to_datetime(out[end_col], utc=True, errors="coerce")
    overlap = starts.lt(boundary_end_ts) & ends.gt(boundary_start_ts)
    return out.loc[~overlap].copy()


def apply_embargo(
    events: pd.DataFrame,
    *,
    anchor_end: str | pd.Timestamp,
    embargo_bars: int,
    time_col: str = "enter_ts",
    bar_duration_minutes: int = 5,
) -> pd.DataFrame:
    if events.empty or int(embargo_bars) <= 0:
        return events.copy()
    out = events.copy()
    ts = pd.to_datetime(out[time_col], utc=True, errors="coerce")
    anchor_end_ts = pd.Timestamp(anchor_end)
    embargo_end = anchor_end_ts + bars_to_timedelta(
        embargo_bars, bar_duration_minutes=bar_duration_minutes
    )
    mask = (ts > anchor_end_ts) & (ts < embargo_end)
    return out.loc[~mask].copy()
