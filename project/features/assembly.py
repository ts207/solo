from __future__ import annotations

import importlib
import re
from pathlib import Path
from typing import Sequence

import pandas as pd

_PARTITION_YEAR_RE = re.compile(r"year=(\d{4})")
_PARTITION_MONTH_RE = re.compile(r"month=(\d{2})")


def build_features(*args, **kwargs):
    """Research-safe adapter to the canonical feature builder."""
    module_name = "project" + ".pipelines.features.build_features"
    module = importlib.import_module(module_name)
    return getattr(module, "build_features")(*args, **kwargs)


def filter_time_window(
    frame: pd.DataFrame,
    *,
    start: str | None,
    end: str | None,
) -> pd.DataFrame:
    if frame.empty or "timestamp" not in frame.columns or (not start and not end):
        return frame
    out = frame.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    if start:
        start_ts = pd.Timestamp(start, tz="UTC")
        out = out[out["timestamp"] >= start_ts]
    if end:
        end_ts = pd.Timestamp(end, tz="UTC")
        out = out[out["timestamp"] <= end_ts]
    return out.reset_index(drop=True)


def resolve_window_bounds(
    start: str | None,
    end: str | None,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    start_ts = pd.to_datetime(start, utc=True, errors="coerce") if start else None
    end_ts = pd.to_datetime(end, utc=True, errors="coerce") if end else None
    if start_ts is not None and pd.isna(start_ts):
        start_ts = None
    if end_ts is not None and pd.isna(end_ts):
        end_ts = None
    if end_ts is not None and end:
        end_text = str(end).strip()
        if len(end_text) == 10 and "T" not in end_text:
            end_ts = end_ts + pd.Timedelta(days=1)
    return start_ts, end_ts


def partition_month_key(path: Path) -> tuple[int, int] | None:
    text = str(path)
    year_match = _PARTITION_YEAR_RE.search(text)
    month_match = _PARTITION_MONTH_RE.search(text)
    if not year_match or not month_match:
        return None
    return int(year_match.group(1)), int(month_match.group(1))


def prune_partition_files_by_window(
    files: Sequence[Path],
    *,
    start: str | None,
    end: str | None,
) -> list[Path]:
    start_ts, end_ts = resolve_window_bounds(start, end)
    if (start_ts is None and end_ts is None) or not files:
        return list(files)

    def _month_floor(ts: pd.Timestamp) -> tuple[int, int]:
        return ts.year, ts.month

    min_month = _month_floor(start_ts) if start_ts is not None else None
    max_month = _month_floor(end_ts) if end_ts is not None else None
    pruned: list[Path] = []
    for file_path in files:
        month_key = partition_month_key(file_path)
        if month_key is None:
            pruned.append(file_path)
            continue
        if min_month is not None and month_key < min_month:
            continue
        if max_month is not None and month_key > max_month:
            continue
        pruned.append(file_path)
    return pruned or list(files)


__all__ = [
    "build_features",
    "filter_time_window",
    "prune_partition_files_by_window",
    "resolve_window_bounds",
    "partition_month_key",
]
