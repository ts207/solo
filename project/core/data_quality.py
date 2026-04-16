from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class DataQualitySummary:
    rows: int
    missing_ratio: float
    outlier_ratio: float
    duplicate_timestamp_count: int
    timestamp_gap_count: int
    max_timestamp_gap_bars: int
    gap_ratio: float
    max_gap_len: int
    coerced_value_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows": int(self.rows),
            "missing_ratio": float(self.missing_ratio),
            "outlier_ratio": float(self.outlier_ratio),
            "duplicate_timestamp_count": int(self.duplicate_timestamp_count),
            "timestamp_gap_count": int(self.timestamp_gap_count),
            "max_timestamp_gap_bars": int(self.max_timestamp_gap_bars),
            "gap_ratio": float(self.gap_ratio),
            "max_gap_len": int(self.max_gap_len),
            "coerced_value_count": int(self.coerced_value_count),
        }


def _safe_numeric_columns(frame: pd.DataFrame, columns: Iterable[str] | None = None) -> list[str]:
    if columns is not None:
        return [col for col in columns if col in frame.columns]
    return [
        col
        for col in frame.columns
        if pd.api.types.is_numeric_dtype(frame[col]) and str(col) not in {"timestamp"}
    ]


def _outlier_ratio(frame: pd.DataFrame, columns: list[str], z_threshold: float = 10.0) -> float:
    if frame.empty or not columns:
        return 0.0
    numeric = frame[columns].apply(pd.to_numeric, errors="coerce")
    if numeric.empty:
        return 0.0
    mean = numeric.mean()
    std = numeric.std().replace(0.0, np.nan)
    z_scores = (numeric - mean) / std
    mask = (z_scores.abs() > z_threshold).fillna(False)
    total = int(mask.count().sum())
    if total <= 0:
        return 0.0
    return float(mask.sum().sum() / total)


def _timestamp_gap_metrics(
    timestamps: pd.Series,
    *,
    expected_freq: str | None = None,
    expected_minutes: int | None = None,
) -> tuple[int, int]:
    if timestamps.empty:
        return 0, 0
    ts = pd.to_datetime(timestamps, utc=True, errors="coerce").dropna().sort_values()
    if len(ts) < 2:
        return 0, 0
    deltas = ts.diff().dropna()
    if expected_minutes is not None:
        expected_delta = pd.Timedelta(minutes=int(expected_minutes))
    elif expected_freq:
        expected_delta = pd.Timedelta(expected_freq)
    else:
        inferred = deltas.mode()
        expected_delta = inferred.iloc[0] if not inferred.empty else deltas.min()
    gap_mask = deltas > expected_delta
    if not gap_mask.any():
        return 0, 0
    gap_bars = (deltas[gap_mask] / expected_delta).round().astype(int) - 1
    gap_bars = gap_bars.clip(lower=1)
    return int(gap_mask.sum()), int(gap_bars.max())


def summarize_frame_quality(
    frame: pd.DataFrame,
    *,
    timestamp_col: str = "timestamp",
    numeric_cols: Iterable[str] | None = None,
    gap_col: str = "is_gap",
    expected_freq: str | None = None,
    expected_minutes: int | None = None,
    coerced_value_count: int = 0,
    z_threshold: float = 10.0,
) -> DataQualitySummary:
    rows = int(len(frame))
    numeric_columns = _safe_numeric_columns(frame, numeric_cols)
    missing_ratio = 0.0
    if rows > 0 and numeric_columns:
        numeric = frame[numeric_columns].apply(pd.to_numeric, errors="coerce")
        missing_ratio = float(numeric.isna().mean().mean())
    duplicate_timestamp_count = 0
    timestamp_gap_count = 0
    max_timestamp_gap_bars = 0
    if timestamp_col in frame.columns:
        ts = pd.to_datetime(frame[timestamp_col], utc=True, errors="coerce")
        duplicate_timestamp_count = int(ts.duplicated().sum())
        timestamp_gap_count, max_timestamp_gap_bars = _timestamp_gap_metrics(
            ts,
            expected_freq=expected_freq,
            expected_minutes=expected_minutes,
        )
    gap_ratio = 0.0
    max_gap_len = 0
    if gap_col in frame.columns and rows > 0:
        gap_mask = frame[gap_col].fillna(False).astype(bool)
        gap_ratio = float(gap_mask.mean())
        if "gap_len" in frame.columns:
            max_gap_len = int(pd.to_numeric(frame["gap_len"], errors="coerce").fillna(0).max())
        elif gap_mask.any():
            groups = (gap_mask != gap_mask.shift()).cumsum()
            max_gap_len = int(gap_mask.groupby(groups).sum().max())
    return DataQualitySummary(
        rows=rows,
        missing_ratio=missing_ratio,
        outlier_ratio=_outlier_ratio(frame, numeric_columns, z_threshold=z_threshold),
        duplicate_timestamp_count=duplicate_timestamp_count,
        timestamp_gap_count=timestamp_gap_count,
        max_timestamp_gap_bars=max_timestamp_gap_bars,
        gap_ratio=gap_ratio,
        max_gap_len=max_gap_len,
        coerced_value_count=int(coerced_value_count),
    )
