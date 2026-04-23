from __future__ import annotations

import numpy as np
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any

from project.io.utils import ensure_dir, read_parquet, write_parquet
from project.events.shared import EVENT_COLUMNS, emit_event, format_event_id
from project.events.sparsify import sparsify_mask
from project.events.thresholding import rolling_mean_std_zscore, rolling_quantile_threshold


def sparsify_event_mask(mask: pd.Series, min_spacing: int) -> List[int]:
    return list(sparsify_mask(mask, min_spacing=int(min_spacing)))


def rolling_z_score(series: pd.Series, window: int) -> pd.Series:
    return rolling_mean_std_zscore(series, window=int(window), shift=0)


def rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    """Calculates rolling percentile rank."""

    def pct_rank(values: np.ndarray) -> float:
        valid = values[~np.isnan(values)]
        if len(valid) == 0:
            return np.nan
        last = values[-1]
        if np.isnan(last):
            return np.nan
        return float(np.sum(valid <= last) / len(valid) * 100.0)

    return series.rolling(window=window, min_periods=window).apply(pct_rank, raw=True)


def safe_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return pd.to_numeric(df[column], errors="coerce")
    return pd.Series(np.nan, index=df.index, dtype=float)


def rows_for_event(
    df: pd.DataFrame,
    *,
    symbol: str,
    event_type: str,
    mask: pd.Series,
    event_score: pd.Series | None = None,
    min_spacing: int = 6,
    log_path: str | None = None,
    seed: int | None = None,
) -> pd.DataFrame:
    idxs = sparsify_event_mask(mask, min_spacing=min_spacing)
    if not idxs:
        return pd.DataFrame(columns=EVENT_COLUMNS)

    score_series = event_score if event_score is not None else safe_series(df, "rv_96")
    basis_series = safe_series(df, "basis_zscore")
    if basis_series.isna().all():
        basis_series = safe_series(df, "cross_exchange_spread_z")

    rows: List[Dict[str, object]] = []
    for n, idx in enumerate(idxs):
        ts = pd.to_datetime(df.at[idx, "timestamp"], utc=True, errors="coerce")
        if pd.isna(ts):
            continue

        intensity = float(np.nan_to_num(score_series.iloc[idx], nan=1.0))
        severity = "moderate"
        if intensity >= 4.0:
            severity = "extreme"
        elif intensity <= 1.5:
            severity = "minor"

        event_id = format_event_id(event_type, symbol, int(idx), n)

        metadata = {
            "event_idx": int(idx),
            "basis_z": float(np.nan_to_num(basis_series.iloc[idx], nan=0.0)),
            "spread_z": float(np.nan_to_num(safe_series(df, "spread_zscore").iloc[idx], nan=0.0)),
            "funding_rate_bps": float(
                np.nan_to_num(safe_series(df, "funding_rate_scaled").iloc[idx], nan=0.0)
            ),
        }

        row = emit_event(
            event_type=event_type,
            symbol=symbol,
            event_id=event_id,
            eval_bar_ts=ts,
            intensity=intensity,
            severity=severity,
            metadata=metadata,
        )
        rows.append(row)

    return pd.DataFrame(rows, columns=EVENT_COLUMNS)


def past_quantile(
    series: pd.Series, q: float, window: int = 576, min_periods: int = 96
) -> pd.Series:
    """Calculates rolling quantile shifted by 1 bar."""
    return rolling_quantile_threshold(
        series, window=int(window), quantile=float(q), min_periods=int(min_periods), shift=1
    )


import fcntl


def merge_event_artifacts(out_path: Path, event_type: str, new_df: pd.DataFrame) -> pd.DataFrame:
    ensure_dir(out_path.parent)
    lock_path = out_path.parent / f"{out_path.name}.lock"

    with open(lock_path, "w") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX)

            if out_path.exists():
                try:
                    if out_path.suffix.lower() == ".parquet":
                        prior = read_parquet([out_path])
                    else:
                        prior = pd.read_csv(out_path)
                except Exception:
                    prior = pd.DataFrame()

                if not prior.empty and "event_type" in prior.columns:
                    prior = prior[prior["event_type"].astype(str) != event_type].copy()
                    new_df = pd.concat([prior, new_df], ignore_index=True)

            if out_path.suffix.lower() == ".parquet":
                write_parquet(new_df, out_path, skip_lock=True)
            else:
                new_df.to_csv(out_path, index=False)

            return new_df
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)
