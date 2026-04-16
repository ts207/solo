from __future__ import annotations

import os
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from project.core.feature_schema import feature_dataset_dir_name
from project.io.utils import (
    choose_partition_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)
from project.core.validation import ensure_utc_timestamp
from project import PROJECT_ROOT

_DEFAULT_TIMEFRAME = "5m"


def dedupe_timestamp_rows(frame: pd.DataFrame, *, label: str) -> Tuple[pd.DataFrame, int]:
    if frame.empty or "timestamp" not in frame.columns:
        return frame, 0
    out = frame.sort_values("timestamp").copy()
    dupes = int(out["timestamp"].duplicated(keep="last").sum())
    return out.drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True), dupes


def load_symbol_raw_data(
    data_root: Path,
    symbol: str,
    run_id: str,
    timeframe: str = _DEFAULT_TIMEFRAME,
    bars_columns: List[str] | None = None,
    feature_columns: List[str] | None = None,
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    feature_dataset = feature_dataset_dir_name()
    feature_candidates = [
        run_scoped_lake_path(
            data_root, run_id, "features", "perp", symbol, timeframe, feature_dataset
        ),
        data_root / "lake" / "features" / "perp" / symbol / timeframe / feature_dataset,
    ]
    bars_candidates = [
        run_scoped_lake_path(data_root, run_id, "cleaned", "perp", symbol, f"bars_{timeframe}"),
        data_root / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}",
    ]
    features_dir = choose_partition_dir(feature_candidates)
    bars_dir = choose_partition_dir(bars_candidates)
    feature_files = list_parquet_files(features_dir) if features_dir else []
    bars_files = list_parquet_files(bars_dir) if bars_dir else []

    # Prune files by date if possible (optional enhancement, for now we read and filter)
    # Most engines perform best with full vectorized history, but we can pruning early
    features = read_parquet(feature_files, columns=feature_columns)
    bars = read_parquet(bars_files, columns=bars_columns)

    if features.empty or bars.empty:
        raise ValueError(f"Missing data for {symbol} (timeframe={timeframe}).")

    if "timestamp" not in features.columns:
        # If columns were pruned, ensure timestamp is always present for filtering
        features = read_parquet(
            feature_files, columns=(feature_columns + ["timestamp"] if feature_columns else None)
        )
    if "timestamp" not in bars.columns:
        bars = read_parquet(
            bars_files, columns=(bars_columns + ["timestamp"] if bars_columns else None)
        )

    features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
    bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
    ensure_utc_timestamp(features["timestamp"], "timestamp")
    ensure_utc_timestamp(bars["timestamp"], "timestamp")

    # Filter by date early to save memory
    if start_ts is not None:
        features = features[features["timestamp"] >= start_ts]
        bars = bars[bars["timestamp"] >= start_ts]
    if end_ts is not None:
        features = features[features["timestamp"] <= end_ts]
        bars = bars[bars["timestamp"] <= end_ts]

    features, _ = dedupe_timestamp_rows(features, label=f"features:{symbol}:{timeframe}")
    bars, _ = dedupe_timestamp_rows(bars, label=f"bars:{symbol}:{timeframe}")

    return bars, features


def load_universe_snapshots(data_root: Path, run_id: str) -> pd.DataFrame:
    candidates = [
        data_root / "lake" / "runs" / run_id / "metadata" / "universe_snapshots",
        data_root / "lake" / "metadata" / "universe_snapshots",
    ]
    src = choose_partition_dir(candidates)
    files = list_parquet_files(src) if src else []
    if not files:
        return pd.DataFrame(columns=["symbol", "listing_start", "listing_end"])
    frame = read_parquet(files)
    if frame.empty or not {"symbol", "listing_start", "listing_end"}.issubset(set(frame.columns)):
        return pd.DataFrame(columns=["symbol", "listing_start", "listing_end"])
    frame = frame[["symbol", "listing_start", "listing_end"]].copy()
    frame["listing_start"] = pd.to_datetime(frame["listing_start"], utc=True, errors="coerce")
    frame["listing_end"] = pd.to_datetime(frame["listing_end"], utc=True, errors="coerce")
    return frame.dropna(subset=["symbol", "listing_start", "listing_end"]).copy()
