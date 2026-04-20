from __future__ import annotations
from project.core.config import get_data_root

import pandas as pd
from project.core.feature_schema import feature_dataset_dir_name
from project.io.utils import (
    choose_partition_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)

def get_research_data_root() -> Path:
    return get_data_root()


def load_research_features(run_id: str, symbol: str, timeframe: str = "5m") -> pd.DataFrame:
    data_root = get_research_data_root()
    feature_dataset = feature_dataset_dir_name()
    candidates = [
        run_scoped_lake_path(
            data_root, run_id, "features", "perp", symbol, timeframe, feature_dataset
        ),
        data_root / "lake" / "features" / "perp" / symbol / timeframe / feature_dataset,
    ]
    features_dir = choose_partition_dir(candidates)
    files = list_parquet_files(features_dir) if features_dir else []
    if not files:
        return pd.DataFrame()
    frame = read_parquet(files)
    if frame.empty or "timestamp" not in frame.columns:
        return pd.DataFrame()
    frame = frame.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return frame


def normalize_research_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "timestamp" in out.columns:
        out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    if "symbol" in out.columns:
        out["symbol"] = out["symbol"].astype(str).str.upper()
    return out
