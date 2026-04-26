from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path

import pandas as pd

from project.core.feature_schema import feature_dataset_dir_name
from project.io.utils import (
    choose_partition_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)

_LOG = logging.getLogger(__name__)


class ProjectDataRepository:
    """
    Centralized data access layer for Project Edge.
    Handles run-scoped path resolution, partitioned reading, and LRU caching.
    """

    def __init__(self, data_root: Path, run_id: str | None = None):
        self.data_root = data_root
        self.run_id = run_id

    @lru_cache(maxsize=32)
    def load_bars(self, symbol: str, timeframe: str = "5m", market: str = "perp") -> pd.DataFrame:
        """Load cleaned bar data, prioritizing run-scoped overrides."""
        candidates = []
        if self.run_id:
            candidates.append(
                run_scoped_lake_path(
                    self.data_root, self.run_id, "cleaned", market, symbol, f"bars_{timeframe}"
                )
            )
        candidates.append(
            self.data_root / "lake" / "cleaned" / market / symbol / f"bars_{timeframe}"
        )

        return self._read_partitioned_candidates(candidates, f"bars for {symbol} {timeframe}")

    @lru_cache(maxsize=32)
    def load_features(
        self,
        symbol: str,
        timeframe: str = "5m",
        market: str = "perp",
        version: str | None = None,
    ) -> pd.DataFrame:
        """Load feature data, prioritizing run-scoped overrides."""
        dataset_name = feature_dataset_dir_name(version)
        candidates = []
        if self.run_id:
            candidates.append(
                run_scoped_lake_path(
                    self.data_root, self.run_id, "features", market, symbol, timeframe, dataset_name
                )
            )
        candidates.append(
            self.data_root / "lake" / "features" / market / symbol / timeframe / dataset_name
        )

        return self._read_partitioned_candidates(
            candidates, f"features {dataset_name} for {symbol} {timeframe}"
        )

    @lru_cache(maxsize=32)
    def load_market_context(
        self, symbol: str, timeframe: str = "5m", market: str = "perp"
    ) -> pd.DataFrame:
        """Load market context features."""
        candidates = []
        if self.run_id:
            candidates.append(
                run_scoped_lake_path(
                    self.data_root,
                    self.run_id,
                    "features",
                    market,
                    symbol,
                    timeframe,
                    "market_context",
                )
            )
        candidates.append(
            self.data_root / "lake" / "features" / market / symbol / timeframe / "market_context"
        )
        if self.run_id:
            candidates.append(
                run_scoped_lake_path(
                    self.data_root,
                    self.run_id,
                    "context",
                    "market_state",
                    symbol,
                    timeframe,
                )
            )
        candidates.append(
            self.data_root / "lake" / "context" / "market_state" / symbol / timeframe
        )
        if self.run_id:
            candidates.append(
                run_scoped_lake_path(
                    self.data_root,
                    self.run_id,
                    "context",
                    "market_state",
                    symbol,
                )
            )
        candidates.append(self.data_root / "lake" / "context" / "market_state" / symbol)
        candidates.append(self.data_root / "features" / "context" / "market_state" / symbol)

        return self._read_partitioned_candidates(
            candidates, f"market context for {symbol} {timeframe}"
        )

    def _read_partitioned_candidates(self, candidates: list[Path], label: str) -> pd.DataFrame:
        path_dir = choose_partition_dir(candidates)
        if not path_dir:
            _LOG.warning(f"No data found for {label}")
            return pd.DataFrame()

        files = list_parquet_files(path_dir)
        if not files:
            return pd.DataFrame()

        df = read_parquet(files)
        if df.empty:
            return pd.DataFrame()

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            df = df.sort_values("timestamp").reset_index(drop=True)

        return df

    def clear_cache(self):
        self.load_bars.cache_clear()
        self.load_features.cache_clear()
        self.load_market_context.cache_clear()
