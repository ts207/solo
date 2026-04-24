from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

from project.contracts.temporal_contracts import TemporalContract
from project.core.causal_primitives import trailing_percentile_rank
from project.core.feature_schema import feature_dataset_dir_name

# --- Temporal Contract ---

TEMPORAL_CONTRACT = TemporalContract(
    name="funding_persistence",
    output_mode="point_feature",
    observation_clock="event_timestamp",
    decision_lag_bars=1,
    lookback_bars=96,
    uses_current_observation=False,
    calibration_mode="rolling",
    fit_scope="streaming",
    approved_primitives=("trailing_percentile_rank"),
    notes="Detects persistent funding regimes using causal percentile ranks.",
)

FP_DEF_VERSION = "v1"
_PERSISTENCE_PERCENTILE = 85.0
_PERSISTENCE_MIN_BARS = 8
_NORM_DUE_BARS = 96


@dataclass(frozen=True)
class FundingPersistenceConfig:
    def_version: str = FP_DEF_VERSION
    persistence_percentile: float = _PERSISTENCE_PERCENTILE
    persistence_min_bars: int = _PERSISTENCE_MIN_BARS
    norm_due_bars: int = _NORM_DUE_BARS


DEFAULT_FP_CONFIG = FundingPersistenceConfig()
SOURCE_EVENT_TYPE = "FUNDING_PERSISTENCE_TRIGGER"


def _rolling_percentile(series: pd.Series, window: int = 96) -> pd.Series:
    """PIT-safe rolling percentile."""
    return trailing_percentile_rank(series, window=window, lag=1) * 100.0


def _contiguous_runs(mask: pd.Series) -> List[tuple[int, int]]:
    runs: List[tuple[int, int]] = []
    start = None
    for idx, is_true in enumerate(mask.astype(bool).tolist()):
        if is_true and start is None:
            start = idx
        elif not is_true and start is not None:
            runs.append((start, idx - 1))
            start = None
    if start is not None:
        runs.append((start, len(mask) - 1))
    return runs


def build_funding_persistence_state(
    frame: pd.DataFrame,
    symbol: str,
    config: FundingPersistenceConfig = DEFAULT_FP_CONFIG,
) -> pd.DataFrame:
    required = {"timestamp", "funding_rate_scaled"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Missing required columns for funding persistence: {sorted(missing)}")

    df = frame[["timestamp", "funding_rate_scaled"]].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = (
        df.sort_values("timestamp")
        .drop_duplicates(subset=["timestamp"], keep="last")
        .reset_index(drop=True)
    )

    funding_abs = df["funding_rate_scaled"].astype(float).abs().fillna(0.0)
    funding_abs_pct = _rolling_percentile(funding_abs, window=96).fillna(0.0)
    is_candidate = funding_abs_pct >= config.persistence_percentile

    run_len = np.zeros(len(df), dtype=np.int32)
    active = np.zeros(len(df), dtype=np.int8)
    age = np.zeros(len(df), dtype=np.int32)
    event_id = np.array([None] * len(df), dtype=object)
    run_start_idx = np.full(len(df), -1, dtype=np.int32)

    current_len = 0
    event_counter = 0
    current_event_id = None
    current_run_start = -1

    is_cand_vals = is_candidate.values
    for i in range(len(df)):
        if is_cand_vals[i]:
            if current_len == 0:
                current_run_start = i
            current_len += 1
        else:
            current_len = 0
            current_event_id = None
            current_run_start = -1

        run_len[i] = current_len
        run_start_idx[i] = current_run_start

        if current_len >= config.persistence_min_bars:
            active[i] = 1
            age[i] = current_len - config.persistence_min_bars + 1
            if current_len == config.persistence_min_bars:
                event_counter += 1
                current_event_id = f"fp_{config.def_version}_{symbol}_{event_counter:06d}"
            event_id[i] = current_event_id

    out = pd.DataFrame(
        {
            "timestamp": df["timestamp"],
            "fp_def_version": config.def_version,
            "fp_source_event_type": SOURCE_EVENT_TYPE,
            "fp_active": active,
            "fp_age_bars": age,
            "fp_event_id": event_id,
            "fp_run_start_index": run_start_idx,
            "fp_severity": ((funding_abs_pct - config.persistence_percentile) / 100.0).clip(
                lower=0.0
            ),
            "fp_norm_due": ((active == 1) & (age >= config.norm_due_bars)).astype(np.int8),
        }
    )

    out["fp_enter_ts"] = pd.Series([pd.NaT] * len(out), dtype="datetime64[ns, UTC]")
    out["fp_exit_ts"] = pd.Series([pd.NaT] * len(out), dtype="datetime64[ns, UTC]")

    active_mask = out["fp_active"] == 1
    if active_mask.any():
        valid_start_indices = out.loc[active_mask, "fp_run_start_index"].astype(int)
        # Assign as a Series to preserve TZ awareness
        out.loc[active_mask, "fp_enter_ts"] = pd.Series(
            df["timestamp"].values[valid_start_indices],
            index=out.index[active_mask],
            dtype="datetime64[ns, UTC]",
        )

        # Exit TS is the last timestamp of each contiguous run
        for _, group in out[active_mask].groupby("fp_event_id"):
            out.loc[group.index, "fp_exit_ts"] = group["timestamp"].max()

    return out


def load_funding_features(data_root: Path, run_id: str, symbol: str) -> pd.DataFrame:
    from project.io.utils import (
        choose_partition_dir,
        list_parquet_files,
        read_parquet,
        run_scoped_lake_path,
    )

    feature_dataset = feature_dataset_dir_name()
    candidates = [
        run_scoped_lake_path(data_root, run_id, "features", "perp", symbol, "5m", feature_dataset),
        data_root / "lake" / "features" / "perp" / symbol / "5m" / feature_dataset,
    ]
    src = choose_partition_dir(candidates)
    files = list_parquet_files(src) if src else []
    if not files:
        return pd.DataFrame()
    return read_parquet(files)


# --- Registration ---
from project.core.feature_capabilities import register_feature_loader

register_feature_loader("funding", load_funding_features)
register_feature_loader("oi", load_funding_features)
register_feature_loader("liquidation", load_funding_features)
