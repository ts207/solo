from __future__ import annotations

import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, Tuple, Mapping

from project.io.utils import (
    choose_partition_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)
from project.core.validation import ensure_utc_timestamp
from project.features.funding_persistence import FP_DEF_VERSION
from project.engine.data_loader import dedupe_timestamp_rows

_CONTEXT_COLUMNS = [
    "fp_def_version",
    "fp_active",
    "fp_age_bars",
    "fp_event_id",
    "fp_enter_ts",
    "fp_exit_ts",
    "fp_severity",
    "fp_norm_due",
    "vol_regime",
    "vol_regime_code",
    "carry_state",
    "carry_state_code",
    "ms_vol_state",
    "ms_liq_state",
    "ms_oi_state",
    "ms_funding_state",
    "ms_trend_state",
    "ms_spread_state",
    "ms_context_state_code",
]


def load_context_data(
    data_root: Path, symbol: str, run_id: str, timeframe: str = "15m"
) -> pd.DataFrame:
    fp_candidates = [
        run_scoped_lake_path(data_root, run_id, "context", "funding_persistence", symbol),
        data_root / "features" / "context" / "funding_persistence" / symbol,
    ]
    fp_dir = choose_partition_dir(fp_candidates)
    fp_files = list_parquet_files(fp_dir) if fp_dir else []
    fp_df = read_parquet(fp_files) if fp_files else pd.DataFrame()

    ms_candidates = [
        run_scoped_lake_path(
            data_root, run_id, "features", "perp", symbol, timeframe, "market_context"
        ),
        data_root / "lake" / "features" / "perp" / symbol / timeframe / "market_context",
        run_scoped_lake_path(data_root, run_id, "context", "market_state", symbol, timeframe),
        data_root / "lake" / "context" / "market_state" / symbol / timeframe,
        run_scoped_lake_path(data_root, run_id, "context", "market_state", symbol),
        data_root / "lake" / "context" / "market_state" / symbol,
        data_root / "features" / "context" / "market_state" / symbol,
    ]
    ms_dir = choose_partition_dir(ms_candidates)
    ms_files = list_parquet_files(ms_dir) if ms_dir else []
    ms_df = read_parquet(ms_files) if ms_files else pd.DataFrame()

    if fp_df.empty and ms_df.empty:
        return pd.DataFrame(columns=["timestamp", *_CONTEXT_COLUMNS])

    for df in [fp_df, ms_df]:
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            ensure_utc_timestamp(df["timestamp"], "timestamp")

    if fp_df.empty:
        context = ms_df
    elif ms_df.empty:
        context = fp_df
    else:
        context = fp_df.merge(ms_df, on="timestamp", how="outer", suffixes=("", "_ms_dup"))
        dup_cols = [c for c in context.columns if c.endswith("_ms_dup")]
        if dup_cols:
            context = context.drop(columns=dup_cols)

    context, _ = dedupe_timestamp_rows(context, label=f"context:{symbol}:{timeframe}")

    if "fp_def_version" not in context.columns:
        context["fp_def_version"] = FP_DEF_VERSION

    for col in _CONTEXT_COLUMNS:
        if col not in context.columns:
            context[col] = np.nan

    for col in ["fp_active", "fp_age_bars", "fp_norm_due"]:
        if col in context.columns:
            context[col] = pd.to_numeric(context[col], errors="coerce").fillna(0).astype(int)

    return context[["timestamp", *_CONTEXT_COLUMNS]].sort_values("timestamp").reset_index(drop=True)


def apply_context_defaults(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "fp_def_version" not in out.columns:
        out["fp_def_version"] = FP_DEF_VERSION
    out["fp_def_version"] = out["fp_def_version"].fillna(FP_DEF_VERSION)

    for col, default in [
        ("fp_active", 0),
        ("fp_age_bars", 0),
        ("fp_norm_due", 0),
        ("fp_severity", 0.0),
        ("fp_event_id", None),
        ("fp_enter_ts", pd.NaT),
        ("fp_exit_ts", pd.NaT),
    ]:
        if col not in out.columns:
            out[col] = default

    for col in ["fp_active", "fp_age_bars", "fp_norm_due"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype(int)

    inactive = out["fp_active"] == 0
    out.loc[inactive, "fp_age_bars"] = 0
    out.loc[inactive, "fp_event_id"] = None
    out.loc[inactive, "fp_enter_ts"] = pd.NaT
    out.loc[inactive, "fp_exit_ts"] = pd.NaT
    out["fp_severity"] = (
        pd.to_numeric(out["fp_severity"], errors="coerce").fillna(0.0).astype(float)
    )
    out.loc[inactive, "fp_severity"] = 0.0
    return out


def merge_event_flags(features: pd.DataFrame, event_flags: pd.DataFrame | None) -> pd.DataFrame:
    if event_flags is None or event_flags.empty:
        return features
    out = features.copy()
    flags = event_flags.copy()
    flags["timestamp"] = pd.to_datetime(flags["timestamp"], utc=True, errors="coerce")
    flags = flags.dropna(subset=["timestamp"]).drop_duplicates(subset=["timestamp"], keep="last")
    flag_cols = [col for col in flags.columns if col not in {"timestamp", "symbol"}]
    if not flag_cols:
        return out
    merged = out.merge(flags[["timestamp", *flag_cols]], on="timestamp", how="left")
    for col in flag_cols:
        merged[col] = merged[col].astype("boolean").fillna(False).astype(bool)
    return merged


def merge_event_features(
    features: pd.DataFrame,
    event_features: pd.DataFrame | None,
    ffill_limit: int | Dict[str, int] = 12,
) -> pd.DataFrame:
    if event_features is None or event_features.empty:
        return features
    out = features.copy()
    ef = event_features.copy()
    ef["timestamp"] = pd.to_datetime(ef["timestamp"], utc=True, errors="coerce")
    ef = ef.dropna(subset=["timestamp"]).drop_duplicates(subset=["timestamp"], keep="last")
    merged = out.merge(ef, on="timestamp", how="left")
    event_cols = [c for c in ef.columns if c != "timestamp"]
    if not event_cols:
        return merged

    if isinstance(ffill_limit, dict):
        default_limit = int(ffill_limit.get("_default", ffill_limit.get("*", 0)))
        for col in event_cols:
            limit = int(ffill_limit.get(col, default_limit))
            if limit > 0:
                merged[col] = merged[col].ffill(limit=limit)
    elif int(ffill_limit) > 0:
        merged[event_cols] = merged[event_cols].ffill(limit=int(ffill_limit))
    return merged


def assemble_symbol_context(
    bars: pd.DataFrame,
    features: pd.DataFrame,
    data_root: Path,
    symbol: str,
    run_id: str,
    timeframe: str,
    start_ts: pd.Timestamp | None = None,
    end_ts: pd.Timestamp | None = None,
    event_flags: pd.DataFrame | None = None,
    event_features: pd.DataFrame | None = None,
    event_feature_ffill_bars: int | Dict[str, int] = 12,
    higher_timeframe_features: Dict[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    context = load_context_data(data_root, symbol, run_id=run_id, timeframe=timeframe)
    if start_ts is not None:
        context = context[context["timestamp"] >= start_ts].copy()
    if end_ts is not None:
        context = context[context["timestamp"] <= end_ts].copy()

    features = features.merge(context, on="timestamp", how="left", validate="one_to_one")
    features = apply_context_defaults(features)

    if isinstance(event_flags, pd.DataFrame):
        flags = event_flags.copy()
        if not flags.empty and "timestamp" in flags.columns:
            if start_ts is not None:
                flags = flags[flags["timestamp"] >= start_ts].copy()
            if end_ts is not None:
                flags = flags[flags["timestamp"] <= end_ts].copy()
            features = merge_event_flags(features, flags)

    if isinstance(event_features, pd.DataFrame):
        ef = event_features.copy()
        if not ef.empty and "timestamp" in ef.columns:
            if start_ts is not None:
                ef = ef[ef["timestamp"] >= start_ts].copy()
            if end_ts is not None:
                ef = ef[ef["timestamp"] <= end_ts].copy()
            features = merge_event_features(features, ef, ffill_limit=int(event_feature_ffill_bars))

    # Step 1: MTF Implementation - Join higher timeframe features
    if higher_timeframe_features:
        features = features.sort_values("timestamp")
        for tf, htf_df in higher_timeframe_features.items():
            if htf_df.empty:
                continue

            # Ensure columns are prefixed/suffixed to avoid collisions
            htf_df = htf_df.copy()
            htf_df["timestamp"] = pd.to_datetime(htf_df["timestamp"], utc=True)
            htf_df = htf_df.sort_values("timestamp")

            cols_to_map = [c for c in htf_df.columns if c != "timestamp"]
            htf_df = htf_df.rename(columns={c: f"{c}_{tf}" for c in cols_to_map})

            # Use backward merge_asof to prevent lookahead bias
            # This ensures that at time T, we only see features from higher timeframes
            # that were completed at or before time T.
            features = pd.merge_asof(
                features,
                htf_df,
                on="timestamp",
                direction="backward",
            )
            htf_cols = [f"{c}_{tf}" for c in cols_to_map]
            if htf_cols:
                features[htf_cols] = features[htf_cols].shift(1)

    return features
