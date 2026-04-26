from __future__ import annotations

import argparse
import json
import logging
import math
import re
import shutil
import sys
from collections.abc import Sequence
from pathlib import Path

import numpy as np
import pandas as pd

from project.core.config import get_data_root
from project.core.feature_quality import summarize_feature_quality
from project.core.feature_registry import ensure_core_feature_definitions_registered
from project.core.feature_schema import feature_dataset_dir_name, normalize_feature_schema_version
from project.core.logging_utils import build_stage_log_handlers
from project.core.timeframes import (
    bars_dataset_name,
    funding_dataset_name,
    normalize_timeframe,
    timeframe_to_minutes,
)
from project.core.validation import ts_ns_utc
from project.features.context_states import (
    calculate_ms_oi_probabilities,
)
from project.features.microstructure import (
    calculate_imbalance,
)
from project.io.utils import (
    choose_partition_dir,
    ensure_dir,
    lake_cache_key,
    list_parquet_files,
    raw_dataset_dir_candidates,
    read_parquet,
    run_scoped_lake_path,
    write_cache_key,
    write_parquet,
)
from project.specs.manifest import finalize_manifest, start_manifest

_FUNDING_MAX_STALENESS_H = 8
_OI_MAX_STALENESS_H = 4
_ZSCORE_WINDOW = 96
_BASE_WINDOW_MINUTES = (
    5  # legacy 5m semantic baseline, converted to active timeframe via _duration_to_bars
)
_WARMUP_COL_PATTERN = re.compile(r"_\d+$")
_PARTITION_YEAR_RE = re.compile(r"year=(\d{4})")
_PARTITION_MONTH_RE = re.compile(r"month=(\d{2})")


def _feature_quality_report_path(
    data_root: Path,
    *,
    run_id: str,
    market: str,
    symbol: str,
    timeframe: str,
    feature_schema_version: str,
) -> Path:
    return (
        data_root
        / "reports"
        / "feature_quality"
        / run_id
        / market
        / symbol
        / timeframe
        / f"feature_quality_{feature_schema_version}.json"
    )


def _write_feature_quality_report(path: Path, payload: dict[str, object]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _written_path(write_result: object, requested_path: Path) -> Path:
    if isinstance(write_result, tuple) and write_result:
        candidate = write_result[0]
        if isinstance(candidate, Path):
            return candidate
    if isinstance(write_result, Path):
        return write_result
    return requested_path


def _resolve_raw_dir(
    data_root: Path,
    *,
    market: str,
    symbol: str,
    dataset: str,
    run_id: str | None = None,
    aliases: tuple[str, ...] = (),
) -> Path | None:
    datasets = [str(dataset).strip(), *[str(alias).strip() for alias in aliases if str(alias).strip()]]
    candidates = raw_dataset_dir_candidates(
        data_root,
        market=market,
        symbol=symbol,
        dataset=dataset,
        run_id=run_id,
        aliases=aliases,
    )
    roots: list[Path] = []
    if run_id:
        roots.append(run_scoped_lake_path(data_root, run_id, "raw"))
    roots.append(Path(data_root) / "lake" / "raw")
    seen = {str(path) for path in candidates}
    for root in roots:
        if not root.exists() or not root.is_dir():
            continue
        for venue_dir in sorted(path for path in root.iterdir() if path.is_dir()):
            for dataset_name in datasets:
                candidate = venue_dir / market / symbol / dataset_name
                key = str(candidate)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(candidate)
    return choose_partition_dir(candidates) or (candidates[0] if candidates else None)


def _load_baseline_features(
    *,
    data_root: Path,
    run_id: str,
    market: str,
    symbol: str,
    timeframe: str,
    feature_schema_version: str,
) -> pd.DataFrame:
    candidates = [
        run_scoped_lake_path(
            data_root,
            run_id,
            "features",
            market,
            symbol,
            timeframe,
            feature_dataset_dir_name(feature_schema_version),
        ),
        data_root
        / "lake"
        / "runs"
        / run_id
        / "features"
        / market
        / symbol
        / timeframe
        / feature_dataset_dir_name(feature_schema_version),
    ]
    path_dir = choose_partition_dir(candidates)
    if not path_dir:
        return pd.DataFrame()
    files = list_parquet_files(path_dir)
    if not files:
        return pd.DataFrame()
    return read_parquet(files)


def _rolling_percentile(series: pd.Series, window: int = 96) -> pd.Series:
    # Use pandas native rolling rank for significant performance speedup
    return series.rolling(window=window, min_periods=min(window, 8)).rank(pct=True) * 100.0


def _duration_to_bars(*, minutes: int, timeframe: str, min_bars: int = 1) -> int:
    tf = normalize_timeframe(timeframe)
    tf_minutes = timeframe_to_minutes(tf)
    return max(min_bars, int(math.ceil(minutes / tf_minutes)))


def _revision_lag_minutes(n: int, timeframe: str = "5m") -> int:
    """Return lag in minutes for n bars of the active timeframe."""
    tf_minutes = timeframe_to_minutes(normalize_timeframe(timeframe))
    return n * tf_minutes


def _safe_logret_1(close: pd.Series) -> pd.Series:
    """Log return vs prior bar. NaN when either close <= 0."""
    prev = close.shift(1)
    valid = (close > 0) & (prev > 0)
    out = pd.Series(np.nan, index=close.index)
    out.loc[valid] = np.log(close.loc[valid] / prev.loc[valid])
    return out


def _load_spot_close_reference(
    symbol: str,
    run_id: str,
    data_root: Path,
    timeframe: str = "5m",
) -> pd.DataFrame:
    bars_dataset = bars_dataset_name(timeframe)
    # Use consistent lake path logic - try cleaned first, then raw spot
    candidates = [
        run_scoped_lake_path(data_root, run_id, "cleaned", "spot", symbol, bars_dataset),
        data_root / "lake" / "cleaned" / "spot" / symbol / bars_dataset,
    ]
    path_dir = choose_partition_dir(candidates)
    if path_dir:
        try:
            files = list_parquet_files(path_dir)
            if files:
                df = read_parquet(files)
                if "timestamp" in df.columns and "close" in df.columns:
                    return df[["timestamp", "close"]].rename(columns={"close": "spot_close"})
        except Exception:
            pass

    # Fallback: try loading from raw spot OHLCV data
    raw_candidates = [
        data_root / "lake" / "runs" / run_id / "raw" / "binance" / "spot" / symbol / ("ohlcv_" + timeframe),
        data_root / "lake" / "raw" / "binance" / "spot" / symbol / ("ohlcv_" + timeframe),
    ]
    raw_dir = choose_partition_dir(raw_candidates)
    if raw_dir:
        try:
            files = list_parquet_files(raw_dir)
            if files:
                df = read_parquet(files)
                if "timestamp" in df.columns and "close" in df.columns:
                    return df[["timestamp", "close"]].rename(columns={"close": "spot_close"})
        except Exception:
            pass

    return pd.DataFrame(columns=["timestamp", "spot_close"])


def _add_basis_features(
    frame: pd.DataFrame,
    symbol: str,
    run_id: str,
    market: str,
    data_root: Path,
    timeframe: str = "5m",
) -> pd.DataFrame:
    out = frame.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)
    spot = _load_spot_close_reference(symbol, run_id, data_root, timeframe=timeframe)

    if not spot.empty and "timestamp" in spot.columns and "spot_close" in spot.columns:
        spot = spot.copy()
        spot["timestamp"] = pd.to_datetime(spot["timestamp"], utc=True)
        # Exact timestamp merge only — no asof
        merged = out.merge(spot[["timestamp", "spot_close"]], on="timestamp", how="left")
        out["spot_close"] = merged["spot_close"].values
        has_spot = merged["spot_close"].notna()
        out["basis_spot_coverage"] = float(has_spot.sum()) / max(len(out), 1)
        valid = has_spot & (merged["spot_close"] > 0) & (merged["close"] > 0)
        out["basis_bps"] = np.nan
        out.loc[valid.values, "basis_bps"] = (
            (merged.loc[valid, "close"] / merged.loc[valid, "spot_close"] - 1.0) * 10_000.0
        ).values
        # PIT safety: lag basis_bps by 1 bar so downstream consumers only see prior-bar values
        out["basis_bps"] = out["basis_bps"].shift(1)
    else:
        out["basis_bps"] = np.nan
        out["basis_spot_coverage"] = 0.0
        out["spot_close"] = np.nan

    zscore_window = _duration_to_bars(
        minutes=_ZSCORE_WINDOW * _BASE_WINDOW_MINUTES,
        timeframe=timeframe,
        min_bars=2,
    )

    # SF-001: Replace Gaussian standard deviation with robust median absolute deviation (MAD)
    # or direct quantile standardization to handle fat-tailed distributions smoothly.
    roll_median = out["basis_bps"].rolling(zscore_window, min_periods=2).median()
    roll_mad = (out["basis_bps"] - roll_median).abs().rolling(zscore_window, min_periods=2).median()
    # Approx convert MAD to std equivalent (1.4826) to maintain existing signal scales
    roll_robust_std = roll_mad * 1.4826
    out["basis_zscore"] = (out["basis_bps"] - roll_median) / roll_robust_std.replace(0.0, np.nan)

    if "spread_bps" in out.columns:
        sm = out["spread_bps"].rolling(zscore_window, min_periods=2).median()
        # Same robust logic for spread to prevent extreme outliers skewing the denominator
        s_mad = (out["spread_bps"] - sm).abs().rolling(zscore_window, min_periods=2).median()
        s_robust_std = s_mad * 1.4826
        out["spread_zscore"] = (out["spread_bps"] - sm) / s_robust_std.replace(0.0, np.nan)
    else:
        out["spread_zscore"] = np.nan

    return out


def _merge_funding_rates(
    bars: pd.DataFrame,
    funding: pd.DataFrame,
    symbol: str,
    timeframe: str = "5m",
) -> pd.DataFrame:
    bars_out = bars.copy()
    existing_scaled = pd.to_numeric(
        bars_out.get("funding_rate_scaled", pd.Series(np.nan, index=bars_out.index)),
        errors="coerce",
    )
    existing_feature = pd.to_numeric(
        bars_out.get("funding_rate_feature", pd.Series(np.nan, index=bars_out.index)),
        errors="coerce",
    )
    if existing_scaled.notna().any() or existing_feature.notna().any():
        bars_out["funding_rate_scaled"] = existing_scaled.where(
            existing_scaled.notna(), existing_feature
        )
        if "funding_rate_feature" not in bars_out.columns:
            bars_out["funding_rate_feature"] = existing_feature
        return bars_out

    if funding.empty or "timestamp" not in funding.columns:
        if "funding_rate_scaled" not in bars_out.columns:
            bars_out["funding_rate_scaled"] = np.nan
        if "funding_rate_feature" not in bars_out.columns:
            bars_out["funding_rate_feature"] = np.nan
        return bars_out

    funding = funding.copy()
    funding["timestamp"] = ts_ns_utc(
        pd.to_datetime(funding["timestamp"], utc=True, errors="coerce")
    )
    if funding["timestamp"].duplicated().any():
        funding = funding.drop_duplicates(subset=["timestamp"], keep="last")
    funding = funding.sort_values("timestamp").reset_index(drop=True)

    if "funding_rate_scaled" not in funding.columns and "funding_rate" in funding.columns:
        funding["funding_rate_scaled"] = funding["funding_rate"].astype(float)
    if "funding_rate_feature" not in funding.columns and "funding_rate_scaled" in funding.columns:
        funding["funding_rate_feature"] = funding["funding_rate_scaled"]

    bars_out["timestamp"] = ts_ns_utc(
        pd.to_datetime(bars_out["timestamp"], utc=True, errors="coerce")
    )
    bars_out = bars_out.sort_values("timestamp").reset_index(drop=True)

    if "funding_rate_scaled" in funding.columns or "funding_rate_feature" in funding.columns:
        merge_cols = ["timestamp"]
        if "funding_rate_scaled" in funding.columns:
            merge_cols.append("funding_rate_scaled")
        if "funding_rate_feature" in funding.columns:
            merge_cols.append("funding_rate_feature")
        merged = pd.merge_asof(
            bars_out,
            funding[merge_cols],
            on="timestamp",
            direction="backward",
        )

        staleness_bars = _duration_to_bars(
            minutes=_FUNDING_MAX_STALENESS_H * 60,
            timeframe=timeframe,
        )
        staleness_limit_ns = int(
            pd.Timedelta(
                minutes=staleness_bars * timeframe_to_minutes(normalize_timeframe(timeframe))
            ).value
        )
        funding_ts_ns = funding["timestamp"].values.astype(np.int64)
        bar_ts_ns = merged["timestamp"].values.astype(np.int64)
        idx = np.searchsorted(funding_ts_ns, bar_ts_ns, side="right") - 1
        valid = idx >= 0
        stale = np.ones(len(merged), dtype=bool)
        if valid.any():
            src_ts = np.where(valid, funding_ts_ns[np.maximum(0, idx)], np.iinfo(np.int64).min)
            stale = (~valid) | ((bar_ts_ns - src_ts) > staleness_limit_ns)
        if "funding_rate_scaled" in merged.columns:
            merged.loc[stale, "funding_rate_scaled"] = np.nan
        if "funding_rate_feature" in merged.columns:
            merged.loc[stale, "funding_rate_feature"] = np.nan
        return merged
    else:
        if "funding_rate_scaled" not in bars_out.columns:
            bars_out["funding_rate_scaled"] = np.nan
        if "funding_rate_feature" not in bars_out.columns:
            bars_out["funding_rate_feature"] = np.nan
        return bars_out


def _merge_optional_microstructure_inputs(
    bars: pd.DataFrame,
    *,
    symbol: str,
    market: str,
    run_id: str,
    data_root: Path,
    timeframe: str = "5m",
) -> pd.DataFrame:
    out = bars.copy()
    out["timestamp"] = ts_ns_utc(pd.to_datetime(out["timestamp"], utc=True, errors="coerce"))

    tob = pd.DataFrame()
    if market == "perp":
        tob_paths = [
            run_scoped_lake_path(data_root, run_id, "cleaned", "perp", symbol, "tob_5m_agg"),
            data_root / "lake" / "cleaned" / "perp" / symbol / "tob_5m_agg",
        ]
        tob_dir = choose_partition_dir(tob_paths)
        tob = read_parquet(list_parquet_files(tob_dir)) if tob_dir else pd.DataFrame()

    if not tob.empty and "timestamp" in tob.columns:
        tob = tob.copy()
        tob["timestamp"] = ts_ns_utc(pd.to_datetime(tob["timestamp"], utc=True, errors="coerce"))
        tob = tob.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last")
        rename_map = {
            "spread_bps_mean": "spread_bps",
            "bid_depth_usd_mean": "bid_depth_usd",
            "ask_depth_usd_mean": "ask_depth_usd",
            "imbalance_mean": "imbalance",
            "valid_snapshot_mean": "tob_coverage",
        }
        available_cols = ["timestamp"] + [c for c in rename_map if c in tob.columns]
        tob_view = tob[available_cols].rename(columns=rename_map)
        out = pd.merge_asof(
            out.sort_values("timestamp"),
            tob_view.sort_values("timestamp"),
            on="timestamp",
            direction="backward",
        )

    volume = pd.to_numeric(out.get("volume", pd.Series(np.nan, index=out.index)), errors="coerce")
    close = pd.to_numeric(out.get("close", pd.Series(np.nan, index=out.index)), errors="coerce")
    high = pd.to_numeric(out.get("high", pd.Series(np.nan, index=out.index)), errors="coerce")
    low = pd.to_numeric(out.get("low", pd.Series(np.nan, index=out.index)), errors="coerce")

    quote_volume = pd.to_numeric(
        out.get("quote_volume", pd.Series(np.nan, index=out.index)), errors="coerce"
    )
    quote_volume = quote_volume.where(quote_volume.notna(), volume * close)
    out["quote_volume"] = quote_volume

    if "spread_bps" not in out.columns or pd.to_numeric(
        out["spread_bps"], errors="coerce"
    ).isna().all():
        spread_proxy = ((high - low) / close.replace(0.0, np.nan)).abs() * 10_000.0
        out["spread_bps"] = spread_proxy.shift(1)
    else:
        out["spread_bps"] = pd.to_numeric(out["spread_bps"], errors="coerce")

    if "imbalance" not in out.columns or pd.to_numeric(
        out["imbalance"], errors="coerce"
    ).isna().all():
        if "ms_imbalance_24" in out.columns:
            out["imbalance"] = pd.to_numeric(out["ms_imbalance_24"], errors="coerce").fillna(0.0)
        else:
            out["imbalance"] = 0.0
    else:
        out["imbalance"] = pd.to_numeric(out["imbalance"], errors="coerce").fillna(0.0)

    if "depth_usd" not in out.columns or pd.to_numeric(
        out["depth_usd"], errors="coerce"
    ).isna().all():
        out["depth_usd"] = quote_volume.shift(1)
    else:
        out["depth_usd"] = pd.to_numeric(out["depth_usd"], errors="coerce")

    if "bid_depth_usd" not in out.columns or "ask_depth_usd" not in out.columns:
        depth = pd.to_numeric(out["depth_usd"], errors="coerce")
        imbalance = pd.to_numeric(out["imbalance"], errors="coerce").clip(-1.0, 1.0)
        bid_share = ((imbalance + 1.0) / 2.0).fillna(0.5)
        out["bid_depth_usd"] = depth * bid_share
        out["ask_depth_usd"] = depth * (1.0 - bid_share)
    else:
        out["bid_depth_usd"] = pd.to_numeric(out["bid_depth_usd"], errors="coerce")
        out["ask_depth_usd"] = pd.to_numeric(out["ask_depth_usd"], errors="coerce")

    if "micro_depth_depletion" not in out.columns or pd.to_numeric(
        out["micro_depth_depletion"], errors="coerce"
    ).isna().all():
        depth = pd.to_numeric(out["depth_usd"], errors="coerce")
        depth_baseline = depth.rolling(24, min_periods=1).mean().shift(1)
        out["micro_depth_depletion"] = (
            1.0 - (depth / depth_baseline.replace(0.0, np.nan))
        ).fillna(0.0)
    else:
        out["micro_depth_depletion"] = pd.to_numeric(
            out["micro_depth_depletion"], errors="coerce"
        ).fillna(0.0)

    if "tob_coverage" in out.columns:
        out["tob_coverage"] = pd.to_numeric(out["tob_coverage"], errors="coerce").fillna(0.0)

    return out


def _merge_optional_oi_liquidation(
    bars: pd.DataFrame,
    symbol: str,
    market: str,
    run_id: str,
    data_root: Path,
    timeframe: str = "5m",
) -> pd.DataFrame:
    out = bars.copy()
    out["timestamp"] = ts_ns_utc(pd.to_datetime(out["timestamp"], utc=True, errors="coerce"))

    # Use consistent lake paths
    oi_dataset = "open_interest"
    liq_dataset = "liquidations"

    # Open interest
    oi_period = "5m"
    oi_dir = _resolve_raw_dir(
        data_root,
        market=market,
        symbol=symbol,
        dataset=oi_dataset,
        run_id=run_id,
        aliases=(oi_period,),
    )
    oi = read_parquet(list_parquet_files(oi_dir)) if oi_dir else pd.DataFrame()

    out["oi_notional"] = np.nan
    if not oi.empty and ("timestamp" in oi.columns or "time" in oi.columns):
        oi = oi.copy()
        oi_ts_col = "timestamp" if "timestamp" in oi.columns else "time"
        oi["timestamp"] = ts_ns_utc(pd.to_datetime(oi[oi_ts_col], utc=True, errors="coerce"))
        if oi["timestamp"].duplicated().any():
            oi = oi.drop_duplicates(subset=["timestamp"], keep="last")
        oi = oi.sort_values("timestamp").reset_index(drop=True)

        oi_val_col = next(
            (c for c in ["sum_open_interest", "open_interest", "oi"] if c in oi.columns),
            None,
        )
        if oi_val_col is None:
            oi = pd.DataFrame()

        if not oi.empty:
            merged_oi = pd.merge_asof(
                out, oi[["timestamp", oi_val_col]], on="timestamp", direction="backward"
            )
            oi_ts_ns = oi["timestamp"].values.astype(np.int64)
            bar_ts_ns = merged_oi["timestamp"].values.astype(np.int64)
            idx = np.searchsorted(oi_ts_ns, bar_ts_ns, side="right") - 1
            valid = idx >= 0
            stale_bars = _duration_to_bars(
                minutes=_OI_MAX_STALENESS_H * 60,
                timeframe=timeframe,
            )
            stale_limit_ns = int(
                pd.Timedelta(
                    minutes=stale_bars * timeframe_to_minutes(normalize_timeframe(timeframe))
                ).value
            )
            stale = np.ones(len(merged_oi), dtype=bool)
            if valid.any():
                src_ts = np.where(valid, oi_ts_ns[np.maximum(0, idx)], np.iinfo(np.int64).min)
                stale = (~valid) | ((bar_ts_ns - src_ts) > stale_limit_ns)
            out["oi_notional"] = merged_oi[oi_val_col].values.astype(float)
            out.loc[stale, "oi_notional"] = np.nan

    # Liquidations
    liq_dir = _resolve_raw_dir(
        data_root,
        market=market,
        symbol=symbol,
        dataset=liq_dataset,
        run_id=run_id,
        aliases=("liquidation_snapshot",),
    )
    liq = read_parquet(list_parquet_files(liq_dir)) if liq_dir else pd.DataFrame()

    out["liquidation_notional"] = 0.0
    out["liquidation_count"] = 0.0

    if not liq.empty and ("timestamp" in liq.columns or "time" in liq.columns):
        # standard liq columns: notional, notional_usd or amount * price
        liq_ts_col = "timestamp" if "timestamp" in liq.columns else "time"
        liq_notional_col = "notional"
        if "notional" not in liq.columns:
            if "notional_usd" in liq.columns:
                liq_notional_col = "notional_usd"
            elif "amount" in liq.columns:
                liq_notional_col = "amount"
            else:
                liq = pd.DataFrame()

        if not liq.empty:
            liq = liq.copy()
            liq["timestamp"] = ts_ns_utc(pd.to_datetime(liq[liq_ts_col], utc=True, errors="coerce"))
            bar_ts_ns = out["timestamp"].values.astype(np.int64)
            liq_ts_ns = liq["timestamp"].values.astype(np.int64)
            bar_width_ns = int(
                pd.Timedelta(minutes=timeframe_to_minutes(normalize_timeframe(timeframe))).value
            )

            idx = np.searchsorted(bar_ts_ns, liq_ts_ns, side="right") - 1
            in_window = (idx >= 0) & (idx < len(out))
            # Ensure it falls WITHIN the bar (from bar_start to bar_start + width)
            in_window[in_window] &= liq_ts_ns[in_window] < (bar_ts_ns[idx[in_window]] + bar_width_ns)

            if in_window.any():
                liq_notional = np.zeros(len(out))
                liq_count = np.zeros(len(out))
                liq_vals = (
                    pd.to_numeric(liq[liq_notional_col], errors="coerce")
                    .fillna(0.0)
                    .to_numpy(dtype=float)
                )
                np.add.at(liq_notional, idx[in_window], liq_vals[in_window])
                np.add.at(liq_count, idx[in_window], 1)
                out["liquidation_notional"] = liq_notional
                out["liquidation_count"] = liq_count

    return out


def _ensure_feature_contract_columns(frame: pd.DataFrame, *, timeframe: str) -> pd.DataFrame:
    out = frame.copy()
    volume = pd.to_numeric(out.get("volume", pd.Series(np.nan, index=out.index)), errors="coerce")
    close = pd.to_numeric(out.get("close", pd.Series(np.nan, index=out.index)), errors="coerce")

    if "quote_volume" not in out.columns:
        out["quote_volume"] = volume * close
    else:
        quote_volume = pd.to_numeric(out["quote_volume"], errors="coerce")
        out["quote_volume"] = quote_volume.where(quote_volume.notna(), volume * close)

    if "taker_base_volume" not in out.columns:
        out["taker_base_volume"] = (volume / 2.0).fillna(0.0)
    else:
        out["taker_base_volume"] = pd.to_numeric(out["taker_base_volume"], errors="coerce").fillna(
            0.0
        )

    funding_scaled = pd.to_numeric(
        out.get("funding_rate_scaled", pd.Series(np.nan, index=out.index)), errors="coerce"
    )
    out["funding_rate_scaled"] = funding_scaled
    if "funding_rate" not in out.columns:
        out["funding_rate"] = funding_scaled
    else:
        funding_rate = pd.to_numeric(out["funding_rate"], errors="coerce")
        out["funding_rate"] = funding_rate.where(funding_rate.notna(), funding_scaled)

    if "funding_rate_realized" not in out.columns:
        out["funding_rate_realized"] = 0.0
    else:
        out["funding_rate_realized"] = pd.to_numeric(
            out["funding_rate_realized"], errors="coerce"
        ).fillna(0.0)

    if "is_gap" not in out.columns:
        out["is_gap"] = False
    out["is_gap"] = out["is_gap"].astype(bool)

    if "cross_exchange_spread_z" not in out.columns:
        out["cross_exchange_spread_z"] = pd.to_numeric(
            out.get("basis_zscore", pd.Series(np.nan, index=out.index)),
            errors="coerce",
        )

    if "revision_lag_bars" not in out.columns:
        out["revision_lag_bars"] = 0
    out["revision_lag_bars"] = (
        pd.to_numeric(out["revision_lag_bars"], errors="coerce").fillna(0).astype(int)
    )
    out["revision_lag_minutes"] = out["revision_lag_bars"].map(
        lambda n: _revision_lag_minutes(int(n), timeframe=timeframe)
    )

    buy_volume = (
        pd.to_numeric(out["taker_base_volume"], errors="coerce").fillna(0.0).clip(lower=0.0)
    )
    total_volume = volume.fillna(0.0).clip(lower=0.0)
    buy_volume = pd.Series(
        np.minimum(buy_volume.to_numpy(), total_volume.to_numpy()), index=out.index
    )
    sell_volume = pd.Series(
        np.maximum(total_volume.to_numpy() - buy_volume.to_numpy(), 0.0), index=out.index
    )

    out["ms_imbalance_24"] = calculate_imbalance(buy_volume, sell_volume, window=24).shift(1)
    if "imbalance" not in out.columns:
        out["imbalance"] = out["ms_imbalance_24"]

    # PIT safety verification: ensure key indicators that should be lagged are indeed shifted.
    # This is a defensive check to prevent look-ahead bias during feature evolution.
    _PIT_LAGGED_FEATURES = {
        "rv_96",
        "rv_pct_17280",
        "funding_abs",
        "funding_abs_pct",
        "basis_bps",
        "basis_zscore",
        "ms_imbalance_24",
        "oi_delta_1h",
        "range_med_2880",
        "spread_zscore",
        "cross_exchange_spread_z",
    }
    for feat in _PIT_LAGGED_FEATURES:
        if feat in out.columns and len(out) > 5:
            # Audit Pattern B: Heuristic check — a lagged rolling indicator MUST
            # start with at least one NaN if correctly shifted.
            # We allow 0.0 because many indicators use .fillna(0.0) after .shift(1)
            first_val = out[feat].iloc[0]
            if pd.notna(first_val) and first_val != 0.0:
                logging.warning(
                    f"PIT Violation Risk: Feature '{feat}' is not NaN at index 0. "
                    "It may be missing a .shift(1) lag."
                )

    return out


def build_features(
    bars: pd.DataFrame,
    funding: pd.DataFrame,
    symbol: str,
    run_id: str = "",
    data_root: Path | None = None,
    market: str = "perp",
    timeframe: str = "5m",
) -> pd.DataFrame:
    """Build the canonical feature set: merge funding, add basis/spread/OI/liquidation features."""
    ensure_core_feature_definitions_registered()
    if data_root is None:
        from project.core.config import get_data_root

        data_root = get_data_root()

    tf = normalize_timeframe(timeframe)

    out = _merge_funding_rates(bars, funding, symbol, timeframe=tf)
    out = _merge_optional_microstructure_inputs(
        out,
        symbol=symbol,
        market=market,
        run_id=run_id,
        data_root=data_root,
        timeframe=tf,
    )
    out = _add_basis_features(
        out,
        symbol=symbol,
        run_id=run_id,
        market=market,
        data_root=data_root,
        timeframe=tf,
    )
    out = _merge_optional_oi_liquidation(
        out,
        symbol=symbol,
        market=market,
        run_id=run_id,
        data_root=data_root,
        timeframe=tf,
    )

    # Add indicators (All lagged by 1 bar to ensure PIT-safety)
    out["logret_1"] = _safe_logret_1(out["close"])
    rv_window = _duration_to_bars(minutes=96 * _BASE_WINDOW_MINUTES, timeframe=tf, min_bars=2)
    rv_min_periods = _duration_to_bars(minutes=8 * _BASE_WINDOW_MINUTES, timeframe=tf, min_bars=2)
    rv_pct_window = _duration_to_bars(
        minutes=17280 * _BASE_WINDOW_MINUTES, timeframe=tf, min_bars=1
    )
    range_med_window = _duration_to_bars(
        minutes=2880 * _BASE_WINDOW_MINUTES, timeframe=tf, min_bars=1
    )
    range_med_min_periods = _duration_to_bars(
        minutes=288 * _BASE_WINDOW_MINUTES, timeframe=tf, min_bars=1
    )

    out["rv_96"] = out["logret_1"].rolling(rv_window, min_periods=rv_min_periods).std().shift(1)
    out["rv_pct_17280"] = _rolling_percentile(out["rv_96"], window=rv_pct_window).shift(1)
    out["high_96"] = out["high"].rolling(rv_window, min_periods=1).max().shift(1)
    out["low_96"] = out["low"].rolling(rv_window, min_periods=1).min().shift(1)
    out["range_96"] = (out["high_96"] / out["low_96"].replace(0.0, np.nan) - 1.0).fillna(0.0)
    out["range_med_2880"] = (
        out["range_96"]
        .rolling(range_med_window, min_periods=range_med_min_periods)
        .median()
        .shift(1)
        .fillna(0.0)
    )

    if "oi_notional" in out.columns:
        oi_delta_window = _duration_to_bars(minutes=60, timeframe=tf, min_bars=1)
        out["oi_delta_1h"] = out["oi_notional"].diff(oi_delta_window).shift(1).fillna(0.0)
        oi_probs = calculate_ms_oi_probabilities(out["oi_delta_1h"])
        # Concat the specific columns we need for guards
        oi_cols = ["ms_oi_state", "ms_oi_confidence", "ms_oi_entropy"]
        out = pd.concat([out, oi_probs[oi_cols]], axis=1)
    else:
        out["oi_delta_1h"] = 0.0
        out["ms_oi_state"] = 1.0
        out["ms_oi_confidence"] = 1.0
        out["ms_oi_entropy"] = 0.0

    # Normalize funding magnitude inputs before deriving downstream features.
    funding_scaled = pd.to_numeric(
        out.get("funding_rate_scaled", pd.Series(np.nan, index=out.index)),
        errors="coerce",
    )
    if "funding_rate_feature" in out.columns:
        funding_feature = pd.to_numeric(out["funding_rate_feature"], errors="coerce")
        funding_scaled = funding_scaled.where(funding_scaled.notna(), funding_feature)
    out["funding_rate_scaled"] = funding_scaled

    # Add funding absolute and percentile features
    if out["funding_rate_scaled"].notna().any():
        funding_abs = out["funding_rate_scaled"].abs().fillna(0.0)
        out["funding_abs"] = funding_abs.shift(1)  # Lag raw magnitude too if used for thresholds
        funding_abs_window = _duration_to_bars(
            minutes=96 * _BASE_WINDOW_MINUTES,
            timeframe=tf,
            min_bars=1,
        )
        out["funding_abs_pct"] = (
            _rolling_percentile(funding_abs, window=funding_abs_window).shift(1).fillna(0.0)
        )
    else:
        out["funding_abs"] = 0.0
        out["funding_abs_pct"] = 0.0

    return _ensure_feature_contract_columns(out, timeframe=tf)


def _filter_time_window(
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


def _resolve_window_bounds(
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


def _partition_month_key(path: Path) -> tuple[int, int] | None:
    text = str(path)
    year_match = _PARTITION_YEAR_RE.search(text)
    month_match = _PARTITION_MONTH_RE.search(text)
    if not year_match or not month_match:
        return None
    return int(year_match.group(1)), int(month_match.group(1))


def _prune_partition_files_by_window(
    files: Sequence[Path],
    *,
    start: str | None,
    end: str | None,
) -> list[Path]:
    start_ts, end_ts = _resolve_window_bounds(start, end)
    if (start_ts is None and end_ts is None) or not files:
        return list(files)

    def _month_floor(ts: pd.Timestamp) -> tuple[int, int]:
        return ts.year, ts.month

    min_month = _month_floor(start_ts) if start_ts is not None else None
    max_month = _month_floor(end_ts) if end_ts is not None else None
    pruned: list[Path] = []
    for file_path in files:
        month_key = _partition_month_key(file_path)
        if month_key is None:
            pruned.append(file_path)
            continue
        if min_month is not None and month_key < min_month:
            continue
        if max_month is not None and month_key > max_month:
            continue
        pruned.append(file_path)
    return pruned or list(files)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build canonical features.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--market", default="perp")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--feature_schema_version", default="v2")
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--log_path", default=None)
    parser.add_argument("--baseline_run_id", default=None)
    parser.add_argument("--skip_if_exists", type=int, default=0)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    market = args.market
    tf = normalize_timeframe(args.timeframe)
    feature_schema_version = normalize_feature_schema_version(args.feature_schema_version)
    args.timeframe = tf
    args.feature_schema_version = feature_schema_version
    baseline_run_id = (
        str(args.baseline_run_id).strip()
        if args.baseline_run_id is not None and str(args.baseline_run_id).strip().lower() != "none"
        else ""
    )


    data_root = get_data_root()

    log_handlers = build_stage_log_handlers(args.log_path)
    logging.basicConfig(
        level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s"
    )

    params = vars(args)
    manifest = start_manifest(
        f"build_features_{tf}" + ("_spot" if market == "spot" else ""), run_id, params, [], []
    )
    stats: dict[str, object] = {"symbols": {}}
    outputs: list[dict[str, object]] = []

    try:
        for symbol in symbols:
            if args.skip_if_exists:
                report_path = _feature_quality_report_path(
                    data_root,
                    run_id=run_id,
                    market=market,
                    symbol=symbol,
                    timeframe=tf,
                    feature_schema_version=feature_schema_version,
                    )
                if report_path.exists():
                    logging.info(f"Skipping {symbol} {tf} as quality report already exists: {report_path}")
                    existing_symbol_root = run_scoped_lake_path(
                        data_root,
                        run_id,
                        "features",
                        market,
                        symbol,
                        tf,
                        feature_dataset_dir_name(feature_schema_version),
                    )
                    if existing_symbol_root.exists():
                        for existing_path in list_parquet_files(existing_symbol_root):
                            outputs.append({"path": str(existing_path), "rows": 0})
                    outputs.append({"path": str(report_path), "rows": 1})
                    stats["symbols"][symbol] = {
                        "rows": 0,
                        "feature_quality_report_path": str(report_path),
                        "feature_quality_summary": {},
                        "skipped_existing": True,
                    }
                    continue

            # Load cleaned bars
            bars_paths = [
                run_scoped_lake_path(
                    data_root, run_id, "cleaned", market, symbol, bars_dataset_name(tf)
                ),
                data_root / "lake" / "cleaned" / market / symbol / bars_dataset_name(tf),
            ]
            bars_dir = choose_partition_dir(bars_paths)
            if not bars_dir:
                logging.warning(f"No cleaned bars found for {symbol} {tf}")
                continue
            bars_files = _prune_partition_files_by_window(
                list_parquet_files(bars_dir),
                start=args.start,
                end=args.end,
            )
            bars = read_parquet(bars_files)
            bars = _filter_time_window(bars, start=args.start, end=args.end)
            if bars.empty:
                logging.warning(
                    f"No cleaned bars remain for {symbol} {tf} after start/end filtering"
                )
                continue

            # Load funding (only for perp)
            funding = pd.DataFrame()
            if market == "perp":
                funding_dir = _resolve_raw_dir(
                    data_root,
                    market="perp",
                    symbol=symbol,
                    dataset=funding_dataset_name(tf),
                    run_id=run_id,
                    aliases=("funding",),
                )
                if funding_dir:
                    funding = read_parquet(
                        _prune_partition_files_by_window(
                            list_parquet_files(funding_dir),
                            start=args.start,
                            end=args.end,
                        )
                    )
                    funding = _filter_time_window(funding, start=args.start, end=args.end)

            # Build features
            out = build_features(
                bars,
                funding,
                symbol,
                run_id=run_id,
                data_root=data_root,
                market=market,
                timeframe=tf,
            )

            if not out.empty:
                out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)
                out["symbol"] = symbol
                baseline_features = pd.DataFrame()
                if baseline_run_id:
                    baseline_features = _load_baseline_features(
                        data_root=data_root,
                        run_id=baseline_run_id,
                        market=market,
                        symbol=symbol,
                        timeframe=tf,
                        feature_schema_version=feature_schema_version,
                    )

                # Write to lake (partitioned by year/month)
                out_root = run_scoped_lake_path(
                    data_root,
                    run_id,
                    "features",
                    market,
                    symbol,
                    tf,
                    feature_dataset_dir_name(feature_schema_version),
                )
                for (year, month), group in out.groupby(
                    [out["timestamp"].dt.year, out["timestamp"].dt.month]
                ):
                    year, month = int(year), int(month)
                    out_dir = out_root / f"year={year}" / f"month={month:02d}"
                    out_path = (
                        out_dir
                        / f"features_{symbol}_{feature_schema_version}_{year}-{month:02d}.parquet"
                    )
                    shared_path = (
                        data_root / "lake" / "features" / market / symbol / tf
                        / feature_dataset_dir_name(feature_schema_version)
                        / f"year={year}" / f"month={month:02d}"
                        / f"features_{symbol}_{feature_schema_version}_{year}-{month:02d}.parquet"
                    )
                    _month_bars_files = [
                        f for f in bars_files
                        if f"year={year}" in str(f) and f"month={month:02d}" in str(f)
                    ]
                    _cache_key = lake_cache_key(
                        symbol, market, tf, year, month,
                        _month_bars_files,
                        feature_schema_version=feature_schema_version,
                        baseline_run_id=baseline_run_id or "",
                        start=str(args.start),
                        end=str(args.end),
                    )
                    actual_path = _written_path(write_parquet(group, out_path), out_path)
                    logging.info(f"Wrote features for {symbol} {year}-{month:02d} to {actual_path}")
                    outputs.append(
                        {
                            "path": str(actual_path),
                            "rows": len(group),
                            "start_ts": group["timestamp"].min().isoformat(),
                            "end_ts": group["timestamp"].max().isoformat(),
                        }
                    )
                    # Populate shared cache for future runs
                    if actual_path.exists() and not shared_path.exists() and _cache_key:
                        ensure_dir(shared_path.parent)
                        shutil.copy2(actual_path, shared_path)
                        write_cache_key(shared_path, _cache_key)

                report_path = _feature_quality_report_path(
                    data_root,
                    run_id=run_id,
                    market=market,
                    symbol=symbol,
                    timeframe=tf,
                    feature_schema_version=feature_schema_version,
                )
                quality_payload = {
                    "schema_version": "feature_quality_report_v2",
                    "run_id": run_id,
                    "market": market,
                    "symbol": symbol,
                    "timeframe": tf,
                    "feature_schema_version": feature_schema_version,
                    "baseline_run_id": baseline_run_id or None,
                    "quality": summarize_feature_quality(
                        out,
                        baseline_frame=baseline_features if not baseline_features.empty else None,
                        baseline_label=baseline_run_id or None,
                    ),
                }
                _write_feature_quality_report(report_path, quality_payload)
                outputs.append({"path": str(report_path), "rows": 1})
                stats["symbols"][symbol] = {
                    "rows": len(out),
                    "feature_quality_report_path": str(report_path),
                    "feature_quality_summary": quality_payload["quality"],
                }

        if not outputs:
            raise RuntimeError("build_features produced no feature artifacts")
        manifest["outputs"] = outputs
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as e:
        logging.exception("Feature building failed")
        finalize_manifest(manifest, "failed", error=str(e), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
