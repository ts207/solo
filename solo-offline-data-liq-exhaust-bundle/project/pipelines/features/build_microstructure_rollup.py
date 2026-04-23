from __future__ import annotations
from project.core.config import get_data_root

import argparse
import logging
import sys
from datetime import timedelta
from pathlib import Path
from argparse import SUPPRESS

import numpy as np
import pandas as pd

from project.io.utils import (
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
    choose_partition_dir,
    write_parquet,
)
from project.specs.manifest import finalize_manifest, start_manifest

_BAR_FREQ = "5min"
_SPREAD_STRESS_THRESHOLD = 2.0  # multiples of median spread


def _load_tob_1s(run_id: str, symbol: str) -> pd.DataFrame:
    data_root = get_data_root()
    # Prefer run-scoped tob_1s, fall back to shared cleaned lake
    run_tob_dir = run_scoped_lake_path(data_root, run_id, "cleaned", "perp", symbol, "tob_1s")
    shared_tob_dir = data_root / "lake" / "cleaned" / "perp" / symbol / "tob_1s"
    tob_dir = choose_partition_dir([run_tob_dir, shared_tob_dir])
    if not tob_dir:
        return pd.DataFrame()
    files = list_parquet_files(tob_dir)
    if not files:
        return pd.DataFrame()
    try:
        return read_parquet(files)
    except Exception:
        return pd.DataFrame()


def _build_rollup(symbol: str, tob: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 1s top-of-book data into 5m microstructure metrics."""
    if tob.empty:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "symbol",
                "micro_spread_stress",
                "micro_depth_depletion",
                "micro_sweep_pressure",
                "micro_imbalance",
                "micro_feature_coverage",
            ]
        )

    tob = tob.copy()
    tob["timestamp"] = pd.to_datetime(tob["timestamp"], utc=True)
    tob = tob.sort_values("timestamp").reset_index(drop=True)

    tob["spread_bps"] = (tob["ask_price"] - tob["bid_price"]) / tob["bid_price"] * 10_000.0
    has_depth = "bid_qty" in tob.columns and "ask_qty" in tob.columns
    if has_depth:
        total_qty = tob["bid_qty"] + tob["ask_qty"]
        tob["imbalance"] = np.where(
            total_qty > 0, (tob["bid_qty"] - tob["ask_qty"]) / total_qty, 0.0
        )
        tob["depth"] = tob["bid_qty"] + tob["ask_qty"]

    # Floor to 5m bars
    tob["bar_ts"] = tob["timestamp"].dt.floor(_BAR_FREQ)

    # Global median spread for stress ratio
    global_median_spread = tob["spread_bps"].median()
    if global_median_spread <= 0:
        global_median_spread = 1.0

    if has_depth:
        tob["abs_imbalance"] = tob["imbalance"].abs()
        tob["sweep_flag"] = (tob["abs_imbalance"] > 0.5).astype(float)
        grouped = (
            tob.groupby("bar_ts", sort=True)
            .agg(
                spread_mean=("spread_bps", "mean"),
                obs_count=("timestamp", "size"),
                depth_mean=("depth", "mean"),
                depth_min=("depth", "min"),
                micro_imbalance=("abs_imbalance", "mean"),
                micro_sweep_pressure=("sweep_flag", "mean"),
            )
            .reset_index()
        )
        grouped["micro_depth_depletion"] = np.where(
            grouped["depth_mean"] > 0.0,
            1.0 - (grouped["depth_min"] / grouped["depth_mean"]),
            0.0,
        )
    else:
        grouped = (
            tob.groupby("bar_ts", sort=True)
            .agg(
                spread_mean=("spread_bps", "mean"),
                obs_count=("timestamp", "size"),
            )
            .reset_index()
        )
        grouped["micro_depth_depletion"] = 0.0
        grouped["micro_imbalance"] = 0.0
        grouped["micro_sweep_pressure"] = 0.0

    if grouped.empty:
        return pd.DataFrame(
            columns=[
                "timestamp",
                "symbol",
                "micro_spread_stress",
                "micro_depth_depletion",
                "micro_sweep_pressure",
                "micro_imbalance",
                "micro_feature_coverage",
            ]
        )

    out = grouped.rename(columns={"bar_ts": "timestamp"}).copy()
    out["symbol"] = symbol
    out["micro_spread_stress"] = out["spread_mean"].astype(float) / float(global_median_spread)
    out["micro_feature_coverage"] = out["obs_count"].astype(float) / 300.0  # 300 = 5*60 1s bars
    out = out[
        [
            "timestamp",
            "symbol",
            "micro_spread_stress",
            "micro_depth_depletion",
            "micro_sweep_pressure",
            "micro_imbalance",
            "micro_feature_coverage",
        ]
    ]
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)
    return out.sort_values("timestamp").reset_index(drop=True)


def main(argv=None) -> int:
    data_root = get_data_root()
    parser = argparse.ArgumentParser(description="Build microstructure rollup.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--log_path", default=None)
    parser.add_argument("--force", default=None, help=SUPPRESS)
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])

    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else data_root / "reports" / "microstructure" / args.run_id
    )
    ensure_dir(out_dir)

    manifest = start_manifest("build_microstructure_rollup", args.run_id, vars(args), [], [])

    try:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        all_frames = []
        for symbol in symbols:
            tob = _load_tob_1s(args.run_id, symbol)
            if tob.empty:
                continue
            if "timestamp" in tob.columns:
                tob["timestamp"] = pd.to_datetime(tob["timestamp"], utc=True)
                if args.start:
                    start_ts = pd.Timestamp(args.start, tz="UTC")
                    tob = tob[tob["timestamp"] >= start_ts]
                if args.end:
                    end_ts = pd.Timestamp(args.end, tz="UTC")
                    end_text = str(args.end or "").strip()
                    if len(end_text) == 10 and "T" not in end_text:
                        end_ts = end_ts + timedelta(days=1)
                    tob = tob[tob["timestamp"] < end_ts]
            rolled = _build_rollup(symbol, tob)
            all_frames.append(rolled)

        if all_frames:
            result = pd.concat(all_frames, ignore_index=True)
        else:
            result = pd.DataFrame(
                columns=[
                    "timestamp",
                    "symbol",
                    "micro_spread_stress",
                    "micro_depth_depletion",
                    "micro_sweep_pressure",
                    "micro_imbalance",
                    "micro_feature_coverage",
                ]
            )

        write_parquet(result, out_dir / "microstructure_rollup.parquet")
        finalize_manifest(manifest, "success", stats={"bars": len(result)})
        return 0
    except Exception:
        logging.exception("Microstructure rollup failed")
        finalize_manifest(manifest, "failed")
        return 1


def build_microstructure_rollup(bars: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Public API wrapper."""
    return _build_rollup(symbol, bars)


if __name__ == "__main__":
    raise SystemExit(main())
