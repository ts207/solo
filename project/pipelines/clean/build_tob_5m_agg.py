from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from project.core.config import get_data_root
from project.io.utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
    write_parquet,
)
from project.specs.manifest import finalize_manifest, start_manifest


def main() -> int:
    data_root = get_data_root()
    parser = argparse.ArgumentParser(description="Build 5m ToB aggregates from 1s snapshots")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(
        level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s"
    )

    params = {
        "symbols": symbols,
        "agg_interval": "5m",
    }
    inputs: list[dict[str, object]] = []
    outputs: list[dict[str, object]] = []
    manifest = start_manifest("build_tob_5m_agg", args.run_id, params, inputs, outputs)
    stats: dict[str, object] = {"symbols": {}}

    try:
        for symbol in symbols:
            # Prefer run-scoped tob_1s inputs, fall back to shared cleaned lake
            run_tob_dir = run_scoped_lake_path(data_root, args.run_id, "cleaned", "perp", symbol, "tob_1s")
            shared_tob_dir = data_root / "lake" / "cleaned" / "perp" / symbol / "tob_1s"
            tob_dir = choose_partition_dir([run_tob_dir, shared_tob_dir])
            if not tob_dir:
                logging.warning("No ToB 1s data for %s", symbol)
                continue
            files = list_parquet_files(tob_dir)
            if not files:
                logging.warning("No ToB 1s data for %s", symbol)
                continue

            for file_path in sorted(files):
                data = read_parquet([file_path])
                if data.empty:
                    continue

                inputs.append(
                    {
                        "path": str(file_path),
                        "rows": len(data),
                    }
                )

                data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)

                data["mid_price"] = (data["bid_price"] + data["ask_price"]) / 2
                data["spread_bps"] = (
                    (data["ask_price"] - data["bid_price"]) / data["mid_price"] * 10000
                )
                data["bid_depth_usd"] = data["bid_price"] * data["bid_qty"]
                data["ask_depth_usd"] = data["ask_price"] * data["ask_qty"]
                data["imbalance"] = data["bid_qty"] / (data["bid_qty"] + data["ask_qty"])
                data["valid_snapshot"] = data["mid_price"].notna().astype(float)

                resampler = data.resample("5min", on="timestamp")
                agg = resampler.agg(
                    {
                        "spread_bps": ["mean", "max", "std"],
                        "bid_depth_usd": "mean",
                        "ask_depth_usd": "mean",
                        "imbalance": "mean",
                        "mid_price": "last",
                        "valid_snapshot": "mean",
                    }
                )

                # Flatten MultiIndex columns
                agg.columns = [
                    f"{col[0]}_{col[1]}" if isinstance(col, tuple) and col[1] else col[0]
                    for col in agg.columns
                ]
                if "valid_snapshot_mean" in agg.columns:
                    agg = agg.rename(columns={"valid_snapshot_mean": "tob_coverage"})
                agg = agg.reset_index()
                agg["symbol"] = symbol

                first_ts = data["timestamp"].min()
                month_key = f"{first_ts.year}-{first_ts.month:02d}"

                # Write to run-scoped output
                out_dir = run_scoped_lake_path(
                    data_root, args.run_id, "cleaned", "perp", symbol, "tob_5m_agg",
                    f"year={first_ts.year}", f"month={first_ts.month:02d}"
                )
                out_path = out_dir / f"tob_agg_{symbol}_5m_{month_key}.parquet"

                ensure_dir(out_dir)
                written, storage = write_parquet(agg, out_path)
                outputs.append(
                    {
                        "path": str(written),
                        "rows": len(agg),
                        "start_ts": agg["timestamp"].min().isoformat(),
                        "end_ts": agg["timestamp"].max().isoformat(),
                        "storage": storage,
                    }
                )

                stats["symbols"].setdefault(symbol, {})[month_key] = {
                    "agg_rows": len(agg),
                }

        manifest["outputs"] = outputs
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("ToB aggregation failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
