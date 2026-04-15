from __future__ import annotations
from project.core.config import get_data_root

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
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
    parser = argparse.ArgumentParser(description="Build 1s ToB snapshots from bookTicker")
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
        "snapshot_interval": "1s",
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("build_tob_snapshots_1s", args.run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        for symbol in symbols:
            # Prefer run-scoped raw inputs, fall back to shared raw lake
            run_raw_dir = run_scoped_lake_path(data_root, args.run_id, "raw", "binance", "perp", symbol, "book_ticker")
            shared_raw_dir = data_root / "lake" / "raw" / "binance" / "perp" / symbol / "book_ticker"
            raw_dir = choose_partition_dir([run_raw_dir, shared_raw_dir])
            if not raw_dir:
                logging.warning("No bookTicker data for %s", symbol)
                continue
            files = list_parquet_files(raw_dir)
            if not files:
                logging.warning("No bookTicker data for %s", symbol)
                continue

            for file_path in sorted(files):
                data = read_parquet([file_path])
                if data.empty:
                    continue

                inputs.append(
                    {
                        "path": str(file_path),
                        "rows": int(len(data)),
                    }
                )

                data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True)
                data = data.sort_values("timestamp")

                start_ts = data["timestamp"].min().floor("1s")
                end_ts = data["timestamp"].max().ceil("1s")
                full_index = pd.date_range(start=start_ts, end=end_ts, freq="1s", tz=timezone.utc)

                grid = pd.DataFrame({"timestamp": full_index})
                resampled = pd.merge_asof(
                    grid, data, on="timestamp", direction="backward", tolerance=pd.Timedelta("5s")
                )

                first_ts = data["timestamp"].min()
                month_key = f"{first_ts.year}-{first_ts.month:02d}"

                # Write to run-scoped output
                out_dir = run_scoped_lake_path(
                    data_root, args.run_id, "cleaned", "perp", symbol, "tob_1s",
                    f"year={first_ts.year}", f"month={first_ts.month:02d}"
                )
                out_path = out_dir / f"tob_{symbol}_1s_{month_key}.parquet"

                ensure_dir(out_dir)
                written, storage = write_parquet(resampled, out_path)
                outputs.append(
                    {
                        "path": str(written),
                        "rows": int(len(resampled)),
                        "start_ts": resampled["timestamp"].min().isoformat(),
                        "end_ts": resampled["timestamp"].max().isoformat(),
                        "storage": storage,
                    }
                )

                stats["symbols"].setdefault(symbol, {})[month_key] = {
                    "raw_rows": int(len(data)),
                    "snapshot_rows": int(len(resampled)),
                }

        manifest["outputs"] = outputs
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("ToB snapshot build failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
