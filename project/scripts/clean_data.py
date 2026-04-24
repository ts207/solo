from __future__ import annotations

import argparse
import logging
import shutil
import sys
from datetime import datetime, timedelta

from project.core.config import get_data_root


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean up old data artifacts.")
    parser.add_argument(
        "--days", type=int, default=14, help="Delete run artifacts older than this many days"
    )
    parser.add_argument(
        "--dry_run", action="store_true", help="Print what would be deleted without deleting"
    )
    parser.add_argument(
        "--full", action="store_true", help="Delete ALL run artifacts, logs, experiments, knowledge, and cache (ignores --days)"
    )
    parser.add_argument(
        "--include_synthetic_test", action="store_true", help="Also delete synthetic/test_run directory"
    )
    parser.add_argument(
        "--include_knowledge", action="store_true", help="Also delete knowledge directory"
    )
    parser.add_argument(
        "--include_research", action="store_true", help="Also delete research directory"
    )
    parser.add_argument(
        "--include_artifacts", action="store_true", help="Also delete artifacts directory"
    )
    parser.add_argument(
        "--include_lake_runs", action="store_true", help="Also delete lake/runs directory"
    )
    parser.add_argument(
        "--include_raw", action="store_true", help="Also delete lake/raw data (ingested OHLCV, funding, etc)"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    data_root = get_data_root()

    if args.full:
        dirs_to_clean = ["runs", "reports", "events", "lake/runs"]

        if args.include_knowledge:
            dirs_to_clean.append("knowledge")
        if args.include_research:
            dirs_to_clean.append("research")
        if args.include_artifacts:
            dirs_to_clean.append("artifacts")
        if args.include_lake_runs:
            dirs_to_clean.append("lake/runs")

        deleted_count = 0
        for dir_name in dirs_to_clean:
            target_dir = data_root / dir_name
            if target_dir.exists() and target_dir.is_dir():
                for sub_dir in target_dir.iterdir():
                    if sub_dir.is_dir() and sub_dir.name != ".gitkeep":
                        logging.info(f"Deleting {sub_dir}")
                        if not args.dry_run:
                            shutil.rmtree(sub_dir, ignore_errors=True)
                        deleted_count += 1

        synthetic_test_dir = data_root / "synthetic" / "test_run"
        if args.include_synthetic_test and synthetic_test_dir.exists():
            logging.info(f"Deleting {synthetic_test_dir}")
            if not args.dry_run:
                shutil.rmtree(synthetic_test_dir, ignore_errors=True)
            deleted_count += 1

        if args.include_raw:
            raw_dir = data_root / "lake" / "raw"
            if raw_dir.exists():
                for sub_dir in raw_dir.rglob("*"):
                    if sub_dir.is_dir() and sub_dir.name not in [".gitkeep"]:
                        logging.info(f"Deleting {sub_dir}")
                        if not args.dry_run:
                            shutil.rmtree(sub_dir, ignore_errors=True)
                        deleted_count += 1
                    elif sub_dir.is_file() and sub_dir.suffix == ".parquet":
                        logging.info(f"Deleting {sub_dir}")
                        if not args.dry_run:
                            sub_dir.unlink()
                        deleted_count += 1
                for sym_dir in (raw_dir / "binance" / "perp").iterdir() if (raw_dir / "binance" / "perp").exists() else []:
                    for sub in sym_dir.iterdir():
                        if sub.is_dir() and sub.name in ["ohlcv_5m", "funding", "ohlcv_1m", "ohlcv_15m", "ohlcv_1h", "ohlcv_4h", "liquidations", "open_interest"]:
                            logging.info(f"Deleting {sub}")
                            if not args.dry_run:
                                shutil.rmtree(sub, ignore_errors=True)
                            deleted_count += 1

        if deleted_count == 0:
            logging.info("No data artifacts found to clean.")
        else:
            logging.info(f"Full clean removed {deleted_count} directories.")

        return 0

    cutoff_date = datetime.now() - timedelta(days=args.days)

    dirs_to_clean = ["runs", "reports", "events", "research", "lake/runs"]

    deleted_count = 0
    for dir_name in dirs_to_clean:
        target_dir = data_root / dir_name
        if target_dir.exists() and target_dir.is_dir():
            for sub_dir in target_dir.iterdir():
                if sub_dir.is_dir():
                    mtime = datetime.fromtimestamp(sub_dir.stat().st_mtime)
                    if mtime < cutoff_date:
                        logging.info(f"Deleting {sub_dir} (last modified {mtime})")
                        if not args.dry_run:
                            shutil.rmtree(sub_dir, ignore_errors=True)
                        deleted_count += 1

    if deleted_count == 0:
        logging.info("No old data artifacts found to clean.")
    else:
        logging.info(f"Cleaned {deleted_count} directories.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
