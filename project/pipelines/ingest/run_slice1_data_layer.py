from __future__ import annotations

import argparse
import logging
import os
import sys
import subprocess
from pathlib import Path
from typing import List

from project import PROJECT_ROOT


def run_script(script_path: Path, args: List[str]):
    cmd = [sys.executable, str(script_path)] + args
    logging.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        logging.error("Script %s failed with return code %d", script_path, result.returncode)
        sys.exit(result.returncode)


def run_parallel(scripts: List[tuple[Path, List[str]]]):
    processes: list[tuple[Path, subprocess.Popen]] = []
    for script_path, args in scripts:
        cmd = [sys.executable, str(script_path)] + args
        logging.info("Starting Parallel: %s", " ".join(cmd))
        processes.append((script_path, subprocess.Popen(cmd)))

    failed: dict[Path, int] = {}
    for script_path, process in processes:
        rc = int(process.wait())
        if rc != 0:
            failed[script_path] = rc

    if failed:
        for script_path, rc in failed.items():
            logging.error("Parallel script failed: %s (return code=%d)", script_path, rc)
        failure_count = len(failed)
        logging.error("Parallel phase failed: %d script(s) failed.", failure_count)
        sys.exit(min(failure_count, 255))


def main():
    parser = argparse.ArgumentParser(description="Run Slice 1 Data Layer Pipeline (Optimized)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--force", type=int, default=0)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    common_args = ["--run_id", args.run_id, "--symbols", args.symbols]
    ingest_time_args = ["--start", args.start, "--end", args.end, "--force", str(args.force)]

    # 1. Parallel Ingestion (Raw Light Data)
    logging.info("--- Starting Light Data Ingestion (Parallel) ---")
    run_parallel(
        [
            (
                PROJECT_ROOT / "pipelines/ingest/ingest_binance_um_ohlcv.py",
                common_args + ingest_time_args + ["--timeframe", "5m"],
            ),
            (PROJECT_ROOT / "pipelines/ingest/ingest_binance_spot_ohlcv_5m.py", common_args + ingest_time_args),
            (
                PROJECT_ROOT / "pipelines/ingest/ingest_binance_um_mark_price_5m.py",
                common_args + ingest_time_args,
            ),
        ]
    )

    # 2. Heavy Ingestion (Sequential/Internal Parallelism)
    logging.info("--- Starting Book Ticker Ingestion (Heavy) ---")
    run_script(PROJECT_ROOT / "pipelines/ingest/ingest_binance_um_book_ticker.py", common_args + ingest_time_args)

    # 3. Remaining Ingestion
    logging.info("--- Starting Remaining Ingestion ---")
    run_parallel(
        [
            (PROJECT_ROOT / "pipelines/ingest/ingest_binance_um_funding.py", common_args + ingest_time_args),
            (
                PROJECT_ROOT / "pipelines/ingest/ingest_binance_um_open_interest_hist.py",
                common_args + ingest_time_args,
            ),
        ]
    )

    # 4. Cleaning (Cleaned) - --force removed (not supported)
    logging.info("--- Starting Data Cleaning ---")
    run_parallel(
        [
            (
                PROJECT_ROOT / "pipelines/clean/build_cleaned_bars.py",
                common_args + ["--market", "perp"],
            ),
            (
                PROJECT_ROOT / "pipelines/clean/build_cleaned_bars.py",
                common_args + ["--market", "spot"],
            ),
        ]
    )

    # 5. ToB & Basis Processing - --force removed (not supported)
    logging.info("--- Starting ToB & Basis Processing ---")
    run_script(
        PROJECT_ROOT / "pipelines/clean/build_tob_snapshots_1s.py",
        common_args,
    )
    run_script(
        PROJECT_ROOT / "pipelines/clean/build_tob_5m_agg.py", common_args
    )
    run_script(
        PROJECT_ROOT / "pipelines/clean/build_basis_state_5m.py",
        common_args,
    )

    # 6. QA Report
    run_script(PROJECT_ROOT / "pipelines/report/qa_data_layer.py", common_args)

    logging.info("Slice 1 Data Layer Pipeline completed successfully.")


if __name__ == "__main__":
    main()
