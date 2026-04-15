from __future__ import annotations
from project.core.config import get_data_root

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd
from project.core.data_quality import summarize_frame_quality
from project.io.utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)
from project.specs.manifest import finalize_manifest, start_manifest


def _report_path(data_root: Path, *, run_id: str, timeframe: str) -> Path:
    return (
        data_root
        / "reports"
        / "data_quality"
        / run_id
        / "validation"
        / f"validate_data_coverage_{timeframe}.json"
    )


def _write_report(path: Path, payload: dict[str, object]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _evaluate_threshold(
    *,
    label: str,
    value: float | int,
    fail_threshold: float | int | None,
    warn_threshold: float | int | None,
    fmt: str,
) -> tuple[str | None, str | None]:
    if fail_threshold is not None and value > fail_threshold:
        return (
            "failure",
            f"{label}={format(value, fmt)} exceeds fail threshold {format(fail_threshold, fmt)}",
        )
    if warn_threshold is not None and value > warn_threshold:
        return (
            "warning",
            f"{label}={format(value, fmt)} exceeds warn threshold {format(warn_threshold, fmt)}",
        )
    return (None, None)


def main() -> int:
    parser = argparse.ArgumentParser(description="Certification Gate: Minimum Data Coverage")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--max_gap_pct", type=float, default=0.05)
    parser.add_argument("--warn_gap_pct", type=float, default=None)
    parser.add_argument("--max_missing_ratio", type=float, default=0.01)
    parser.add_argument("--warn_missing_ratio", type=float, default=None)
    parser.add_argument("--max_outlier_ratio", type=float, default=0.01)
    parser.add_argument("--warn_outlier_ratio", type=float, default=None)
    parser.add_argument("--max_duplicate_timestamps", type=int, default=0)
    parser.add_argument("--warn_duplicate_timestamps", type=int, default=None)
    parser.add_argument("--timeframe", default="5m")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    data_root = get_data_root()

    manifest = start_manifest(
        f"validate_data_coverage_{args.timeframe}", args.run_id, vars(args), [], []
    )

    symbol_stats = {}
    total_failures = 0
    total_warnings = 0

    for symbol in symbols:
        candidates = [
            run_scoped_lake_path(
                data_root, args.run_id, "cleaned", "perp", symbol, f"bars_{args.timeframe}"
            ),
            data_root / "lake" / "cleaned" / "perp" / symbol / f"bars_{args.timeframe}",
        ]
        path = choose_partition_dir(candidates)
        if not path:
            logging.error(f"Missing cleaned bars for {symbol}")
            total_failures += 1
            continue

        df = read_parquet(list_parquet_files(path))
        if df.empty:
            logging.error(f"Empty cleaned bars for {symbol}")
            total_failures += 1
            continue

        if "is_gap" not in df.columns:
            logging.error(f"Missing 'is_gap' column for {symbol}")
            total_failures += 1
            continue

        quality = summarize_frame_quality(
            df,
            expected_minutes=5 if args.timeframe == "5m" else None,
            numeric_cols=[
                "open",
                "high",
                "low",
                "close",
                "volume",
                "quote_volume",
                "taker_base_volume",
            ],
        )
        gap_pct = float(df["is_gap"].mean())
        warnings: list[str] = []
        failures: list[str] = []

        for level, message in [
            _evaluate_threshold(
                label="gap_pct",
                value=gap_pct,
                fail_threshold=args.max_gap_pct,
                warn_threshold=args.warn_gap_pct,
                fmt=".2%",
            ),
            _evaluate_threshold(
                label="missing_ratio",
                value=quality.missing_ratio,
                fail_threshold=args.max_missing_ratio,
                warn_threshold=args.warn_missing_ratio,
                fmt=".4f",
            ),
            _evaluate_threshold(
                label="outlier_ratio",
                value=quality.outlier_ratio,
                fail_threshold=args.max_outlier_ratio,
                warn_threshold=args.warn_outlier_ratio,
                fmt=".4f",
            ),
            _evaluate_threshold(
                label="duplicate_timestamp_count",
                value=quality.duplicate_timestamp_count,
                fail_threshold=args.max_duplicate_timestamps,
                warn_threshold=args.warn_duplicate_timestamps,
                fmt="d",
            ),
        ]:
            if level == "failure" and message is not None:
                logging.error("Symbol %s %s", symbol, message)
                failures.append(message)
                total_failures += 1
            elif level == "warning" and message is not None:
                logging.warning("Symbol %s %s", symbol, message)
                warnings.append(message)
                total_warnings += 1

        symbol_status = "failed" if failures else "warn" if warnings else "success"
        symbol_stats[symbol] = {
            "status": symbol_status,
            "gap_pct": gap_pct,
            **quality.to_dict(),
            "warnings": warnings,
            "failures": failures,
        }

    status = "success" if total_failures == 0 else "failed"
    report_path = _report_path(data_root, run_id=args.run_id, timeframe=args.timeframe)
    report_payload = {
        "schema_version": "data_quality_validation_v1",
        "run_id": args.run_id,
        "timeframe": args.timeframe,
        "thresholds": {
            "max_gap_pct": args.max_gap_pct,
            "warn_gap_pct": args.warn_gap_pct,
            "max_missing_ratio": args.max_missing_ratio,
            "warn_missing_ratio": args.warn_missing_ratio,
            "max_outlier_ratio": args.max_outlier_ratio,
            "warn_outlier_ratio": args.warn_outlier_ratio,
            "max_duplicate_timestamps": args.max_duplicate_timestamps,
            "warn_duplicate_timestamps": args.warn_duplicate_timestamps,
        },
        "failure_count": total_failures,
        "warning_count": total_warnings,
        "symbols": symbol_stats,
    }
    _write_report(report_path, report_payload)
    finalize_manifest(
        manifest,
        status,
        stats={
            "failure_count": total_failures,
            "warning_count": total_warnings,
            "report_path": str(report_path),
            "symbols": symbol_stats,
        },
    )
    return 1 if total_failures > 0 else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sys.exit(main())
