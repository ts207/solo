from __future__ import annotations
from project.core.config import get_data_root

import argparse
import logging
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple
from zipfile import ZipFile

import pandas as pd
import requests
from project.io.http_utils import download_with_retries
from project.io.utils import ensure_dir, read_parquet, write_parquet
from project.specs.manifest import finalize_manifest, start_manifest
from project.io.url_utils import join_url
from project.core.validation import ensure_utc_timestamp

ARCHIVE_BASE = "https://data.binance.vision/data/futures/um"
EARLIEST_UM_FUTURES = datetime(2019, 9, 1, tzinfo=timezone.utc)

def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)

def _month_start(ts: datetime) -> datetime:
    return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def _next_month(ts: datetime) -> datetime:
    year = ts.year + (ts.month // 12)
    month = 1 if ts.month == 12 else ts.month + 1
    return ts.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)

def _iter_months(start: datetime, end: datetime) -> List[datetime]:
    months: List[datetime] = []
    cursor = _month_start(start)
    while cursor <= end:
        months.append(cursor)
        cursor = _next_month(cursor)
    return months

def _iter_days(start: datetime, end: datetime) -> List[datetime]:
    days: List[datetime] = []
    cursor = start.replace(hour=0, minute=0, second=0, microsecond=0)
    while cursor <= end:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days

def _normalize_metrics(df: pd.DataFrame, symbol: str, source: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    # Determine timestamp column
    ts_col = None
    for candidate in ("create_time", "createTime", "timestamp", "time", "open_time"):
        if candidate in df.columns:
            ts_col = candidate
            break
    if ts_col is None:
        ts_col = df.columns[0]

    # Mapping based on typical Binance metrics columns
    mapping = {
        ts_col: "timestamp",
        "sum_open_interest": "sum_open_interest",
        "sumOpenInterest": "sum_open_interest",
        "sum_open_interest_value": "sum_open_interest_value",
        "sumOpenInterestValue": "sum_open_interest_value",
        "top_long_short_account_ratio": "top_long_short_account_ratio",
        "topLongShortAccountRatio": "top_long_short_account_ratio",
        "top_long_short_position_ratio": "top_long_short_position_ratio",
        "topLongShortPositionRatio": "top_long_short_position_ratio",
        "global_long_short_account_ratio": "global_long_short_account_ratio",
        "globalLongShortAccountRatio": "global_long_short_account_ratio",
        "taker_buy_sell_vol": "taker_buy_sell_vol",
        "takerBuySellVol": "taker_buy_sell_vol",
        "taker_buy_sell_quote_vol": "taker_buy_sell_quote_vol",
        "takerBuySellQuoteVol": "taker_buy_sell_quote_vol",
    }

    rename_map = {k: v for k, v in mapping.items() if k in df.columns}
    out = df[list(rename_map.keys())].rename(columns=rename_map).copy()
    
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    for col in out.columns:
        if col not in ("timestamp", "symbol", "source"):
            out[col] = pd.to_numeric(out[col], errors="coerce")
            
    out["symbol"] = symbol
    out["source"] = source
    out = out.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"])
    ensure_utc_timestamp(out["timestamp"], "timestamp")
    return out

def _read_metrics_from_zip(path: Path, symbol: str, source: str) -> pd.DataFrame:
    with ZipFile(path) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            df = pd.read_csv(f)
    return _normalize_metrics(df, symbol, source)

def main() -> int:
    data_root = get_data_root()
    parser = argparse.ArgumentParser(description="Ingest Binance USD-M Monthly Metrics")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--out_root", default=str(data_root / "lake" / "raw" / "binance" / "perp"))
    parser.add_argument("--max_retries", type=int, default=5)
    parser.add_argument("--retry_backoff_sec", type=float, default=2.0)
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    requested_start = _parse_date(args.start)
    requested_end = _parse_date(args.end)
    effective_start = max(requested_start, EARLIEST_UM_FUTURES)
    effective_end = requested_end

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(
        level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s"
    )

    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    params = vars(args)
    manifest = start_manifest("ingest_binance_um_metrics", run_id, params, inputs, outputs)

    stats: Dict[str, object] = {"symbols": {}}

    try:
        out_root = Path(args.out_root)
        session = requests.Session()

        for symbol in symbols:
            missing_archives: List[str] = []
            partitions_written: List[str] = []
            partitions_skipped: List[str] = []
            rows_written_total = 0

            for month_start in _iter_months(effective_start, effective_end):
                month_end = _next_month(month_start)
                range_start = max(effective_start, month_start)
                range_end_exclusive = min(effective_end + timedelta(days=1), month_end)

                out_dir = (
                    out_root
                    / symbol
                    / "metrics"
                    / f"year={month_start.year}"
                    / f"month={month_start.month:02d}"
                )
                out_path = (
                    out_dir
                    / f"metrics_{symbol}_{month_start.year}-{month_start.month:02d}.parquet"
                )

                if not args.force and out_path.exists():
                    partitions_skipped.append(str(out_path))
                    continue

                # Try monthly first
                monthly_url = join_url(
                    ARCHIVE_BASE,
                    "monthly",
                    "metrics",
                    symbol,
                    f"{symbol}-metrics-{month_start.year}-{month_start.month:02d}.zip",
                )
                logging.info("Downloading monthly archive %s", monthly_url)

                with tempfile.TemporaryDirectory() as tmpdir:
                    temp_zip = Path(tmpdir) / "data.zip"
                    result = download_with_retries(
                        monthly_url,
                        temp_zip,
                        max_retries=args.max_retries,
                        backoff_sec=args.retry_backoff_sec,
                        session=session,
                    )

                    frames: List[pd.DataFrame] = []
                    if result.status == "ok":
                        frames.append(
                            _read_metrics_from_zip(temp_zip, symbol, "archive_monthly")
                        )
                    else:
                        if result.status == "not_found":
                            missing_archives.append(monthly_url)
                        else:
                            raise RuntimeError(f"Failed to download {monthly_url}: {result.error}")

                        # Fallback to daily
                        for day in _iter_days(
                            range_start, range_end_exclusive - timedelta(seconds=1)
                        ):
                            daily_url = join_url(
                                ARCHIVE_BASE,
                                "daily",
                                "metrics",
                                symbol,
                                f"{symbol}-metrics-{day.year}-{day.month:02d}-{day.day:02d}.zip",
                            )
                            logging.info("Downloading daily archive %s", daily_url)
                            daily_zip = Path(tmpdir) / f"data_{day:%Y%m%d}.zip"
                            daily_result = download_with_retries(
                                daily_url,
                                daily_zip,
                                max_retries=args.max_retries,
                                backoff_sec=args.retry_backoff_sec,
                                session=session,
                            )
                            if daily_result.status == "ok":
                                frames.append(
                                    _read_metrics_from_zip(daily_zip, symbol, "archive_daily")
                                )
                            elif daily_result.status == "not_found":
                                missing_archives.append(daily_url)

                if frames:
                    data = pd.concat(frames, ignore_index=True)
                    data = data.sort_values("timestamp").drop_duplicates(subset=["timestamp"])
                    data = data[
                        (data["timestamp"] >= range_start)
                        & (data["timestamp"] < range_end_exclusive)
                    ]
                else:
                    data = pd.DataFrame()

                if not data.empty:
                    ensure_dir(out_dir)
                    written_path, storage = write_parquet(data, out_path)
                    outputs.append(
                        {
                            "path": str(written_path),
                            "rows": int(len(data)),
                            "start_ts": data["timestamp"].min().isoformat(),
                            "end_ts": data["timestamp"].max().isoformat(),
                            "storage": storage,
                        }
                    )
                    partitions_written.append(str(written_path))
                    rows_written_total += int(len(data))
                else:
                    logging.info(
                        "No data for %s %s-%02d", symbol, month_start.year, month_start.month
                    )

            stats["symbols"][symbol] = {
                "rows_written": rows_written_total,
                "missing_archive_files": missing_archives,
                "partitions_written": partitions_written,
                "partitions_skipped": partitions_skipped,
            }

        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Ingestion failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1

if __name__ == "__main__":
    sys.exit(main())
