"""
Bybit V5 Derivatives OHLCV ingestion for arbitrary minute timeframes.

This script fetches historical OHLCV, Mark Price, or Index Price data from the Bybit V5 REST API
and writes it to partitioned Parquet files.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import aiohttp
import pandas as pd

from project.core.config import get_data_root
from project.core.validation import ensure_utc_timestamp, filter_ohlcv_geometry_violations
from project.io.utils import ensure_dir, write_parquet
from project.specs.manifest import finalize_manifest, start_manifest

_LOG = logging.getLogger(__name__)


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


def _minutes_per_bar(timeframe: str) -> int:
    tf = timeframe.strip().lower()
    if tf == "1h":
        return 60
    if tf == "4h":
        return 240
    if tf == "1d":
        return 1440
    if not tf.endswith("m"):
        raise ValueError(f"Unsupported timeframe format: {timeframe}")
    return int(tf[:-1])


async def _fetch_klines(
    session: aiohttp.ClientSession,
    endpoint: str,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
) -> List[List[Any]]:
    """Fetch klines from Bybit V5 API."""
    url = f"https://api.bybit.com{endpoint}"
    params = {
        "category": "linear",
        "symbol": symbol.upper(),
        "interval": interval,
        "start": start_ms,
        "end": end_ms,
        "limit": limit,
    }

    if interval.endswith("m"):
        params["interval"] = interval[:-1]
    elif interval == "1h":
        params["interval"] = "60"
    elif interval == "4h":
        params["interval"] = "240"
    elif interval == "1d":
        params["interval"] = "D"

    async with session.get(url, params=params) as resp:
        if resp.status != 200:
            _LOG.error(f"Bybit API Error: {resp.status} for {url}")
            resp.raise_for_status()
        data = await resp.json()
        if data.get("retCode") != 0:
            # If no data for this range, Bybit might return 0 or error.
            # We treat certain retCodes as 'no data'
            if data.get("retCode") in [10001, 10002]: # Example codes
                 return []
            raise Exception(f"Bybit Error: {data.get('retMsg')} ({data.get('retCode')})")
        return data.get("result", {}).get("list", [])


async def _ingest_symbol_month(
    semaphore: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    endpoint: str,
    symbol: str,
    timeframe: str,
    month_start: datetime,
    requested_start: datetime,
    requested_end: datetime,
    out_root: Path,
    data_type: str,
    force: bool,
    max_retries: int = 5,
    retry_backoff_sec: float = 2.0,
) -> Dict[str, Any]:
    async with semaphore:
        month_end = _next_month(month_start)
        actual_start = max(requested_start, month_start)
        actual_end = min(requested_end + timedelta(days=1), month_end)

        if actual_start >= actual_end:
            return {"status": "noop", "bars": 0}

        out_dir = (
            out_root
            / symbol
            / f"{data_type}_{timeframe}"
            / f"year={month_start.year}"
            / f"month={month_start.month:02d}"
        )
        out_path = (
            out_dir
            / f"{data_type}_{symbol}_{timeframe}_{month_start.year}-{month_start.month:02d}.parquet"
        )

        if not force and out_path.exists():
            return {"status": "skipped", "partition": str(out_path), "bars": 0}

        _LOG.info(f"Ingesting {data_type} for {symbol} for {month_start.strftime('%Y-%m')}")

        all_klines = []
        current_end = int(actual_end.timestamp() * 1000) - 1
        final_start = int(actual_start.timestamp() * 1000)
        consecutive_errors = 0

        while current_end > final_start:
            try:
                batch = await _fetch_klines(
                    session, endpoint, symbol, timeframe, final_start, current_end, limit=1000
                )
                consecutive_errors = 0
                if not batch:
                    break

                all_klines.extend(batch)

                oldest_in_batch = min(int(k[0]) for k in batch)

                if len(batch) < 1000:
                    break

                current_end = oldest_in_batch - 1

                await asyncio.sleep(0.05)
            except Exception as e:
                consecutive_errors += 1
                if consecutive_errors >= max_retries:
                    _LOG.error(
                        f"Error fetching batch for {symbol} after {max_retries} attempts: {e}"
                    )
                    break
                wait = retry_backoff_sec * (2 ** (consecutive_errors - 1))
                _LOG.warning(
                    f"Fetch error for {symbol} (attempt {consecutive_errors}/{max_retries}): "
                    f"{e}. Retrying in {wait:.1f}s..."
                )
                await asyncio.sleep(wait)

        if not all_klines:
            return {"status": "empty", "bars": 0}

        # Bybit kline: [0]startTime, [1]open, [2]high, [3]low, [4]close, [5]volume, [6]turnover
        # Mark/Index price klines: [0]startTime, [1]open, [2]high, [3]low, [4]close
        col_count = len(all_klines[0])
        if col_count == 7:
            df = pd.DataFrame(
                all_klines,
                columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"],
            )
        elif col_count == 5:
            df = pd.DataFrame(
                all_klines,
                columns=["timestamp", "open", "high", "low", "close"],
            )
            df["volume"] = 0.0
            df["turnover"] = 0.0
        else:
            raise ValueError(f"Unexpected column count in Bybit kline data: {col_count}")

        df["timestamp"] = pd.to_datetime(df["timestamp"].astype(int), unit="ms", utc=True)
        df = df.sort_values("timestamp").drop_duplicates("timestamp")

        # Filter to requested range
        df = df[(df["timestamp"] >= actual_start) & (df["timestamp"] < actual_end)]

        if df.empty:
            return {"status": "empty", "bars": 0}

        df["symbol"] = symbol.upper()
        df["source"] = "bybit_v5"
        for col in ["open", "high", "low", "close", "volume", "turnover"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        if data_type == "ohlcv":
            df = df.rename(columns={"turnover": "quote_volume"})
            df["taker_base_volume"] = 0.0
            df, dropped = filter_ohlcv_geometry_violations(df, label=f"{symbol}/{timeframe}")
            if dropped:
                _LOG.warning(
                    "Dropped %d row(s) with OHLCV geometry violations for %s %s",
                    dropped, symbol, timeframe,
                )
        elif data_type == "mark_price":
            df = df.rename(columns={"close": "mark_price"})
            df = df[["timestamp", "mark_price", "symbol", "source"]]
        elif data_type == "index_price":
            df = df.rename(columns={"close": "index_price"})
            df = df[["timestamp", "index_price", "symbol", "source"]]

        ensure_utc_timestamp(df["timestamp"], "timestamp")
        ensure_dir(out_dir)
        write_parquet(df, out_path)

        return {"status": "written", "partition": str(out_path), "bars": len(df)}


async def async_main(args: argparse.Namespace) -> Dict[str, Any]:
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    start_dt = _parse_date(args.start)
    end_dt = _parse_date(args.end)
    out_root = Path(args.out_root)

    endpoint_map = {
        "ohlcv": "/v5/market/kline",
        "mark_price": "/v5/market/mark-price-kline",
        "index_price": "/v5/market/index-price-kline",
    }
    endpoint = endpoint_map.get(args.data_type, "/v5/market/kline")

    semaphore = asyncio.Semaphore(args.concurrency)
    stats: Dict[str, Any] = {"symbols": {}}
    outputs = []

    async with aiohttp.ClientSession() as session:
        for symbol in symbols:
            tasks = []
            for month_start in _iter_months(start_dt, end_dt):
                tasks.append(
                    _ingest_symbol_month(
                        semaphore,
                        session,
                        endpoint,
                        symbol,
                        args.timeframe,
                        month_start,
                        start_dt,
                        end_dt,
                        out_root,
                        args.data_type,
                        bool(args.force),
                        max_retries=args.max_retries,
                        retry_backoff_sec=args.retry_backoff_sec,
                    )
                )

            results = await asyncio.gather(*tasks)

            bars_total = 0
            written = []
            for res in results:
                if res["status"] == "written":
                    bars_total += res["bars"]
                    written.append(res["partition"])
                    outputs.append({"path": res["partition"], "rows": res["bars"], "storage": "parquet"})

            stats["symbols"][symbol] = {
                "bars_written_total": bars_total,
                "partitions_written": written,
            }

    return {"stats": stats, "outputs": outputs}


def main() -> int:
    data_root = get_data_root()
    parser = argparse.ArgumentParser(description="Ingest Bybit Derivatives Kline Data (OHLCV, Mark, Index)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--data_type", default="ohlcv", choices=["ohlcv", "mark_price", "index_price"])
    parser.add_argument(
        "--out_root",
        default=str(data_root / "lake" / "raw" / "bybit" / "perp"),
    )
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--max_retries", type=int, default=5)
    parser.add_argument("--retry_backoff_sec", type=float, default=2.0)
    parser.add_argument("--force", type=int, default=0)

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    manifest = start_manifest(
        f"ingest_bybit_derivatives_{args.data_type}_{args.timeframe}",
        args.run_id,
        vars(args),
        [],
        []
    )

    try:
        result = asyncio.run(async_main(args))
        manifest["outputs"] = result["outputs"]
        finalize_manifest(manifest, "success", stats=result["stats"])
        return 0
    except Exception as e:
        _LOG.exception(f"Ingestion failed: {e}")
        finalize_manifest(manifest, "failed", error=str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
