"""
Bybit V5 Derivatives Funding Rate history ingestion.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd

from project.core.config import get_data_root
from project.core.validation import ensure_utc_timestamp
from project.io.utils import ensure_dir, write_parquet
from project.specs.manifest import finalize_manifest, start_manifest

_LOG = logging.getLogger(__name__)


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)


def _month_start(ts: datetime) -> datetime:
    return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(ts: datetime) -> datetime:
    year = ts.year + (ts.month // 12)
    month = 1 if ts.month == 12 else ts.month + 1
    return ts.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)


def _iter_months(start: datetime, end: datetime) -> list[datetime]:
    months: list[datetime] = []
    cursor = _month_start(start)
    while cursor <= end:
        months.append(cursor)
        cursor = _next_month(cursor)
    return months


async def _fetch_funding(
    session: aiohttp.ClientSession,
    symbol: str,
    start_ms: int,
    end_ms: int,
    limit: int = 200,
) -> list[dict[str, Any]]:
    url = "https://api.bybit.com/v5/market/funding/history"
    params = {
        "category": "linear",
        "symbol": symbol.upper(),
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": limit,
    }

    async with session.get(url, params=params) as resp:
        if resp.status != 200:
            resp.raise_for_status()
        data = await resp.json()
        if data.get("retCode") != 0:
            raise Exception(f"Bybit Error: {data.get('retMsg')} ({data.get('retCode')})")
        return data.get("result", {}).get("list", [])


async def _ingest_funding_month(
    semaphore: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    symbol: str,
    month_start: datetime,
    requested_start: datetime,
    requested_end: datetime,
    out_root: Path,
    force: bool,
    max_retries: int = 5,
    retry_backoff_sec: float = 2.0,
) -> dict[str, Any]:
    async with semaphore:
        month_end = _next_month(month_start)
        actual_start = max(requested_start, month_start)
        actual_end = min(requested_end + timedelta(days=1), month_end)

        if actual_start >= actual_end:
            return {"status": "noop", "count": 0}

        out_dir = out_root / symbol / "funding" / f"year={month_start.year}" / f"month={month_start.month:02d}"
        out_path = out_dir / f"funding_{symbol}_{month_start.year}-{month_start.month:02d}.parquet"

        if not force and out_path.exists():
            return {"status": "skipped", "partition": str(out_path), "count": 0}

        _LOG.info(f"Ingesting funding for {symbol} for {month_start.strftime('%Y-%m')}")

        all_records = []
        current_end = int(actual_end.timestamp() * 1000)
        final_start = int(actual_start.timestamp() * 1000)
        consecutive_errors = 0

        while current_end > final_start:
            try:
                batch = await _fetch_funding(session, symbol, final_start, current_end, limit=200)
                consecutive_errors = 0
                if not batch:
                    break

                all_records.extend(batch)
                oldest_in_batch = min(int(k["fundingRateTimestamp"]) for k in batch)

                if len(batch) < 200:
                    break

                current_end = oldest_in_batch - 1
                await asyncio.sleep(0.1)
            except Exception as e:
                consecutive_errors += 1
                if consecutive_errors >= max_retries:
                    _LOG.error(
                        f"Error fetching funding for {symbol} after {max_retries} attempts: {e}"
                    )
                    break
                wait = retry_backoff_sec * (2 ** (consecutive_errors - 1))
                _LOG.warning(
                    f"Funding fetch error for {symbol} (attempt {consecutive_errors}/{max_retries}): "
                    f"{e}. Retrying in {wait:.1f}s..."
                )
                await asyncio.sleep(wait)

        if not all_records:
            return {"status": "empty", "count": 0}

        df = pd.DataFrame(all_records)
        df["timestamp"] = pd.to_datetime(df["fundingRateTimestamp"].astype(int), unit="ms", utc=True)
        df["funding_rate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
        df["symbol"] = symbol.upper()
        df["source"] = "bybit_v5"

        df = df[["timestamp", "funding_rate", "symbol", "source"]].sort_values("timestamp").drop_duplicates("timestamp")
        df = df[(df["timestamp"] >= actual_start) & (df["timestamp"] < actual_end)]

        if df.empty:
            return {"status": "empty", "count": 0}

        ensure_utc_timestamp(df["timestamp"], "timestamp")
        ensure_dir(out_dir)
        write_parquet(df, out_path)

        return {"status": "written", "partition": str(out_path), "count": len(df)}


async def async_main(args: argparse.Namespace) -> dict[str, Any]:
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    start_dt = _parse_date(args.start)
    end_dt = _parse_date(args.end)
    out_root = Path(args.out_root)

    semaphore = asyncio.Semaphore(args.concurrency)
    stats: dict[str, Any] = {"symbols": {}}
    outputs = []

    async with aiohttp.ClientSession() as session:
        for symbol in symbols:
            tasks = [
                _ingest_funding_month(
                    semaphore, session, symbol, m, start_dt, end_dt, out_root, bool(args.force),
                    max_retries=args.max_retries,
                    retry_backoff_sec=args.retry_backoff_sec,
                )
                for m in _iter_months(start_dt, end_dt)
            ]
            results = await asyncio.gather(*tasks)

            count_total = 0
            written = []
            for res in results:
                if res["status"] == "written":
                    count_total += res["count"]
                    written.append(res["partition"])
                    outputs.append({"path": res["partition"], "rows": res["count"], "storage": "parquet"})

            stats["symbols"][symbol] = {"funding_records_total": count_total, "partitions_written": written}

    return {"stats": stats, "outputs": outputs}


def main() -> int:
    data_root = get_data_root()
    parser = argparse.ArgumentParser(description="Ingest Bybit Derivatives Funding Rate history")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--data_type", default="funding")
    parser.add_argument("--out_root", default=str(data_root / "lake" / "raw" / "bybit" / "perp"))
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--max_retries", type=int, default=5)
    parser.add_argument("--retry_backoff_sec", type=float, default=2.0)
    parser.add_argument("--force", type=int, default=0)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    manifest = start_manifest("ingest_bybit_derivatives_funding", args.run_id, vars(args), [], [])

    try:
        result = asyncio.run(async_main(args))
        manifest["outputs"] = result["outputs"]
        finalize_manifest(manifest, "success", stats=result["stats"])
        return 0
    except Exception as e:
        _LOG.exception(f"Funding ingestion failed: {e}")
        finalize_manifest(manifest, "failed", error=str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
