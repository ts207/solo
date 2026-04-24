"""
Generic Binance USD‑M OHLCV ingestion for arbitrary minute timeframes.

This script downloads zipped OHLCV archives from the Binance
``data.binance.vision`` endpoint for USD‑M perpetual futures and writes
them to partitioned Parquet files.  Unlike the legacy ``ingest_binance_um_ohlcv_1m.py``
and ``ingest_binance_um_ohlcv_5m.py`` scripts, this implementation
accepts a ``--timeframe`` argument so that any minute‑based interval
("1m", "5m", "15m", etc.) can be ingested without duplicating
code.  The script assumes that the archive naming convention is
``{symbol}-{timeframe}-{year}-{month:02d}.zip`` and that the
corresponding Parquet files should live under
``<out_root>/<symbol>/ohlcv_<timeframe>/year=<year>/month=<month>``.

Usage example::

    python ingest_binance_um_ohlcv.py \
        --run_id my_run \
        --symbols BTCUSDT,ETHUSDT \
        --start 2024-01-01 \
        --end 2024-02-01 \
        --timeframe 5m

Notes
-----
* Only minute‑granularity timeframes are supported.  An error will
  be raised if you pass a timeframe such as ``1h`` or ``4h``.
* The script reuses helper functions from the existing 1m ingestion
  implementation (e.g. ``_read_ohlcv_from_zip`` and ``_partition_complete``)
  to maintain consistency.
* ``_expected_bars`` computes the number of expected bars based on
  the difference between ``start`` and ``end_exclusive`` divided by
  the bar duration.  The end timestamp is exclusive to avoid
  double‑counting the last bar of the range.
* Refer to ``docs/03_OPERATOR_WORKFLOW.md`` and ``docs/04_COMMANDS_AND_ENTRY_POINTS.md``
  for the maintained description of the ingestion stage in the broader research pipeline.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional
from zipfile import ZipFile

import aiohttp
import pandas as pd

from project.core.config import get_data_root
from project.core.validation import ensure_utc_timestamp, filter_ohlcv_geometry_violations
from project.io.url_utils import join_url
from project.io.utils import ensure_dir, read_parquet, write_parquet
from project.specs.manifest import finalize_manifest, start_manifest

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


def _minutes_per_bar(timeframe: str) -> int:
    """Return the number of minutes corresponding to a Binance timeframe."""
    tf = timeframe.strip().lower()
    if tf == "1h":
        return 60
    if tf == "4h":
        return 240
    if tf == "1d":
        return 1440
    if not tf.endswith("m"):
        raise ValueError(f"Unsupported timeframe format: {timeframe}")
    try:
        minutes = int(tf[:-1])
    except Exception as exc:
        raise ValueError(f"Invalid timeframe format: {timeframe}") from exc
    return minutes


def _expected_bars(start: datetime, end_exclusive: datetime, timeframe: str) -> int:
    """Compute the expected number of bars between two timestamps.

    Parameters
    ----------
    start : datetime
        Inclusive start timestamp.
    end_exclusive : datetime
        Exclusive end timestamp.  Bars with timestamps >= ``end_exclusive``
        will not be included.
    timeframe : str
        Minute‑based timeframe string (e.g. ``"1m"`` or ``"5m"``).
    """
    if end_exclusive <= start:
        return 0
    minutes = _minutes_per_bar(timeframe)
    total_seconds = (end_exclusive - start).total_seconds()
    return int(total_seconds // (minutes * 60))


def _read_ohlcv_from_zip(path: Path, symbol: str, source: str) -> pd.DataFrame:
    """Read OHLCV data from a Binance ZIP archive into a DataFrame.

    The CSV inside the zip is expected to contain the standard Binance
    kline schema: ``open_time``, ``open``, ``high``, ``low``, ``close``,
    ``volume``, ``close_time``, ``quote_volume``, ``trade_count``,
    ``taker_base_volume``, ``taker_quote_volume``, ``ignore``.

    Returns a DataFrame with columns ``timestamp``, ``open``, ``high``,
    ``low``, ``close``, ``volume``, ``quote_volume``, ``taker_base_volume``,
    ``symbol`` and ``source``.
    """
    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trade_count",
        "taker_base_volume",
        "taker_quote_volume",
        "ignore",
    ]
    with ZipFile(path) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            df = pd.read_csv(f, header=None)

    if df.empty:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume", "symbol", "source"]
        )

    usable_cols = min(len(columns), df.shape[1])
    df = df.iloc[:, :usable_cols].copy()
    df.columns = columns[:usable_cols]

    if "open_time" not in df.columns:
        raise ValueError(f"Unexpected OHLCV archive schema in {path}: missing open_time column")

    df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce")
    df = df[df["open_time"].notna()].copy()
    if df.empty:
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume", "symbol", "source"]
        )

    df["timestamp"] = pd.to_datetime(df["open_time"].astype("int64"), unit="ms", utc=True)
    df = df[
        ["timestamp", "open", "high", "low", "close", "volume", "quote_volume", "taker_base_volume"]
    ].copy()
    for col in ["open", "high", "low", "close", "volume", "quote_volume", "taker_base_volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(
        subset=["open", "high", "low", "close", "volume", "quote_volume", "taker_base_volume"]
    ).copy()
    df["symbol"] = symbol
    df["source"] = source
    ensure_utc_timestamp(df["timestamp"], "timestamp")
    df, dropped = filter_ohlcv_geometry_violations(df, label=str(path))
    if dropped:
        logging.warning(
            "Dropped %d row(s) with OHLCV geometry violations from %s", dropped, path
        )
    return df


def _partition_complete(path: Path, expected_rows: int) -> bool:
    """Return ``True`` if the Parquet file contains at least ``expected_rows`` rows."""
    if not path.exists():
        csv_path = path.with_suffix(".csv")
        if csv_path.exists():
            path = csv_path
        else:
            return False
    try:
        if expected_rows == 0:
            return True
        data = read_parquet([path])
        return len(data) >= expected_rows
    except Exception:
        return False


def _symbol_month_results_are_complete(
    results: List[Dict[str, object]], *, symbol: str
) -> tuple[bool, str | None]:
    statuses = [str(res.get("status", "")).strip().lower() for res in results]
    required_statuses = [status for status in statuses if status != "noop"]
    if not required_statuses:
        return True, None
    if any(status in {"written", "skipped"} for status in required_statuses):
        return True, None
    return (
        False,
        f"{symbol}: no required OHLCV partitions were written "
        f"(statuses={sorted(set(required_statuses))})",
    )


async def _download_archive(
    session: aiohttp.ClientSession, url: str, max_retries: int, backoff_sec: float
) -> Optional[bytes]:
    """
    Download a ZIP archive from the given URL using aiohttp.

    Returns the raw bytes of the response if successful, ``None`` if the resource
    does not exist (HTTP 404), and raises an exception on persistent failure.
    Retries are attempted with exponential backoff on non‑404 errors.
    """
    attempt = 0
    while True:
        try:
            async with session.get(url) as resp:
                # Treat 404 as missing data and return None
                if resp.status == 404:
                    return None
                if resp.status != 200:
                    # On other errors retry with backoff
                    raise aiohttp.ClientResponseError(
                        request_info=resp.request_info,
                        history=resp.history,
                        status=resp.status,
                        message=f"Unexpected status {resp.status}",
                        headers=resp.headers,
                    )
                return await resp.read()
        except aiohttp.ClientError:
            if attempt >= max_retries:
                raise
            await asyncio.sleep(backoff_sec * (2**attempt))
            attempt += 1


async def _process_month(
    semaphore: asyncio.Semaphore,
    session: aiohttp.ClientSession,
    symbol: str,
    timeframe: str,
    month_start: datetime,
    effective_start: datetime,
    effective_end: datetime,
    out_root: Path,
    max_retries: int,
    backoff_sec: float,
    force: bool,
) -> Dict[str, object]:
    """Asynchronous helper to download and process a single month of OHLCV data.

    This function is designed to run concurrently for multiple months.  It respects
    the provided semaphore to bound concurrent downloads.

    Returns a dictionary summarising the result for this month, including
    whether the archive was missing, skipped, or written, and the number of
    bars written.
    """
    async with semaphore:
        month_end = _next_month(month_start)
        range_start = max(effective_start, month_start)
        range_end_exclusive = min(effective_end + timedelta(days=1), month_end)
        expected_rows = _expected_bars(range_start, range_end_exclusive, timeframe)
        if expected_rows == 0:
            return {"status": "noop", "bars": 0}

        out_dir = (
            out_root
            / symbol
            / f"ohlcv_{timeframe}"
            / f"year={month_start.year}"
            / f"month={month_start.month:02d}"
        )
        out_path = (
            out_dir
            / f"ohlcv_{symbol}_{timeframe}_{month_start.year}-{month_start.month:02d}.parquet"
        )

        # Skip if the partition already exists with expected rows unless force is set
        if not force and _partition_complete(out_path, expected_rows):
            return {"status": "skipped", "partition": str(out_path), "bars": 0}

        monthly_url = join_url(
            ARCHIVE_BASE,
            "monthly",
            "klines",
            symbol,
            timeframe,
            f"{symbol}-{timeframe}-{month_start.year}-{month_start.month:02d}.zip",
        )
        logging.info("Downloading monthly archive %s", monthly_url)
        try:
            data_bytes = await _download_archive(session, monthly_url, max_retries, backoff_sec)
        except Exception as exc:
            logging.error("Failed to download %s: %s", monthly_url, exc)
            return {"status": "failed", "archive": monthly_url, "bars": 0}
        if data_bytes is None:
            logging.warning("Archive not found: %s", monthly_url)
            return {"status": "not_found", "archive": monthly_url, "bars": 0}

        # Write to temporary zip file and then extract
        temp_zip_path = out_path.with_suffix(".zip.tmp")
        ensure_dir(temp_zip_path.parent)
        try:
            with open(temp_zip_path, "wb") as tmpf:
                tmpf.write(data_bytes)
            df = _read_ohlcv_from_zip(temp_zip_path, symbol, source=f"archive_{timeframe}")
        except Exception as exc:
            logging.error("Failed to extract zip for %s: %s", monthly_url, exc)
            return {"status": "failed", "archive": monthly_url, "bars": 0}
        finally:
            try:
                temp_zip_path.unlink()
            except Exception:
                pass

        if df.empty:
            logging.warning("No data in archive %s", monthly_url)
            return {"status": "empty", "archive": monthly_url, "bars": 0}

        ensure_dir(out_dir)
        write_parquet(df, out_path)
        bars_written = len(df)
        if bars_written < expected_rows:
            logging.warning(
                "Bar count shortfall for %s: expected=%d actual=%d (%.1f%% missing) archive=%s",
                symbol,
                expected_rows,
                bars_written,
                (expected_rows - bars_written) / expected_rows * 100,
                monthly_url,
            )
        return {"status": "written", "partition": str(out_path), "bars": bars_written}


async def async_main(args: argparse.Namespace) -> Dict[str, object]:
    """
    Entrypoint for asynchronous ingestion.

    This function orchestrates ingestion across multiple symbols and months
    concurrently.  It returns a stats dictionary summarising the run.
    """
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    requested_start = _parse_date(args.start)
    requested_end = _parse_date(args.end)
    effective_start = max(requested_start, EARLIEST_UM_FUTURES)
    effective_end = requested_end
    timeframe = args.timeframe.strip().lower()
    # Validate timeframe
    _minutes_per_bar(timeframe)
    out_root = Path(args.out_root)
    concurrency = args.concurrency

    stats: Dict[str, object] = {"symbols": {}}
    outputs: List[Dict[str, object]] = []
    failures: List[str] = []
    semaphore = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession() as session:
        for symbol in symbols:
            tasks: List[asyncio.Task] = []
            for month_start in _iter_months(effective_start, effective_end):
                tasks.append(
                    asyncio.create_task(
                        _process_month(
                            semaphore,
                            session,
                            symbol,
                            timeframe,
                            month_start,
                            effective_start,
                            effective_end,
                            out_root,
                            args.max_retries,
                            args.retry_backoff_sec,
                            bool(args.force),
                        )
                    )
                )
            missing_archives: List[str] = []
            partitions_written: List[str] = []
            partitions_skipped: List[str] = []
            bars_written_total = 0
            results = await asyncio.gather(*tasks)
            for res in results:
                status = res.get("status")
                if status in {"not_found"}:
                    missing_archives.append(res.get("archive"))
                elif status == "failed":
                    missing_archives.append(res.get("archive"))
                elif status == "written":
                    partitions_written.append(res.get("partition"))
                    bars_written_total += res.get("bars", 0)
                    if res.get("partition"):
                        outputs.append(
                            {
                                "path": str(res.get("partition")),
                                "rows": int(res.get("bars", 0) or 0),
                                "storage": "parquet",
                            }
                        )
                elif status == "skipped":
                    partitions_skipped.append(res.get("partition"))
                    if res.get("partition"):
                        outputs.append(
                            {
                                "path": str(res.get("partition")),
                                "rows": int(res.get("bars", 0) or 0),
                                "storage": "parquet",
                            }
                        )
                # noop and empty statuses contribute nothing
            stats["symbols"][symbol] = {
                "missing_archives": missing_archives,
                "partitions_written": partitions_written,
                "partitions_skipped": partitions_skipped,
                "bars_written_total": bars_written_total,
            }
            complete, reason = _symbol_month_results_are_complete(results, symbol=symbol)
            if not complete and reason:
                failures.append(reason)
    return {"stats": stats, "outputs": outputs, "failures": failures}


def main() -> int:
    """CLI entrypoint for asynchronous Binance OHLCV ingestion."""
    data_root = get_data_root()
    parser = argparse.ArgumentParser(
        description="Ingest Binance USD‑M OHLCV data for a configurable minute timeframe"
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument(
        "--timeframe",
        default="1m",
        help="Minute timeframe to ingest (e.g. 1m, 5m, 15m). Only minute‑based intervals are allowed.",
    )
    parser.add_argument(
        "--out_root",
        default=str(data_root / "lake" / "raw" / "binance" / "perp"),
        help="Base output directory for Parquet files",
    )
    parser.add_argument("--max_retries", type=int, default=5)
    parser.add_argument("--retry_backoff_sec", type=float, default=2.0)
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--log_path", default=None)
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Maximum number of concurrent archive downloads",
    )
    args = parser.parse_args()

    # Configure logging
    log_handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(
        level=logging.INFO,
        handlers=log_handlers,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Manifest recording
    run_id = args.run_id
    params: Dict[str, object] = {
        "symbols": args.symbols.split(","),
        "requested_start": args.start,
        "requested_end": args.end,
        "out_root": args.out_root,
        "max_retries": args.max_retries,
        "retry_backoff_sec": args.retry_backoff_sec,
        "force": int(args.force),
        "timeframe": args.timeframe,
        "concurrency": args.concurrency,
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest(
        f"ingest_binance_um_ohlcv_{args.timeframe}", run_id, params, inputs, outputs
    )

    result: Dict[str, object] = {"stats": {"symbols": {}}, "outputs": [], "failures": []}
    try:
        result = asyncio.run(async_main(args))
        failures = list(result.get("failures", []))
        if failures:
            raise RuntimeError("; ".join(str(item) for item in failures))
        manifest["outputs"] = list(result.get("outputs", []))
        finalize_manifest(manifest, "success", stats=result.get("stats", {"symbols": {}}))
        return 0
    except Exception as exc:
        logging.exception("Unexpected error during ingestion: %s", exc)
        finalize_manifest(
            manifest,
            "failed",
            error=str(exc),
            stats=result.get("stats", {"symbols": {}}),
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
