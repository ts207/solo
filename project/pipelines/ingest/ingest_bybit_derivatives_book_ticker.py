"""
Bybit V5 derivatives book-ticker snapshot ingestion.

This collects current best bid/ask and top-of-book size from Bybit's public
tickers endpoint. It is a forward collection feed, not a historical book
backfill.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd

from project.core.config import get_data_root
from project.core.validation import ensure_utc_timestamp
from project.io.utils import ensure_dir, write_parquet
from project.specs.manifest import finalize_manifest, start_manifest

_LOG = logging.getLogger(__name__)


def _positive_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0.0:
        return None
    return parsed


async def _fetch_ticker(
    session: aiohttp.ClientSession,
    symbol: str,
    *,
    max_retries: int,
    retry_backoff_sec: float,
) -> dict[str, Any]:
    url = "https://api.bybit.com/v5/market/tickers"
    params = {"category": "linear", "symbol": symbol.upper()}
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                payload = await resp.json()
            if payload.get("retCode") != 0:
                raise RuntimeError(f"Bybit error: {payload.get('retMsg')} ({payload.get('retCode')})")
            rows = payload.get("result", {}).get("list", [])
            if not rows:
                raise RuntimeError(f"empty ticker response for {symbol}")
            row = dict(rows[0])
            row["_response_time_ms"] = payload.get("time")
            return row
        except Exception:
            if attempt >= max_retries:
                raise
            await asyncio.sleep(float(retry_backoff_sec) * (2 ** (attempt - 1)))
    raise RuntimeError(f"ticker fetch exhausted for {symbol}")


def _normalize_ticker(symbol: str, raw: dict[str, Any]) -> pd.DataFrame:
    response_time = raw.get("_response_time_ms")
    timestamp = pd.to_datetime(response_time, unit="ms", utc=True, errors="coerce")
    if pd.isna(timestamp):
        timestamp = pd.Timestamp(datetime.now(UTC))
    bid = _positive_float(raw.get("bid1Price"))
    ask = _positive_float(raw.get("ask1Price"))
    bid_qty = _positive_float(raw.get("bid1Size"))
    ask_qty = _positive_float(raw.get("ask1Size"))
    mid = ((bid + ask) / 2.0) if bid is not None and ask is not None else None
    spread_bps = ((ask - bid) / mid) * 10_000.0 if bid is not None and ask is not None and mid else None
    bid_depth_usd = bid * bid_qty if bid is not None and bid_qty is not None else None
    ask_depth_usd = ask * ask_qty if ask is not None and ask_qty is not None else None
    depth_usd = min(bid_depth_usd, ask_depth_usd) if bid_depth_usd is not None and ask_depth_usd is not None else None
    return pd.DataFrame(
        [
            {
                "timestamp": timestamp,
                "symbol": symbol.upper(),
                "best_bid": bid,
                "best_ask": ask,
                "best_bid_qty": bid_qty,
                "best_ask_qty": ask_qty,
                "spread_bps": spread_bps,
                "bid_depth_usd": bid_depth_usd,
                "ask_depth_usd": ask_depth_usd,
                "depth_usd": depth_usd,
                "source": "bybit_v5_market_tickers",
            }
        ]
    )


def _write_symbol_partition(df: pd.DataFrame, symbol: str, out_root: Path) -> tuple[Path, int]:
    if df.empty:
        raise ValueError(f"empty book ticker frame for {symbol}")
    ts = pd.to_datetime(df["timestamp"], utc=True).iloc[0]
    out_dir = out_root / symbol / "book_ticker" / f"year={ts.year}" / f"month={ts.month:02d}"
    out_path = out_dir / f"book_ticker_{symbol}_{ts.year}-{ts.month:02d}.parquet"
    ensure_dir(out_dir)
    if out_path.exists():
        existing = pd.read_parquet(out_path)
        combined = pd.concat([existing, df], ignore_index=True)
        combined["timestamp"] = pd.to_datetime(combined["timestamp"], utc=True, errors="coerce")
        combined = combined.dropna(subset=["timestamp"]).sort_values("timestamp")
        combined = combined.drop_duplicates(["timestamp", "symbol"], keep="last")
    else:
        combined = df.copy()
    ensure_utc_timestamp(combined["timestamp"], "timestamp")
    write_parquet(combined, out_path)
    return out_path, len(combined)


async def async_main(args: argparse.Namespace) -> dict[str, Any]:
    symbols = [symbol.strip().upper() for symbol in str(args.symbols).split(",") if symbol.strip()]
    out_root = Path(args.out_root)
    semaphore = asyncio.Semaphore(int(args.concurrency))
    outputs: list[dict[str, Any]] = []
    stats: dict[str, Any] = {"symbols": {}}

    async def ingest_one(symbol: str) -> tuple[str, dict[str, Any]]:
        async with semaphore:
            raw = await _fetch_ticker(
                session,
                symbol,
                max_retries=int(args.max_retries),
                retry_backoff_sec=float(args.retry_backoff_sec),
            )
            df = _normalize_ticker(symbol, raw)
            path, rows = _write_symbol_partition(df, symbol, out_root)
            return symbol, {
                "status": "written",
                "partition": str(path),
                "rows_in_partition": rows,
                "spread_bps": None if pd.isna(df["spread_bps"].iloc[0]) else float(df["spread_bps"].iloc[0]),
                "depth_usd": None if pd.isna(df["depth_usd"].iloc[0]) else float(df["depth_usd"].iloc[0]),
            }

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*(ingest_one(symbol) for symbol in symbols), return_exceptions=True)

    failed = False
    for result in results:
        if isinstance(result, Exception):
            failed = True
            key = f"error_{len(stats['symbols']) + 1}"
            stats["symbols"][key] = {"status": "failed", "error": str(result)}
            continue
        symbol, row = result
        stats["symbols"][symbol] = row
        if row["status"] == "written":
            outputs.append({"path": row["partition"], "rows": row["rows_in_partition"], "storage": "parquet"})
    if failed:
        raise RuntimeError(f"one or more Bybit book ticker ingests failed: {stats['symbols']}")
    return {"stats": stats, "outputs": outputs}


def main() -> int:
    data_root = get_data_root()
    parser = argparse.ArgumentParser(description="Ingest current Bybit derivatives book ticker snapshots")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--out_root", default=str(data_root / "lake" / "raw" / "bybit" / "perp"))
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--max_retries", type=int, default=5)
    parser.add_argument("--retry_backoff_sec", type=float, default=1.0)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    manifest = start_manifest("ingest_bybit_derivatives_book_ticker", args.run_id, vars(args), [], [])
    try:
        result = asyncio.run(async_main(args))
        manifest["outputs"] = result["outputs"]
        finalize_manifest(manifest, "success", stats=result["stats"])
        print({"status": "pass", "stats": result["stats"]})
        return 0
    except Exception as exc:
        _LOG.exception("Bybit book ticker ingestion failed")
        finalize_manifest(manifest, "failed", error=str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
