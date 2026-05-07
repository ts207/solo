from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DEFAULT_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "LINKUSDT",
    "AVAXUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "LTCUSDT",
)
DEFAULT_YEARS = (2022, 2023, 2024, 2025)
DEFAULT_REPORT_DIR = Path("data/reports/detectors/no_liquidations_v1")

BEST_BID_COLUMNS = ("best_bid", "best_bid_price", "bid", "bid_price")
BEST_ASK_COLUMNS = ("best_ask", "best_ask_price", "ask", "ask_price")
SPREAD_COLUMNS = ("spread_bps",)
DEPTH_COLUMNS = ("depth_usd",)
KNOWN_FEEDS = ("ohlcv_5m", "open_interest", "funding")


def _parse_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _parse_ints(value: str) -> list[int]:
    return [int(item) for item in _parse_csv(value)]


def _read_many(paths: list[Path]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in paths:
        try:
            frames.append(pd.read_parquet(path))
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    if "timestamp" in out.columns:
        out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
        out = out.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates("timestamp")
    return out.reset_index(drop=True)


def _feed_files(repo_root: Path, symbol: str, feed: str, years: list[int]) -> list[Path]:
    base = repo_root / "data" / "lake" / "raw" / "bybit" / "perp" / symbol / feed
    if feed == "ohlcv_5m":
        patterns = [
            str(base / f"year={year}" / "month=*" / f"ohlcv_{symbol}_5m_{year}-*.parquet")
            for year in years
        ]
    elif feed == "open_interest":
        patterns = [
            str(base / f"year={year}" / "month=*" / f"oi_{symbol}_{year}-*.parquet")
            for year in years
        ]
    elif feed == "funding":
        patterns = [
            str(base / f"year={year}" / "month=*" / f"funding_{symbol}_{year}-*.parquet")
            for year in years
        ]
    else:
        patterns = [str(base / f"year={year}" / "month=*" / "*.parquet") for year in years]
    files: list[Path] = []
    for pattern in patterns:
        files.extend(Path(path) for path in sorted(glob.glob(pattern)))
    return files


def _unknown_files(repo_root: Path, symbol: str, years: list[int]) -> list[Path]:
    symbol_root = repo_root / "data" / "lake" / "raw" / "bybit" / "perp" / symbol
    if not symbol_root.exists():
        return []
    files: list[Path] = []
    for path in sorted(symbol_root.glob("**/*.parquet")):
        parts = set(path.parts)
        if parts.intersection(KNOWN_FEEDS):
            continue
        if not any(f"year={year}" in parts for year in years):
            continue
        files.append(path)
    return files


def _columns_present(frames: list[pd.DataFrame], aliases: tuple[str, ...]) -> bool:
    for frame in frames:
        if not frame.empty and any(column in frame.columns for column in aliases):
            return True
    return False


def _first_column(frame: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    for column in aliases:
        if column in frame.columns:
            return column
    return None


def _range(frame: pd.DataFrame) -> dict[str, str | None]:
    if frame.empty or "timestamp" not in frame.columns:
        return {"start": None, "end": None}
    timestamps = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce").dropna()
    if timestamps.empty:
        return {"start": None, "end": None}
    return {"start": timestamps.min().isoformat(), "end": timestamps.max().isoformat()}


def _median_interval_minutes(frame: pd.DataFrame) -> float | None:
    if frame.empty or "timestamp" not in frame.columns:
        return None
    deltas = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce").dropna().sort_values().diff().dropna()
    if deltas.empty:
        return None
    return float(deltas.dt.total_seconds().median() / 60.0)


def _pct_missing(series: pd.Series) -> float | None:
    if len(series) == 0:
        return None
    return float(series.isna().mean() * 100.0)


def _alignment_and_missing(
    bars: pd.DataFrame,
    oi: pd.DataFrame,
    funding: pd.DataFrame,
    book: pd.DataFrame,
) -> tuple[bool, dict[str, Any], dict[str, Any]]:
    if bars.empty or "timestamp" not in bars.columns:
        return False, {}, {}
    base = bars[["timestamp"]].copy().sort_values("timestamp")
    base["month"] = base["timestamp"].dt.strftime("%Y-%m")
    aligned = base.copy()
    if not oi.empty and {"timestamp", "open_interest"}.issubset(oi.columns):
        aligned = aligned.merge(
            oi[["timestamp", "open_interest"]].sort_values("timestamp"),
            on="timestamp",
            how="left",
        )
    else:
        aligned["open_interest"] = np.nan
    if not funding.empty and {"timestamp", "funding_rate"}.issubset(funding.columns):
        funding_src = funding[["timestamp", "funding_rate"]].sort_values("timestamp").rename(
            columns={"timestamp": "funding_source_ts"}
        )
        aligned = pd.merge_asof(
            aligned.sort_values("timestamp"),
            funding_src,
            left_on="timestamp",
            right_on="funding_source_ts",
            direction="backward",
        )
        aligned["funding_staleness_hours"] = (
            aligned["timestamp"] - aligned["funding_source_ts"]
        ).dt.total_seconds() / 3600.0
        aligned.loc[aligned["funding_staleness_hours"] > 8.5, "funding_rate"] = np.nan
    else:
        aligned["funding_rate"] = np.nan
        aligned["funding_staleness_hours"] = np.nan
    for output, aliases in {
        "best_bid": BEST_BID_COLUMNS,
        "best_ask": BEST_ASK_COLUMNS,
        "spread_bps": SPREAD_COLUMNS,
        "depth_usd": DEPTH_COLUMNS,
    }.items():
        column = _first_column(book, aliases)
        if column and "timestamp" in book.columns:
            aligned = aligned.merge(
                book[["timestamp", column]].sort_values("timestamp").rename(columns={column: output}),
                on="timestamp",
                how="left",
            )
        else:
            aligned[output] = np.nan
    by_month: dict[str, Any] = {}
    for month, group in aligned.groupby("month", sort=True):
        by_month[str(month)] = {
            "ohlcv_rows": int(len(group)),
            "oi_missing_pct": _pct_missing(group["open_interest"]),
            "funding_missing_pct": _pct_missing(group["funding_rate"]),
            "best_bid_missing_pct": _pct_missing(group["best_bid"]),
            "best_ask_missing_pct": _pct_missing(group["best_ask"]),
            "spread_bps_missing_pct": _pct_missing(group["spread_bps"]),
            "depth_usd_missing_pct": _pct_missing(group["depth_usd"]),
        }
    missing = {
        "ohlcv_5m_missing_pct": 0.0 if not bars.empty else 100.0,
        "oi_5m_missing_pct": _pct_missing(aligned["open_interest"]),
        "funding_missing_pct": _pct_missing(aligned["funding_rate"]),
        "best_bid_missing_pct": _pct_missing(aligned["best_bid"]),
        "best_ask_missing_pct": _pct_missing(aligned["best_ask"]),
        "spread_bps_missing_pct": _pct_missing(aligned["spread_bps"]),
        "depth_usd_missing_pct": _pct_missing(aligned["depth_usd"]),
    }
    timestamp_alignment_ok = bool(
        missing["oi_5m_missing_pct"] is not None
        and missing["funding_missing_pct"] is not None
        and missing["oi_5m_missing_pct"] <= 1.0
        and missing["funding_missing_pct"] <= 1.0
    )
    return timestamp_alignment_ok, missing, by_month


def _audit_symbol(repo_root: Path, symbol: str, years: list[int]) -> dict[str, Any]:
    files = {feed: _feed_files(repo_root, symbol, feed, years) for feed in KNOWN_FEEDS}
    unknown = _unknown_files(repo_root, symbol, years)
    frames = {feed: _read_many(paths) for feed, paths in files.items()}
    unknown_frame = _read_many(unknown)
    all_frames = [*frames.values(), unknown_frame]
    timestamp_alignment_ok, missing_pct, by_month = _alignment_and_missing(
        frames["ohlcv_5m"],
        frames["open_interest"],
        frames["funding"],
        unknown_frame,
    )
    return {
        "symbol": symbol,
        "has_ohlcv_5m": not frames["ohlcv_5m"].empty,
        "has_oi_5m": (
            not frames["open_interest"].empty
            and _median_interval_minutes(frames["open_interest"]) is not None
            and float(_median_interval_minutes(frames["open_interest"]) or 999.0) <= 5.5
        ),
        "has_funding": not frames["funding"].empty,
        "has_best_bid": _columns_present(all_frames, BEST_BID_COLUMNS),
        "has_best_ask": _columns_present(all_frames, BEST_ASK_COLUMNS),
        "has_spread_bps": _columns_present(all_frames, SPREAD_COLUMNS),
        "has_depth_usd": _columns_present(all_frames, DEPTH_COLUMNS),
        "timestamp_alignment_ok": timestamp_alignment_ok,
        "missing_pct": missing_pct,
        "missing_pct_by_month": by_month,
        "evidence": {
            "file_counts": {feed: len(paths) for feed, paths in files.items()} | {"unknown_book_like": len(unknown)},
            "columns": {
                feed: sorted(str(column) for column in frame.columns)
                for feed, frame in frames.items()
                if not frame.empty
            }
            | ({"unknown_book_like": sorted(str(column) for column in unknown_frame.columns)} if not unknown_frame.empty else {}),
            "timestamp_ranges": {feed: _range(frame) for feed, frame in frames.items()}
            | ({"unknown_book_like": _range(unknown_frame)} if not unknown_frame.empty else {}),
            "median_interval_minutes": {
                feed: _median_interval_minutes(frame) for feed, frame in frames.items()
            },
        },
    }


def build_data_feed_audit(
    *,
    repo_root: Path,
    symbols: list[str],
    years: list[int],
    json_output: Path,
    csv_output: Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    rows = [_audit_symbol(repo_root, symbol, years) for symbol in symbols]
    summary = {
        "all_symbols_have_ohlcv_5m": all(row["has_ohlcv_5m"] for row in rows),
        "all_symbols_have_oi_5m": all(row["has_oi_5m"] for row in rows),
        "all_symbols_have_funding": all(row["has_funding"] for row in rows),
        "all_symbols_have_best_bid": all(row["has_best_bid"] for row in rows),
        "all_symbols_have_best_ask": all(row["has_best_ask"] for row in rows),
        "all_symbols_have_spread_bps": all(row["has_spread_bps"] for row in rows),
        "all_symbols_have_depth_usd": all(row["has_depth_usd"] for row in rows),
        "all_symbols_timestamp_alignment_ok": all(row["timestamp_alignment_ok"] for row in rows),
    }
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": {"symbols": symbols, "years": years, "data_root": "data/lake/raw/bybit/perp"},
        "summary": summary,
        "governance_recommendation": {
            "available_feeds": {
                "ohlcv": summary["all_symbols_have_ohlcv_5m"],
                "funding": summary["all_symbols_have_funding"],
                "open_interest": summary["all_symbols_have_oi_5m"],
                "best_bid": summary["all_symbols_have_best_bid"],
                "best_ask": summary["all_symbols_have_best_ask"],
                "spread_bps": summary["all_symbols_have_spread_bps"],
                "depth_usd": summary["all_symbols_have_depth_usd"],
                "liquidations": False,
            }
        },
        "symbols": rows,
    }
    csv = pd.DataFrame(
        [
            {
                "symbol": row["symbol"],
                "has_ohlcv_5m": row["has_ohlcv_5m"],
                "has_oi_5m": row["has_oi_5m"],
                "has_funding": row["has_funding"],
                "has_best_bid": row["has_best_bid"],
                "has_best_ask": row["has_best_ask"],
                "has_spread_bps": row["has_spread_bps"],
                "has_depth_usd": row["has_depth_usd"],
                "timestamp_alignment_ok": row["timestamp_alignment_ok"],
                **{key: value for key, value in row["missing_pct"].items()},
            }
            for row in rows
        ]
    )
    json_output.parent.mkdir(parents=True, exist_ok=True)
    csv_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    csv.to_csv(csv_output, index=False)
    return report, csv


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit historical detector feed availability and alignment")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in DEFAULT_YEARS))
    parser.add_argument("--json-output", default=str(DEFAULT_REPORT_DIR / "data_feed_audit.json"))
    parser.add_argument("--csv-output", default=str(DEFAULT_REPORT_DIR / "data_feed_audit.csv"))
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[2]
    report, _ = build_data_feed_audit(
        repo_root=repo_root,
        symbols=[item.upper() for item in _parse_csv(args.symbols)],
        years=_parse_ints(args.years),
        json_output=repo_root / args.json_output,
        csv_output=repo_root / args.csv_output,
    )
    print(
        json.dumps(
            {
                "status": "pass",
                "json_output": str(repo_root / args.json_output),
                "csv_output": str(repo_root / args.csv_output),
                "summary": report["summary"],
                "governance_recommendation": report["governance_recommendation"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
