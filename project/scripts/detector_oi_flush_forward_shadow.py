from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.scripts.detector_oi_flush_lab import (
    DEFAULT_SYMBOLS,
    _base_oi_flush_mask,
    _prepare_oi_flush_frame,
)


DEFAULT_REPORT_DIR = Path("data/reports/detectors/no_liquidations_v1")
DEFAULT_PRICE_PCT = 95.0
DEFAULT_OI_DROP_PCT = 97.5
DEFAULT_BOOK_MAX_AGE_SECONDS = 60.0
DEFAULT_DEPTH_USD_BY_SYMBOL = {
    "BTCUSDT": 25_000.0,
    "ETHUSDT": 20_000.0,
    "SOLUSDT": 15_000.0,
    "BNBUSDT": 10_000.0,
    "XRPUSDT": 10_000.0,
    "LINKUSDT": 7_500.0,
    "AVAXUSDT": 7_500.0,
    "ADAUSDT": 7_500.0,
    "DOGEUSDT": 7_500.0,
    "LTCUSDT": 7_500.0,
}
DEFAULT_SPREAD_CAP_BPS_BY_SYMBOL = {
    "BTCUSDT": 2.0,
    "ETHUSDT": 3.0,
    "SOLUSDT": 5.0,
    "BNBUSDT": 5.0,
    "XRPUSDT": 6.0,
    "LINKUSDT": 8.0,
    "AVAXUSDT": 8.0,
    "ADAUSDT": 8.0,
    "DOGEUSDT": 8.0,
    "LTCUSDT": 8.0,
}


def _parse_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _parse_ints(value: str) -> list[int]:
    return [int(item) for item in _parse_csv(value)]


def _read_many(paths: list[Path]) -> pd.DataFrame:
    frames = [pd.read_parquet(path) for path in paths if path.exists()]
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    return out.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates("timestamp")


def _book_files(repo_root: Path, symbol: str, years: list[int]) -> list[Path]:
    files: list[Path] = []
    for year in years:
        files.extend(
            sorted(
                (
                    repo_root
                    / "data"
                    / "lake"
                    / "raw"
                    / "bybit"
                    / "perp"
                    / symbol
                    / "book_ticker"
                    / f"year={year}"
                ).glob("month=*/book_ticker_*.parquet")
            )
        )
    return files


def _load_book(repo_root: Path, symbol: str, years: list[int]) -> pd.DataFrame:
    book = _read_many(_book_files(repo_root, symbol, years))
    if book.empty:
        return book
    rename = {
        "bid_price": "best_bid",
        "ask_price": "best_ask",
        "best_bid_price": "best_bid",
        "best_ask_price": "best_ask",
    }
    book = book.rename(columns={k: v for k, v in rename.items() if k in book.columns and v not in book.columns})
    return book


def _direction_mult(direction: str) -> float:
    return 1.0 if direction == "long" else -1.0


def _event_masks(frame: pd.DataFrame, price_pct: float, oi_drop_pct: float) -> list[dict[str, Any]]:
    down = _base_oi_flush_mask(frame, price_side="down", price_pct=price_pct, oi_pct=oi_drop_pct)
    up = _base_oi_flush_mask(frame, price_side="up", price_pct=price_pct, oi_pct=oi_drop_pct)
    return [
        {
            "event_id": "OI_FLUSH_DOWN_REVERSAL",
            "direction": "long",
            "mask": down & frame["failed_breakdown_reclaim_24"].fillna(False),
        },
        {
            "event_id": "OI_FLUSH_UP_REVERSAL",
            "direction": "short",
            "mask": up & frame["failed_breakout_rejection_24"].fillna(False),
        },
        {
            "event_id": "OI_FLUSH_DOWN_CONTINUATION",
            "direction": "short",
            "mask": down & frame["close_near_low"].fillna(False) & ~frame["failed_breakdown_reclaim_24"].fillna(False),
        },
        {
            "event_id": "OI_FLUSH_UP_CONTINUATION",
            "direction": "long",
            "mask": up & frame["close_near_high"].fillna(False) & ~frame["failed_breakout_rejection_24"].fillna(False),
        },
    ]


def _nearest_book(events: pd.DataFrame, book: pd.DataFrame) -> pd.DataFrame:
    if events.empty or book.empty:
        return events.assign(
            nearest_book_ts=pd.NaT,
            book_age_seconds=np.nan,
            spread_at_entry_bps=np.nan,
            depth_at_entry_usd=np.nan,
        )
    book_cols = ["timestamp", "spread_bps", "depth_usd", "best_bid", "best_ask"]
    for column in book_cols:
        if column not in book.columns:
            book[column] = np.nan
    joined = pd.merge_asof(
        events.sort_values("event_ts"),
        book[book_cols].sort_values("timestamp").rename(columns={"timestamp": "nearest_book_ts"}),
        left_on="event_ts",
        right_on="nearest_book_ts",
        direction="backward",
    )
    joined["book_age_seconds"] = (
        joined["event_ts"] - joined["nearest_book_ts"]
    ).dt.total_seconds()
    joined = joined.rename(
        columns={"spread_bps": "spread_at_entry_bps", "depth_usd": "depth_at_entry_usd"}
    )
    return joined


def _forward_return(frame: pd.DataFrame, idx: int, direction: str, horizon_bars: int) -> float | None:
    end_idx = idx + horizon_bars
    if end_idx >= len(frame):
        return None
    entry = float(frame.iloc[idx]["close"])
    exit_price = float(frame.iloc[end_idx]["close"])
    if entry <= 0.0 or not math.isfinite(entry) or not math.isfinite(exit_price):
        return None
    return ((exit_price / entry) - 1.0) * 10_000.0 * _direction_mult(direction)


def _next_bar_gap(frame: pd.DataFrame, idx: int, direction: str) -> float | None:
    if idx + 1 >= len(frame):
        return None
    entry = float(frame.iloc[idx]["close"])
    next_open = float(frame.iloc[idx + 1]["open"])
    if entry <= 0.0 or not math.isfinite(entry) or not math.isfinite(next_open):
        return None
    return ((next_open / entry) - 1.0) * 10_000.0 * _direction_mult(direction)


def _status_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    return dict(Counter(str(row.get("reject_reason") or "passed_liquidity") for row in rows))


def _evaluate_symbol(
    repo_root: Path,
    symbol: str,
    years: list[int],
    price_pct: float,
    oi_drop_pct: float,
    horizon_bars: int,
    max_book_age_seconds: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    frame = _prepare_oi_flush_frame(repo_root, symbol, years)
    book = _load_book(repo_root, symbol, years)
    book_start = pd.to_datetime(book["timestamp"], utc=True).min() if not book.empty else pd.NaT
    events: list[dict[str, Any]] = []
    for spec in _event_masks(frame, price_pct, oi_drop_pct):
        raw_indices = np.flatnonzero(spec["mask"].fillna(False).to_numpy())
        if pd.notna(book_start):
            raw_indices = np.asarray(
                [idx for idx in raw_indices if frame.iloc[int(idx)]["timestamp"] >= book_start],
                dtype=int,
            )
        for idx in raw_indices:
            event_ts = pd.to_datetime(frame.iloc[int(idx)]["timestamp"], utc=True)
            gross = _forward_return(frame, int(idx), str(spec["direction"]), horizon_bars)
            gap = _next_bar_gap(frame, int(idx), str(spec["direction"]))
            events.append(
                {
                    "symbol": symbol,
                    "event_id": spec["event_id"],
                    "direction": spec["direction"],
                    "event_ts": event_ts,
                    "bar_index": int(idx),
                    "forward_gross_bps": gross,
                    "next_bar_gap_bps": gap,
                }
            )
    event_frame = pd.DataFrame(events)
    joined = _nearest_book(event_frame, book)
    rows: list[dict[str, Any]] = []
    spread_cap = float(DEFAULT_SPREAD_CAP_BPS_BY_SYMBOL.get(symbol, 8.0))
    depth_floor = float(DEFAULT_DEPTH_USD_BY_SYMBOL.get(symbol, 7_500.0))
    for _, row in joined.iterrows():
        spread = row.get("spread_at_entry_bps")
        depth = row.get("depth_at_entry_usd")
        age = row.get("book_age_seconds")
        reject_reason = None
        if age is None or not math.isfinite(float(age)) or float(age) > max_book_age_seconds:
            reject_reason = "stale_or_missing_book"
        elif spread is None or not math.isfinite(float(spread)) or float(spread) > spread_cap:
            reject_reason = "spread_filter_failed"
        elif depth is None or not math.isfinite(float(depth)) or float(depth) < depth_floor:
            reject_reason = "depth_filter_failed"
        gross = row.get("forward_gross_bps")
        gap = row.get("next_bar_gap_bps")
        simulated_after_spread = (
            float(gross) - float(spread)
            if gross is not None and spread is not None and math.isfinite(float(gross)) and math.isfinite(float(spread))
            else None
        )
        simulated_after_spread_and_gap = (
            simulated_after_spread + min(0.0, float(gap))
            if simulated_after_spread is not None and gap is not None and math.isfinite(float(gap))
            else None
        )
        rows.append(
            {
                "symbol": symbol,
                "event_id": row.get("event_id"),
                "direction": row.get("direction"),
                "event_ts": pd.to_datetime(row.get("event_ts"), utc=True).isoformat(),
                "nearest_book_ts": (
                    pd.to_datetime(row.get("nearest_book_ts"), utc=True).isoformat()
                    if pd.notna(row.get("nearest_book_ts"))
                    else None
                ),
                "book_age_seconds": None if pd.isna(age) else float(age),
                "spread_at_entry_bps": None if pd.isna(spread) else float(spread),
                "depth_at_entry_usd": None if pd.isna(depth) else float(depth),
                "entry_passed_liquidity_filter": reject_reason is None,
                "reject_reason": reject_reason,
                "forward_gross_bps": None if pd.isna(gross) else float(gross),
                "next_bar_gap_bps": None if pd.isna(gap) else float(gap),
                "simulated_net_after_spread": simulated_after_spread,
                "simulated_net_after_spread_and_gap": simulated_after_spread_and_gap,
            }
        )
    meta = {
        "book_rows": int(len(book)),
        "book_collection_start_ts": (
            pd.to_datetime(book["timestamp"], utc=True).min().isoformat() if not book.empty else None
        ),
        "book_collection_end_ts": (
            pd.to_datetime(book["timestamp"], utc=True).max().isoformat() if not book.empty else None
        ),
        "raw_forward_event_count": int(len(events)),
        "joined_event_count": int(len(rows)),
    }
    return rows, meta


def build_forward_shadow_report(
    *,
    repo_root: Path,
    symbols: list[str],
    years: list[int],
    price_pct: float,
    oi_drop_pct: float,
    horizon_bars: int,
    max_book_age_seconds: float,
    json_output: Path,
    csv_output: Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    all_rows: list[dict[str, Any]] = []
    symbol_meta: dict[str, Any] = {}
    missing: dict[str, str] = {}
    for symbol in symbols:
        try:
            rows, meta = _evaluate_symbol(
                repo_root,
                symbol,
                years,
                price_pct,
                oi_drop_pct,
                horizon_bars,
                max_book_age_seconds,
            )
        except Exception as exc:
            missing[symbol] = str(exc)
            continue
        all_rows.extend(rows)
        symbol_meta[symbol] = meta
    passed = [row for row in all_rows if row["entry_passed_liquidity_filter"]]
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "symbols": symbols,
            "years": years,
            "price_pct": price_pct,
            "oi_drop_pct": oi_drop_pct,
            "horizon_bars": horizon_bars,
            "max_book_age_seconds": max_book_age_seconds,
            "approval_policy": "forward_shadow_only_no_paper_or_live_approval",
        },
        "symbol_meta": symbol_meta,
        "missing_symbols": missing,
        "event_count": len(all_rows),
        "passed_liquidity_event_count": len(passed),
        "reject_reason_counts": _status_counts(all_rows),
        "events": all_rows,
        "paper_approved_events": [],
        "live_approved_events": [],
    }
    csv = pd.DataFrame(all_rows)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    csv_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    csv.to_csv(csv_output, index=False)
    return report, csv


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Forward-only OI flush shadow validation with observed book snapshots")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=str(datetime.now(timezone.utc).year))
    parser.add_argument("--price-pct", type=float, default=DEFAULT_PRICE_PCT)
    parser.add_argument("--oi-drop-pct", type=float, default=DEFAULT_OI_DROP_PCT)
    parser.add_argument("--horizon-bars", type=int, default=96)
    parser.add_argument("--max-book-age-seconds", type=float, default=DEFAULT_BOOK_MAX_AGE_SECONDS)
    parser.add_argument("--json-output", default=str(DEFAULT_REPORT_DIR / "oi_flush_forward_shadow.json"))
    parser.add_argument("--csv-output", default=str(DEFAULT_REPORT_DIR / "oi_flush_forward_shadow.csv"))
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    report, _ = build_forward_shadow_report(
        repo_root=repo_root,
        symbols=[item.upper() for item in _parse_csv(args.symbols)],
        years=_parse_ints(args.years),
        price_pct=float(args.price_pct),
        oi_drop_pct=float(args.oi_drop_pct),
        horizon_bars=int(args.horizon_bars),
        max_book_age_seconds=float(args.max_book_age_seconds),
        json_output=repo_root / args.json_output,
        csv_output=repo_root / args.csv_output,
    )
    print(
        json.dumps(
            {
                "status": "pass",
                "json_output": str(repo_root / args.json_output),
                "csv_output": str(repo_root / args.csv_output),
                "event_count": report["event_count"],
                "passed_liquidity_event_count": report["passed_liquidity_event_count"],
                "reject_reason_counts": report["reject_reason_counts"],
                "paper_approved_events": report["paper_approved_events"],
                "live_approved_events": report["live_approved_events"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
