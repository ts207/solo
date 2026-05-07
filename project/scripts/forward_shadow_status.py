from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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
DEFAULT_REPORT_DIR = Path("data/reports/detectors/no_liquidations_v1")
MIN_FORWARD_EVENTS = 100
MIN_PASSED_LIQUIDITY_EVENTS = 80
MIN_COLLECTION_DAYS = 30.0


def _parse_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _read_book(repo_root: Path, symbol: str) -> pd.DataFrame:
    root = repo_root / "data" / "lake" / "raw" / "bybit" / "perp" / symbol / "book_ticker"
    files = sorted(root.glob("year=*/month=*/*.parquet"))
    frames: list[pd.DataFrame] = []
    for path in files:
        try:
            frames.append(pd.read_parquet(path))
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    return out.dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates("timestamp")


def _load_shadow(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "event_count": 0,
            "passed_liquidity_event_count": 0,
            "reject_reason_counts": {},
            "missing": True,
        }
    payload = json.loads(path.read_text())
    payload["missing"] = False
    return payload


def build_forward_shadow_status(
    *,
    repo_root: Path,
    symbols: list[str],
    shadow_report: Path,
    now: pd.Timestamp | None = None,
) -> dict[str, Any]:
    reference = now or pd.Timestamp(datetime.now(timezone.utc))
    symbol_rows: dict[str, Any] = {}
    starts: list[pd.Timestamp] = []
    latest: list[pd.Timestamp] = []
    for symbol in symbols:
        book = _read_book(repo_root, symbol)
        if book.empty:
            symbol_rows[symbol] = {
                "book_snapshots": 0,
                "collection_start_ts": None,
                "latest_snapshot_ts": None,
                "latest_snapshot_age_seconds": None,
            }
            continue
        start_ts = pd.to_datetime(book["timestamp"], utc=True).min()
        latest_ts = pd.to_datetime(book["timestamp"], utc=True).max()
        starts.append(start_ts)
        latest.append(latest_ts)
        symbol_rows[symbol] = {
            "book_snapshots": int(len(book)),
            "collection_start_ts": start_ts.isoformat(),
            "latest_snapshot_ts": latest_ts.isoformat(),
            "latest_snapshot_age_seconds": float((reference - latest_ts).total_seconds()),
        }
    shadow = _load_shadow(repo_root / shadow_report)
    event_count = int(shadow.get("event_count") or 0)
    passed_liquidity = int(shadow.get("passed_liquidity_event_count") or 0)
    collection_start = min(starts) if starts else None
    collection_end = max(latest) if latest else None
    days_since_collection_start = (
        float((reference - collection_start).total_seconds() / 86400.0) if collection_start is not None else 0.0
    )
    reasons: list[str] = []
    if event_count < MIN_FORWARD_EVENTS:
        reasons.append("insufficient_forward_events")
    if passed_liquidity < MIN_PASSED_LIQUIDITY_EVENTS:
        reasons.append("insufficient_liquidity_passed_events")
    if days_since_collection_start < MIN_COLLECTION_DAYS:
        reasons.append("insufficient_collection_days")
    if shadow.get("missing"):
        reasons.append("missing_forward_shadow_report")
    return {
        "generated_at_utc": reference.isoformat(),
        "symbols": symbol_rows,
        "book_snapshot_total": int(sum(row["book_snapshots"] for row in symbol_rows.values())),
        "book_collection_start_ts": collection_start.isoformat() if collection_start is not None else None,
        "book_collection_end_ts": collection_end.isoformat() if collection_end is not None else None,
        "days_since_collection_start": days_since_collection_start,
        "forward_shadow_event_count": event_count,
        "passed_liquidity_event_count": passed_liquidity,
        "reject_reason_counts": shadow.get("reject_reason_counts") or {},
        "approval_eligibility": False,
        "reason": reasons[0] if reasons else "approval_review_required",
        "reasons": reasons,
        "paper_approved_events": [],
        "live_approved_events": [],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Summarize forward book collection and OI-flush shadow status")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--shadow-report", default=str(DEFAULT_REPORT_DIR / "oi_flush_forward_shadow.json"))
    parser.add_argument("--json-output", default=str(DEFAULT_REPORT_DIR / "forward_shadow_status.json"))
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    status = build_forward_shadow_status(
        repo_root=repo_root,
        symbols=[item.upper() for item in _parse_csv(args.symbols)],
        shadow_report=Path(args.shadow_report),
    )
    output = repo_root / args.json_output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(status, indent=2, sort_keys=True, default=str), encoding="utf-8")
    print(
        json.dumps(
            {
                "status": "pass",
                "json_output": str(output),
                "book_snapshot_total": status["book_snapshot_total"],
                "latest_snapshot_age_seconds_by_symbol": {
                    symbol: row["latest_snapshot_age_seconds"]
                    for symbol, row in status["symbols"].items()
                },
                "forward_shadow_event_count": status["forward_shadow_event_count"],
                "passed_liquidity_event_count": status["passed_liquidity_event_count"],
                "days_since_collection_start": status["days_since_collection_start"],
                "approval_eligibility": status["approval_eligibility"],
                "reason": status["reason"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
