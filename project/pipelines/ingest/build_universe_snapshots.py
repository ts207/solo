from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

from project.core.config import get_data_root
from project.core.timeframes import bars_dataset_name, normalize_timeframe
from project.io.utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
    write_parquet,
)
from project.specs.manifest import finalize_manifest, start_manifest


def _load_symbol_bars(run_id: str, symbol: str, market: str, timeframe: str) -> pd.DataFrame:
    data_root = get_data_root()
    dataset = bars_dataset_name(timeframe)
    candidates = [
        run_scoped_lake_path(data_root, run_id, "cleaned", market, symbol, dataset),
        data_root / "lake" / "cleaned" / market / symbol / dataset,
    ]
    bars_dir = choose_partition_dir(candidates)
    files = list_parquet_files(bars_dir) if bars_dir else []
    if not files:
        return pd.DataFrame()
    df = read_parquet(files)
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    return df.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)


def main() -> int:
    data_root = get_data_root()
    parser = argparse.ArgumentParser(description="Build historical universe snapshots")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--market", choices=["perp", "spot"], default="perp")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    timeframe = normalize_timeframe(args.timeframe)
    symbols = [s.strip() for s in str(args.symbols).split(",") if s.strip()]
    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(
        level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s"
    )

    params = {
        "symbols": symbols,
        "market": args.market,
        "timeframe": timeframe,
        "force": int(args.force),
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("build_universe_snapshots", args.run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}, "monthly_membership": {}, "timeframe": timeframe}
    dataset = bars_dataset_name(timeframe)

    try:
        rows: List[Dict[str, object]] = []
        for symbol in symbols:
            df = _load_symbol_bars(args.run_id, symbol, args.market, timeframe)
            if df.empty:
                stats["symbols"][symbol] = {"rows": 0, "listed": False, "timeframe": timeframe}
                continue
            start_ts = df["timestamp"].min()
            end_ts = df["timestamp"].max()
            rows.append(
                {
                    "symbol": symbol,
                    "listing_start": start_ts,
                    "listing_end": end_ts,
                    "market": args.market,
                    "timeframe": timeframe,
                }
            )
            stats["symbols"][symbol] = {
                "rows": int(len(df)),
                "listed": True,
                "listing_start": start_ts.isoformat(),
                "listing_end": end_ts.isoformat(),
                "timeframe": timeframe,
            }
            month_cursor = pd.Timestamp(start_ts.year, start_ts.month, 1, tz="UTC")
            month_end = pd.Timestamp(end_ts.year, end_ts.month, 1, tz="UTC")
            while month_cursor <= month_end:
                key = f"{month_cursor.year:04d}-{month_cursor.month:02d}"
                stats["monthly_membership"].setdefault(key, []).append(symbol)
                month_cursor = (
                    (month_cursor + pd.offsets.MonthBegin(1)).normalize().tz_localize("UTC")
                    if month_cursor.tz is None
                    else month_cursor + pd.offsets.MonthBegin(1)
                )
            inputs.append(
                {
                    "path": str(
                        run_scoped_lake_path(
                            data_root, args.run_id, "cleaned", args.market, symbol, dataset
                        )
                    ),
                    "rows": int(len(df)),
                    "start_ts": start_ts.isoformat(),
                    "end_ts": end_ts.isoformat(),
                    "timeframe": timeframe,
                }
            )

        snap_df = pd.DataFrame(rows)
        if not snap_df.empty:
            global_latest = pd.to_datetime(snap_df["listing_end"], utc=True, errors="coerce").max()
            active_threshold = (
                global_latest - pd.Timedelta(days=7)
                if pd.notna(global_latest)
                else pd.Timestamp("1970-01-01", tz="UTC")
            )
            snap_df["status"] = snap_df["listing_end"].apply(
                lambda ts: (
                    "active"
                    if pd.to_datetime(ts, utc=True, errors="coerce") >= active_threshold
                    else "inactive"
                )
            )
        else:
            snap_df["status"] = pd.Series(dtype=str)
        for key, members in list(stats["monthly_membership"].items()):
            stats["monthly_membership"][key] = sorted(set(members))

        base_out = (
            Path(args.out_dir)
            if args.out_dir
            else data_root
            / "lake"
            / "runs"
            / args.run_id
            / "metadata"
            / "universe_snapshots"
            / timeframe
        )
        report_out = data_root / "reports" / "universe" / args.run_id / timeframe
        ensure_dir(base_out)
        ensure_dir(report_out)

        snapshot_path = base_out / "universe_snapshots.parquet"
        written, storage = write_parquet(snap_df, snapshot_path)
        outputs.append(
            {
                "path": str(written),
                "rows": int(len(snap_df)),
                "start_ts": None,
                "end_ts": None,
                "storage": storage,
                "timeframe": timeframe,
            }
        )

        summary_payload = {
            "run_id": args.run_id,
            "market": args.market,
            "timeframe": timeframe,
            "dataset": dataset,
            "symbols_requested": symbols,
            "symbols_with_history": int(len(snap_df)),
            "snapshots": snap_df.assign(
                listing_start=snap_df["listing_start"].astype(str)
                if not snap_df.empty
                else pd.Series(dtype=str),
                listing_end=snap_df["listing_end"].astype(str)
                if not snap_df.empty
                else pd.Series(dtype=str),
            ).to_dict(orient="records"),
            "monthly_membership": stats["monthly_membership"],
        }
        summary_json = report_out / "universe_membership.json"
        summary_json.write_text(
            json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        outputs.append({"path": str(summary_json), "rows": 1, "start_ts": None, "end_ts": None})

        if snap_df.empty:
            md_text = "# Universe Membership\n\nNo eligible symbols found.\n"
        else:
            try:
                table = snap_df.to_markdown(index=False)
            except Exception:
                table = snap_df.to_string(index=False)
            md_text = "# Universe Membership\n\n" + table + "\n"
        summary_md = report_out / "universe_membership.md"
        summary_md.write_text(md_text, encoding="utf-8")
        outputs.append(
            {
                "path": str(summary_md),
                "rows": int(len(md_text.splitlines())),
                "start_ts": None,
                "end_ts": None,
            }
        )

        manifest["outputs"] = outputs
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Universe snapshot build failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
