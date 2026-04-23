from __future__ import annotations
from project.core.config import get_data_root

import argparse
import json
import logging
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from project.core.config import load_configs
from project.io.utils import (
    choose_partition_dir,
    ensure_dir,
    lake_cache_key,
    list_parquet_files,
    raw_dataset_dir_candidates,
    read_cache_key,
    read_parquet,
    run_scoped_lake_path,
    write_cache_key,
    write_parquet,
)
from project.specs.manifest import (
    finalize_manifest,
    schema_hash_from_columns,
    start_manifest,
    validate_input_provenance,
)
from project.core.logging_utils import build_stage_log_handlers
from project.core.validation import (
    FUNDING_SCALE_NAME_TO_MULTIPLIER,
    assert_funding_event_grid,
    assert_funding_sane,
    assert_monotonic_utc_timestamp,
    assert_ohlcv_geometry,
    assert_ohlcv_schema,
    coerce_timestamps_to_hour,
    infer_and_apply_funding_scale,
    is_constant_series,
)
from project.core.validation import validate_columns
from project.core.data_quality import summarize_frame_quality
from project.core.timeframes import (
    bars_dataset_name,
    normalize_timeframe,
    ohlcv_dataset_name,
    timeframe_to_minutes,
    timeframe_to_pandas_freq,
)
from project.schemas.data_contracts import Cleaned5mBarsSchema

FUNDING_EVENT_HOURS = 8
FUNDING_MAX_STALENESS = pd.Timedelta(hours=8)


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


def _gap_lengths(is_gap: pd.Series) -> pd.Series:
    gap_group = (is_gap != is_gap.shift()).cumsum()
    lengths = is_gap.groupby(gap_group).transform("sum")
    return lengths.where(is_gap, 0).astype(int)


def _parse_optional_utc(ts_raw: str | None) -> pd.Timestamp | None:
    raw = str(ts_raw or "").strip()
    if not raw:
        return None
    ts = pd.Timestamp(raw)
    if ts.tzinfo is None:
        return ts.tz_localize(timezone.utc)
    return ts.tz_convert(timezone.utc)


def _resolve_requested_window(
    start_raw: str | None, end_raw: str | None
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    start_ts = _parse_optional_utc(start_raw)
    end_ts = _parse_optional_utc(end_raw)
    end_exclusive: pd.Timestamp | None = None
    if end_ts is not None:
        end_text = str(end_raw or "").strip()
        if len(end_text) == 10 and "T" not in end_text:
            end_exclusive = end_ts + timedelta(days=1)
        else:
            end_exclusive = end_ts
    if start_ts is not None and end_exclusive is not None and end_exclusive <= start_ts:
        raise ValueError("--end must be after --start")
    return start_ts, end_exclusive


def _align_funding(bars: pd.DataFrame, funding: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
    if funding.empty:
        aligned = bars[["timestamp"]].copy()
        aligned["funding_event_ts"] = pd.NaT
        aligned["funding_rate_feature"] = np.nan
        aligned["funding_rate_realized"] = 0.0
        aligned["funding_missing"] = True
        return aligned, 1.0

    funding_sorted = funding.copy()
    funding_sorted["timestamp"] = pd.to_datetime(
        funding_sorted["timestamp"], utc=True, errors="coerce"
    )
    funding_sorted = funding_sorted.dropna(subset=["timestamp"]).reset_index(drop=True)
    # Round sub-hour offsets to nearest hour (Binance archive CSVs have ms-level jitter)
    funding_sorted, coerced_count = coerce_timestamps_to_hour(funding_sorted, "timestamp")
    if coerced_count > 0:
        logging.info(
            "Coerced %s funding timestamps to nearest hour (sub-hour offsets in raw CSV data)",
            coerced_count,
        )
    funding_sorted = (
        funding_sorted.sort_values("timestamp")
        .drop_duplicates(subset=["timestamp"], keep="last")
        .reset_index(drop=True)
    )
    assert_monotonic_utc_timestamp(funding_sorted, "timestamp")
    assert_funding_sane(funding_sorted, "funding_rate_scaled")
    assert_funding_event_grid(funding_sorted, "timestamp", expected_hours=FUNDING_EVENT_HOURS)
    funding_sorted = funding_sorted.rename(columns={"timestamp": "funding_event_ts"})
    # Normalize both sides to bars timestamp dtype to avoid datetime resolution mismatch in pandas 2/3+
    ts_dtype = bars["timestamp"].dtype
    funding_sorted["funding_event_ts"] = funding_sorted["funding_event_ts"].astype(ts_dtype)
    merged = pd.merge_asof(
        bars.sort_values("timestamp"),
        funding_sorted[["funding_event_ts", "funding_rate_scaled"]],
        left_on="timestamp",
        right_on="funding_event_ts",
        direction="nearest",
        tolerance=FUNDING_MAX_STALENESS,
    )
    if "funding_rate_scaled" in merged.columns:
        # Keep the full rate as the "feature"
        merged["funding_rate_feature"] = merged["funding_rate_scaled"]

        # Realized cashflow applies ONLY exactly on the funding timestamp
        is_exact = merged["timestamp"] == merged["funding_event_ts"]
        merged["funding_rate_realized"] = np.where(is_exact, merged["funding_rate_scaled"], 0.0)

    merged["funding_missing"] = merged["funding_rate_feature"].isna()
    missing_pct = float(merged["funding_missing"].mean()) if len(merged) else 0.0
    return merged[
        [
            "timestamp",
            "funding_event_ts",
            "funding_rate_feature",
            "funding_rate_realized",
            "funding_missing",
        ]
    ], missing_pct


def _coerce_numeric_columns(frame: pd.DataFrame, columns: list[str]) -> int:
    coerced_value_count = 0
    for col in columns:
        if col not in frame.columns:
            continue
        before_non_null = int(frame[col].notna().sum())
        coerced = pd.to_numeric(frame[col], errors="coerce")
        after_non_null = int(coerced.notna().sum())
        coerced_value_count += max(0, before_non_null - after_non_null)
        frame[col] = coerced.astype(float)
    return coerced_value_count


def _data_quality_report_path(
    data_root: Path,
    *,
    run_id: str,
    market: str,
    symbol: str,
    timeframe: str,
) -> Path:
    return (
        data_root
        / "reports"
        / "data_quality"
        / run_id
        / "cleaned"
        / market
        / symbol
        / f"bars_{timeframe}_quality.json"
    )


def _write_data_quality_report(path: Path, payload: dict[str, object]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _resolve_raw_dir(
    data_root: Path,
    *,
    market: str,
    symbol: str,
    dataset: str,
    run_id: str | None = None,
    aliases: tuple[str, ...] = (),
) -> Path | None:
    datasets = [str(dataset).strip(), *[str(alias).strip() for alias in aliases if str(alias).strip()]]
    candidates = raw_dataset_dir_candidates(
        data_root,
        market=market,
        symbol=symbol,
        dataset=dataset,
        run_id=run_id,
        aliases=aliases,
    )
    roots: list[Path] = []
    if run_id:
        roots.append(run_scoped_lake_path(data_root, run_id, "raw"))
    roots.append(Path(data_root) / "lake" / "raw")
    seen = {str(path) for path in candidates}
    for root in roots:
        if not root.exists() or not root.is_dir():
            continue
        for venue_dir in sorted(path for path in root.iterdir() if path.is_dir()):
            for dataset_name in datasets:
                candidate = venue_dir / market / symbol / dataset_name
                key = str(candidate)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(candidate)
    return choose_partition_dir(candidates) or (candidates[0] if candidates else None)


def main() -> int:
    data_root = get_data_root()
    parser = argparse.ArgumentParser(description="Build cleaned bars for the requested timeframe")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--market", choices=["perp", "spot"], default="perp")
    parser.add_argument("--start", required=False)
    parser.add_argument("--end", required=False)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument(
        "--funding_scale", choices=["auto", "decimal", "percent", "bps"], default="auto"
    )
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()
    requested_start, requested_end_exclusive = _resolve_requested_window(args.start, args.end)

    run_id = args.run_id
    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    market = str(args.market).strip().lower()
    timeframe = normalize_timeframe(args.timeframe)
    tf_minutes = timeframe_to_minutes(timeframe)
    tf_freq = timeframe_to_pandas_freq(timeframe)
    bars_dataset = bars_dataset_name(timeframe)
    ohlcv_dataset = ohlcv_dataset_name(timeframe)

    log_handlers = build_stage_log_handlers(args.log_path)
    logging.basicConfig(
        level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s"
    )

    params = {
        "symbols": symbols,
        "market": market,
        "start": args.start,
        "end": args.end,
        "funding_scale": str(args.funding_scale),
        "source_vendor": "bybit",
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    stage_name = (
        f"build_cleaned_{timeframe}" if market == "perp" else f"build_cleaned_{timeframe}_spot"
    )
    manifest = start_manifest(stage_name, run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        for symbol in symbols:
            raw_dir = _resolve_raw_dir(
                data_root,
                market=market,
                symbol=symbol,
                dataset=ohlcv_dataset,
                run_id=run_id,
            )
            raw_files = list_parquet_files(raw_dir) if raw_dir else []

            # Accept both "funding" (new ingest) and "fundingRate" (legacy ingest path)
            funding_files: list = []
            funding_dir = None
            if market == "perp":
                for _subdir in ("funding", "fundingRate"):
                    _candidate = _resolve_raw_dir(
                        data_root,
                        market=market,
                        symbol=symbol,
                        dataset=_subdir,
                        run_id=run_id,
                    )
                    _files = list_parquet_files(_candidate) if _candidate else []
                    if _files:
                        funding_dir = _candidate
                        funding_files = _files
                        break

            raw = read_parquet(raw_files)
            funding = (
                read_parquet(funding_files)
                if market == "perp" and funding_files
                else pd.DataFrame()
            )

            if raw.empty:
                logging.warning("No raw OHLCV %s data for %s", timeframe, symbol)
                continue
            assert_ohlcv_schema(raw)
            assert_ohlcv_geometry(raw)

            raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True)
            raw = (
                raw.sort_values("timestamp")
                .drop_duplicates(subset=["timestamp"])
                .reset_index(drop=True)
            )
            if requested_start is not None:
                raw = raw[raw["timestamp"] >= requested_start].copy()
            if requested_end_exclusive is not None:
                raw = raw[raw["timestamp"] < requested_end_exclusive].copy()
            if raw.empty:
                logging.warning(
                    "No raw OHLCV %s data for %s in requested window start=%s end=%s",
                    timeframe,
                    symbol,
                    requested_start,
                    requested_end_exclusive,
                )
                continue
            inputs.append(
                {
                    "path": str(raw_dir),
                    "rows": int(len(raw)),
                    "start_ts": raw["timestamp"].min().isoformat(),
                    "end_ts": raw["timestamp"].max().isoformat(),
                    "provenance": {
                        "vendor": "bybit",
                        "exchange": "bybit",
                        "schema_version": f"raw_ohlcv_{timeframe}_v1",
                        "schema_hash": schema_hash_from_columns(raw.columns.tolist()),
                        "extraction_start": raw["timestamp"].min().isoformat(),
                        "extraction_end": raw["timestamp"].max().isoformat(),
                    },
                }
            )

            start_ts = raw["timestamp"].min()
            end_ts = raw["timestamp"].max()
            end_exclusive = end_ts + timedelta(minutes=tf_minutes)

            full_index = pd.date_range(
                start=start_ts,
                end=end_exclusive - timedelta(minutes=tf_minutes),
                freq=tf_freq,
                tz=timezone.utc,
            )
            bars = (
                raw.set_index("timestamp")
                .reindex(full_index)
                .reset_index()
                .rename(columns={"index": "timestamp"})
            )

            gap_cols = ["open", "high", "low", "close", "volume"]
            for opt_col in ["quote_volume", "taker_base_volume"]:
                if opt_col in bars.columns:
                    gap_cols.append(opt_col)

            bars["is_gap"] = bars[gap_cols].isna().any(axis=1)
            bars["gap_len"] = _gap_lengths(bars["is_gap"])
            bars["symbol"] = symbol

            # Do NOT forward fill prices for gaps to prevent optimistic stability bias
            price_cols = ["open", "high", "low", "close"]

            # Zero fill volume for gaps
            vol_cols = [c for c in gap_cols if c not in price_cols]
            bars[vol_cols] = bars[vol_cols].fillna(0.0)

            # Ensure quote_volume column exists for schema compliance.
            if "quote_volume" not in bars.columns:
                bars["quote_volume"] = 0.0

            # Normalize numeric OHLCV dtypes to float for schema stability.
            # Upstream parquet chunks can carry integer volume when there are no NaNs.
            coerced_value_count = _coerce_numeric_columns(
                bars,
                ["open", "high", "low", "close", "volume", "quote_volume", "taker_base_volume"],
            )

            if market == "perp" and not funding.empty:
                funding["timestamp"] = pd.to_datetime(
                    funding["timestamp"], utc=True, format="ISO8601"
                )
                funding = (
                    funding.dropna(subset=["timestamp"])
                    .sort_values("timestamp")
                    .drop_duplicates(subset=["timestamp"], keep="last")
                )
                funding_raw_start = funding["timestamp"].min()
                funding_raw_end = funding["timestamp"].max()
                funding_window_start = start_ts - FUNDING_MAX_STALENESS
                funding = funding[funding["timestamp"] >= funding_window_start].copy()
                if requested_end_exclusive is not None:
                    funding = funding[funding["timestamp"] < requested_end_exclusive].copy()
                if funding.empty:
                    logging.warning(
                        "Funding data for %s does not overlap requested window; raw funding range=%s..%s, "
                        "requested bars range=%s..%s. Downstream funding features will be missing.",
                        symbol,
                        funding_raw_start.isoformat() if pd.notna(funding_raw_start) else "unknown",
                        funding_raw_end.isoformat() if pd.notna(funding_raw_end) else "unknown",
                        funding_window_start.isoformat(),
                        requested_end_exclusive.isoformat()
                        if requested_end_exclusive is not None
                        else end_exclusive.isoformat(),
                    )
                inputs.append(
                    {
                        "path": str(funding_dir),
                        "rows": int(len(funding)),
                        "start_ts": funding["timestamp"].min().isoformat()
                        if not funding.empty
                        else funding_raw_start.isoformat(),
                        "end_ts": funding["timestamp"].max().isoformat()
                        if not funding.empty
                        else funding_raw_end.isoformat(),
                        "provenance": {
                            "vendor": "bybit",
                            "exchange": "bybit",
                            "schema_version": "raw_funding_v1",
                            "schema_hash": schema_hash_from_columns(funding.columns.tolist()),
                            "extraction_start": funding["timestamp"].min().isoformat()
                            if not funding.empty
                            else funding_raw_start.isoformat(),
                            "extraction_end": funding["timestamp"].max().isoformat()
                            if not funding.empty
                            else funding_raw_end.isoformat(),
                        },
                    }
                )
                if "funding_rate" not in funding.columns:
                    raise ValueError(
                        "funding input must contain canonical raw funding_rate before scale inference"
                    )
                # Provide source hint so scale inference uses the known-decimal path
                if "source" not in funding.columns:
                    funding = funding.copy()
                    funding["source"] = "archive_monthly"

                explicit_scale = None
                if str(args.funding_scale).strip().lower() != "auto":
                    explicit_scale = float(
                        FUNDING_SCALE_NAME_TO_MULTIPLIER[str(args.funding_scale).strip().lower()]
                    )

                if (
                    funding.empty
                    or pd.to_numeric(funding["funding_rate"], errors="coerce").dropna().empty
                ):
                    funding["funding_rate_scaled"] = np.nan
                    inferred_scale, scale_confidence = 1.0, 1.0
                else:
                    funding, inferred_scale, scale_confidence = infer_and_apply_funding_scale(
                        funding,
                        "funding_rate",
                        explicit_scale=explicit_scale,
                    )

                    # ENFORCEABLE GATE: Range sanity check for scale errors (100x/10000x)
                    max_abs_funding = funding["funding_rate_scaled"].abs().max()
                    if max_abs_funding > 0.15:  # 15% per 8h is insane (normally < 0.3% decimal)
                        raise ValueError(
                            f"Extreme funding rate detected (max={max_abs_funding:.4f}) for {symbol}. "
                            f"Inferred scale {inferred_scale} may be incorrect. Verify data vendor scale."
                        )
                logging.info(
                    "Funding scale inference symbol=%s mode=%s scale=%.6g confidence=%.4f",
                    symbol,
                    args.funding_scale,
                    inferred_scale,
                    scale_confidence,
                )
                if (
                    str(args.funding_scale).strip().lower() == "auto"
                    and float(scale_confidence) < 0.99
                ):
                    raise ValueError(
                        f"Low confidence funding scale inference for {symbol}: "
                        f"confidence={scale_confidence:.4f} (<0.99). "
                        "Set --funding_scale explicitly (decimal|percent|bps)."
                    )
                aligned_funding, _ = _align_funding(bars, funding)
                bars = bars.merge(
                    aligned_funding[
                        [
                            "timestamp",
                            "funding_event_ts",
                            "funding_rate_feature",
                            "funding_rate_realized",
                            "funding_missing",
                        ]
                    ],
                    on="timestamp",
                    how="left",
                )
                bars["funding_rate_scaled"] = bars["funding_rate_feature"]
                bars["funding_missing"] = bars["funding_missing"].fillna(True).astype(bool)
            else:
                bars["funding_rate_feature"] = np.nan
                bars["funding_rate_scaled"] = np.nan
                bars["funding_rate_realized"] = 0.0
                bars["funding_missing"] = True

            overall_quality = summarize_frame_quality(
                bars,
                expected_minutes=tf_minutes,
                numeric_cols=[
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "quote_volume",
                    "taker_base_volume",
                    "funding_rate_scaled",
                ],
                coerced_value_count=coerced_value_count,
            )
            monthly_quality: dict[str, dict[str, object]] = {}

            cleaned_dir = data_root / "lake" / "cleaned" / market / symbol / bars_dataset
            run_cleaned_dir = run_scoped_lake_path(
                data_root, run_id, "cleaned", market, symbol, bars_dataset
            )

            for month_start in _iter_months(start_ts, end_ts):
                month_end = _next_month(month_start)
                range_start = max(start_ts, month_start)
                range_end_exclusive = min(end_exclusive, month_end)

                bars_month = bars[
                    (bars["timestamp"] >= range_start) & (bars["timestamp"] < range_end_exclusive)
                ]
                filename_symbol = f"{symbol}_spot" if market == "spot" else symbol

                out_path = (
                    run_cleaned_dir
                    / f"year={month_start.year}"
                    / f"month={month_start.month:02d}"
                    / f"bars_{filename_symbol}_{timeframe}_{month_start.year}-{month_start.month:02d}.parquet"
                )
                compat_path = (
                    cleaned_dir
                    / f"year={month_start.year}"
                    / f"month={month_start.month:02d}"
                    / f"bars_{filename_symbol}_{timeframe}_{month_start.year}-{month_start.month:02d}.parquet"
                )

                # Cache hit: copy from shared lake if the key matches and run-scoped is absent
                _month_raw_files = [
                    f for f in raw_files
                    if f"year={month_start.year}" in str(f)
                    and f"month={month_start.month:02d}" in str(f)
                ]
                _month_funding_files = [
                    f for f in funding_files
                    if f"year={month_start.year}" in str(f)
                    and f"month={month_start.month:02d}" in str(f)
                ] if funding_files else []
                _cache_key = lake_cache_key(
                    symbol, market, timeframe,
                    month_start.year, month_start.month,
                    _month_raw_files + _month_funding_files,
                    funding_scale=str(args.funding_scale),
                    requested_start=str(requested_start),
                    requested_end_exclusive=str(requested_end_exclusive),
                )
                # Enforce Runtime Data Contract
                Cleaned5mBarsSchema.validate(bars_month)

                assert_monotonic_utc_timestamp(bars_month, "timestamp")
                if bars_month["is_gap"].isna().any():
                    raise ValueError("is_gap column must be strictly boolean without NaNs.")

                logging.info("Writing cleaned data to out_path: %s", out_path)
                ensure_dir(out_path.parent)
                written, storage = write_parquet(bars_month.reset_index(drop=True), out_path)
                written_path = Path(written)

                # Populate shared cache for future runs
                if written_path.exists() and not compat_path.exists() and _cache_key:
                    ensure_dir(compat_path.parent)
                    shutil.copy2(written_path, compat_path)
                    write_cache_key(compat_path, _cache_key)

                outputs.append(
                    {
                        "path": str(written_path),
                        "rows": int(len(bars_month)),
                        "start_ts": bars_month["timestamp"].min().isoformat(),
                        "end_ts": bars_month["timestamp"].max().isoformat(),
                        "storage": storage,
                    }
                )
                month_key = f"{month_start.year}-{month_start.month:02d}"
                monthly_quality[month_key] = summarize_frame_quality(
                    bars_month,
                    expected_minutes=tf_minutes,
                    numeric_cols=[
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "quote_volume",
                        "taker_base_volume",
                        "funding_rate_scaled",
                    ],
                ).to_dict()

            report_path = _data_quality_report_path(
                data_root,
                run_id=run_id,
                market=market,
                symbol=symbol,
                timeframe=timeframe,
            )
            _write_data_quality_report(
                report_path,
                {
                    "schema_version": "data_quality_report_v1",
                    "run_id": run_id,
                    "stage": stage_name,
                    "market": market,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "requested_start": requested_start.isoformat()
                    if requested_start is not None
                    else None,
                    "requested_end_exclusive": (
                        requested_end_exclusive.isoformat()
                        if requested_end_exclusive is not None
                        else None
                    ),
                    "overall": overall_quality.to_dict(),
                    "by_month": monthly_quality,
                },
            )

            stats["symbols"][symbol] = {
                "start": start_ts.isoformat(),
                "end": end_ts.isoformat(),
                "rows": int(len(bars)),
                "funding_scale_mode": str(args.funding_scale),
                "requested_start": requested_start.isoformat()
                if requested_start is not None
                else None,
                "requested_end_exclusive": (
                    requested_end_exclusive.isoformat()
                    if requested_end_exclusive is not None
                    else None
                ),
                "funding_missing_pct": float(
                    pd.to_numeric(bars["funding_missing"], errors="coerce").mean()
                ),
                "data_quality": overall_quality.to_dict(),
                "data_quality_report_path": str(report_path),
            }

        validate_input_provenance(inputs)
        manifest["outputs"] = outputs
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Cleaning failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
