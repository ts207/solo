from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from project.core.timeframes import bars_dataset_name, funding_dataset_name, normalize_timeframe
from project.events.detectors.registry import get_detector_class
from project.features import build_features
from project.features.assembly import filter_time_window, prune_partition_files_by_window
from project.io.utils import (
    choose_partition_dir,
    list_parquet_files,
    raw_dataset_dir_candidates,
    read_parquet,
    run_scoped_lake_path,
)

_BLOCKING_COLUMN_STATUSES = {"missing", "no_coverage", "constant"}
_WARN_COLUMN_STATUSES = {"low_coverage", "warmup_limited"}
_WARMUP_SENSITIVE_COLUMNS = {"rv_96", "rv_pct_17280", "range_med_2880", "funding_abs_pct"}


@dataclass(frozen=True)
class FeatureSurfaceConfig:
    min_non_null_fraction: float = 0.05
    sample_symbol_limit: int = 3
    min_strict_rows: int = 20


def _load_partitioned_frame(path_dir: Path | None, *, start: str | None, end: str | None) -> pd.DataFrame:
    if path_dir is None:
        return pd.DataFrame()
    files = prune_partition_files_by_window(list_parquet_files(path_dir), start=start, end=end)
    if not files:
        return pd.DataFrame()
    frame = read_parquet(files)
    return filter_time_window(frame, start=start, end=end)


def _resolve_raw_dir(
    data_root: Path,
    *,
    market: str,
    symbol: str,
    dataset: str,
    run_id: str | None = None,
    aliases: Sequence[str] = (),
) -> Path | None:
    candidates = raw_dataset_dir_candidates(
        data_root,
        market=market,
        symbol=symbol,
        dataset=dataset,
        run_id=run_id,
        aliases=tuple(aliases),
    )
    roots: list[Path] = []
    if run_id:
        roots.append(run_scoped_lake_path(data_root, run_id, "raw"))
    roots.append(Path(data_root) / "lake" / "raw")
    seen = {str(path) for path in candidates}
    dataset_names = [str(dataset).strip(), *[str(alias).strip() for alias in aliases if str(alias).strip()]]
    for root in roots:
        if not root.exists() or not root.is_dir():
            continue
        for venue_dir in sorted(path for path in root.iterdir() if path.is_dir()):
            for dataset_name in dataset_names:
                candidate = venue_dir / market / symbol / dataset_name
                key = str(candidate)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(candidate)
    return choose_partition_dir(candidates) or (candidates[0] if candidates else None)


def _load_bars_for_viability(
    *,
    data_root: Path,
    run_id: str,
    market: str,
    symbol: str,
    timeframe: str,
    start: str,
    end: str,
) -> tuple[pd.DataFrame, str | None]:
    cleaned_dir = choose_partition_dir(
        [
            run_scoped_lake_path(data_root, run_id, "cleaned", market, symbol, bars_dataset_name(timeframe)),
            Path(data_root) / "lake" / "cleaned" / market / symbol / bars_dataset_name(timeframe),
        ]
    )
    if cleaned_dir is not None:
        return _load_partitioned_frame(cleaned_dir, start=start, end=end), "cleaned"

    raw_dir = _resolve_raw_dir(
        data_root,
        market=market,
        symbol=symbol,
        dataset=f"ohlcv_{timeframe}",
        run_id=run_id,
        aliases=(bars_dataset_name(timeframe),),
    )
    if raw_dir is not None:
        return _load_partitioned_frame(raw_dir, start=start, end=end), "raw"
    return pd.DataFrame(), None


def _load_funding_for_viability(
    *,
    data_root: Path,
    run_id: str,
    symbol: str,
    timeframe: str,
    start: str,
    end: str,
) -> pd.DataFrame:
    funding_dir = _resolve_raw_dir(
        data_root,
        market="perp",
        symbol=symbol,
        dataset=funding_dataset_name(timeframe),
        run_id=run_id,
        aliases=("funding",),
    )
    return _load_partitioned_frame(funding_dir, start=start, end=end)


def _series_status(series: pd.Series, *, column_name: str, min_non_null_fraction: float, min_strict_rows: int) -> dict[str, Any]:
    total = len(series)
    non_null = series.dropna()
    non_null_count = len(non_null)
    coverage_ratio = float(non_null_count / total) if total > 0 else 0.0
    if non_null_count == 0:
        status = "warmup_limited" if total < int(min_strict_rows) and column_name in _WARMUP_SENSITIVE_COLUMNS else "no_coverage"
        return {
            "status": status,
            "coverage_ratio": coverage_ratio,
            "unique_non_null": 0,
            "sample_size": total,
        }
    unique_non_null = int(non_null.nunique(dropna=True))
    if unique_non_null <= 1:
        status = "warmup_limited" if total < int(min_strict_rows) and column_name in _WARMUP_SENSITIVE_COLUMNS else "constant"
        return {
            "status": status,
            "coverage_ratio": coverage_ratio,
            "unique_non_null": unique_non_null,
            "sample_size": total,
        }
    if coverage_ratio < float(min_non_null_fraction):
        return {
            "status": "low_coverage",
            "coverage_ratio": coverage_ratio,
            "unique_non_null": unique_non_null,
            "sample_size": total,
        }
    return {
        "status": "ok",
        "coverage_ratio": coverage_ratio,
        "unique_non_null": unique_non_null,
        "sample_size": total,
    }


def _required_columns_for_event(event_type: str) -> list[str]:
    detector_cls = get_detector_class(event_type)
    if detector_cls is None:
        return []
    return [str(col).strip() for col in getattr(detector_cls, "required_columns", ()) if str(col).strip()]


def _event_status(column_statuses: Mapping[str, Mapping[str, Any]]) -> tuple[str, list[str], list[str]]:
    blocking = [column for column, stats in column_statuses.items() if stats.get("status") in _BLOCKING_COLUMN_STATUSES]
    degraded = [column for column, stats in column_statuses.items() if stats.get("status") in _WARN_COLUMN_STATUSES]
    if blocking:
        return "block", blocking, degraded
    if degraded:
        return "warn", blocking, degraded
    return "pass", blocking, degraded


def analyze_feature_surface_viability(
    *,
    data_root: Path,
    run_id: str,
    symbols: Sequence[str],
    timeframe: str,
    start: str,
    end: str,
    event_types: Sequence[str],
    market: str = "perp",
    config: FeatureSurfaceConfig | None = None,
) -> dict[str, Any]:
    cfg = config or FeatureSurfaceConfig()
    tf = normalize_timeframe(timeframe)
    requested_events = [str(event).strip().upper() for event in event_types if str(event).strip()]
    if not requested_events:
        return {
            "schema_version": "feature_surface_viability_v1",
            "status": "unknown",
            "timeframe": tf,
            "event_types": [],
            "symbols": {},
            "detectors": {},
            "issues": ["no target event types were provided"],
        }

    per_symbol: dict[str, Any] = {}
    event_aggregate: dict[str, dict[str, list[str]]] = {
        event: {"pass": [], "warn": [], "block": [], "unknown": []} for event in requested_events
    }
    issues: list[str] = []

    for symbol in [str(item).strip().upper() for item in symbols if str(item).strip()][: cfg.sample_symbol_limit or None]:
        try:
            bars, source = _load_bars_for_viability(
                data_root=data_root,
                run_id=run_id,
                market=market,
                symbol=symbol,
                timeframe=tf,
                start=start,
                end=end,
            )
        except Exception as exc:
            per_symbol[symbol] = {"status": "unknown", "error": f"bars_load_failed: {exc}", "detectors": {}}
            issues.append(f"{symbol}: failed to load bars for viability analysis: {exc}")
            for event in requested_events:
                event_aggregate[event]["unknown"].append(symbol)
            continue

        if bars.empty:
            per_symbol[symbol] = {"status": "unknown", "error": "no bars available", "detectors": {}, "bars_source": source}
            issues.append(f"{symbol}: no cleaned or raw bars available for viability analysis")
            for event in requested_events:
                event_aggregate[event]["unknown"].append(symbol)
            continue

        try:
            funding = _load_funding_for_viability(
                data_root=data_root,
                run_id=run_id,
                symbol=symbol,
                timeframe=tf,
                start=start,
                end=end,
            )
            features = build_features(
                bars,
                funding,
                symbol,
                run_id=run_id,
                data_root=data_root,
                market=market,
                timeframe=tf,
            )
        except Exception as exc:
            per_symbol[symbol] = {
                "status": "unknown",
                "error": f"feature_build_failed: {exc}",
                "detectors": {},
                "bars_source": source,
                "bar_count": len(bars),
            }
            issues.append(f"{symbol}: feature viability build failed: {exc}")
            for event in requested_events:
                event_aggregate[event]["unknown"].append(symbol)
            continue

        detectors: dict[str, Any] = {}
        symbol_status = "pass"
        for event in requested_events:
            required_columns = _required_columns_for_event(event)
            if not required_columns:
                detectors[event] = {
                    "status": "unknown",
                    "required_columns": [],
                    "column_status": {},
                    "blocking_columns": [],
                    "degraded_columns": [],
                    "reason": "detector metadata unavailable",
                }
                event_aggregate[event]["unknown"].append(symbol)
                symbol_status = "warn" if symbol_status == "pass" else symbol_status
                continue
            column_status: dict[str, Any] = {}
            for column in required_columns:
                if column not in features.columns:
                    column_status[column] = {
                        "status": "missing",
                        "coverage_ratio": 0.0,
                        "unique_non_null": 0,
                        "sample_size": len(features),
                    }
                    continue
                column_status[column] = _series_status(
                    features[column],
                    column_name=column,
                    min_non_null_fraction=cfg.min_non_null_fraction,
                    min_strict_rows=cfg.min_strict_rows,
                )
            detector_status, blocking_columns, degraded_columns = _event_status(column_status)
            detectors[event] = {
                "status": detector_status,
                "required_columns": required_columns,
                "column_status": column_status,
                "blocking_columns": blocking_columns,
                "degraded_columns": degraded_columns,
            }
            event_aggregate[event][detector_status].append(symbol)
            if detector_status == "block":
                symbol_status = "block"
            elif detector_status == "warn" and symbol_status != "block":
                symbol_status = "warn"

        per_symbol[symbol] = {
            "status": symbol_status,
            "bars_source": source,
            "bar_count": len(bars),
            "feature_row_count": len(features),
            "detectors": detectors,
        }

    detectors_summary: dict[str, Any] = {}
    statuses: list[str] = []
    for event, buckets in event_aggregate.items():
        if buckets["pass"] and not buckets["warn"] and not buckets["block"] and not buckets["unknown"]:
            status = "pass"
        elif buckets["block"] and not buckets["pass"] and not buckets["warn"]:
            status = "block"
        elif buckets["unknown"] and not buckets["pass"] and not buckets["warn"] and not buckets["block"]:
            status = "unknown"
        else:
            status = "warn"
        statuses.append(status)
        detectors_summary[event] = {
            "status": status,
            "pass_symbols": buckets["pass"],
            "warn_symbols": buckets["warn"],
            "block_symbols": buckets["block"],
            "unknown_symbols": buckets["unknown"],
        }

    if statuses and all(status == "block" for status in statuses):
        overall_status = "block"
    elif any(status in {"warn", "block"} for status in statuses):
        overall_status = "warn"
    elif statuses and all(status == "unknown" for status in statuses):
        overall_status = "unknown"
    else:
        overall_status = "pass"

    return {
        "schema_version": "feature_surface_viability_v1",
        "status": overall_status,
        "timeframe": tf,
        "event_types": requested_events,
        "symbols": per_symbol,
        "detectors": detectors_summary,
        "issues": issues,
    }
