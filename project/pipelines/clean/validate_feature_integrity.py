from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from project.core.config import get_data_root
from project.core.feature_quality import summarize_feature_quality
from project.core.feature_schema import feature_dataset_dir_name
from project.eval.drift_detection import detect_feature_drift
from project.io.utils import (
    choose_partition_dir,
    list_parquet_files,
    read_parquet,
    resolve_raw_dataset_dir,
    run_scoped_lake_path,
)
from project.specs.manifest import finalize_manifest, start_manifest

LOGGER = logging.getLogger(__name__)

_CONSTANT_OK_COLUMNS = {
    "funding_missing",
    "gap_len",
    "is_gap",
    "revision_lag_bars",
    "revision_lag_minutes",
}
_TOB_DEPENDENT_COLUMNS = {
    "imbalance",
    "tob_coverage",
}
_SPOT_DEPENDENT_COLUMNS = {
    "basis_bps",
    "basis_spot_coverage",
    "basis_zscore",
    "cross_exchange_spread_z",
    "spot_close",
}
_LIQUIDATION_DEPENDENT_COLUMNS = {
    "liquidation_count",
    "liquidation_notional",
}
_OI_DEPENDENT_COLUMNS = {
    "oi_notional",
}


def _report_path(data_root: Path, *, run_id: str, timeframe: str) -> Path:
    return (
        data_root
        / "reports"
        / "feature_quality"
        / run_id
        / "validation"
        / f"validate_feature_integrity_{timeframe}.json"
    )


def _write_report(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _summarize_drift_flags(symbol: str, drift_flags: list[dict[str, float]]) -> None:
    if not drift_flags:
        return
    preview = ", ".join(flag["feature"] for flag in drift_flags[:5])
    if len(drift_flags) > 5:
        preview += ", ..."
    LOGGER.warning(
        "%s feature drift summary: %s flagged columns (%s)",
        symbol,
        len(drift_flags),
        preview,
    )


def _has_source_artifacts(candidates: list[Path]) -> bool:
    source_dir = choose_partition_dir(candidates)
    if not source_dir:
        return False
    return bool(list_parquet_files(source_dir))


def _ignored_feature_columns(
    *,
    data_root: Path,
    run_id: str,
    symbol: str,
    timeframe: str,
) -> tuple[set[str], set[str]]:
    ignored_nan_columns: set[str] = set()
    ignored_constant_columns = set(_CONSTANT_OK_COLUMNS)

    def _existing_raw_dirs(*, market: str, dataset: str, aliases: tuple[str, ...] = ()) -> list[Path]:
        results: list[Path] = []
        seen: set[str] = set()
        for venue in ("bybit", "binance"):
            resolved = resolve_raw_dataset_dir(
                data_root,
                market=market,
                symbol=symbol,
                dataset=dataset,
                run_id=run_id,
                venue=venue,
                aliases=aliases,
            )
            if resolved is None:
                continue
            key = str(resolved)
            if key in seen:
                continue
            seen.add(key)
            results.append(resolved)
        return results

    spot_candidates = [
        run_scoped_lake_path(data_root, run_id, "cleaned", "spot", symbol, f"bars_{timeframe}"),
        data_root / "lake" / "cleaned" / "spot" / symbol / f"bars_{timeframe}",
    ]
    spot_candidates.extend(_existing_raw_dirs(market="spot", dataset=f"ohlcv_{timeframe}"))
    if not _has_source_artifacts(spot_candidates):
        ignored_nan_columns.update(_SPOT_DEPENDENT_COLUMNS)
        ignored_constant_columns.update(_SPOT_DEPENDENT_COLUMNS)

    liquidation_candidates = []
    for dataset_name in ("liquidations", "liquidation_snapshot"):
        liquidation_candidates.extend(_existing_raw_dirs(market="perp", dataset=dataset_name))
    if not _has_source_artifacts(liquidation_candidates):
        ignored_nan_columns.update(_LIQUIDATION_DEPENDENT_COLUMNS)
        ignored_constant_columns.update(_LIQUIDATION_DEPENDENT_COLUMNS)

    oi_candidates = []
    for dataset_name in ("open_interest",):
        oi_candidates.extend(_existing_raw_dirs(market="perp", dataset=dataset_name, aliases=(timeframe,)))
    if not _has_source_artifacts(oi_candidates):
        ignored_nan_columns.update(_OI_DEPENDENT_COLUMNS)
        ignored_constant_columns.update(_OI_DEPENDENT_COLUMNS)

    tob_candidates = [
        run_scoped_lake_path(data_root, run_id, "cleaned", "perp", symbol, "tob_5m_agg"),
        data_root / "lake" / "cleaned" / "perp" / symbol / "tob_5m_agg",
    ]
    if not _has_source_artifacts(tob_candidates):
        ignored_constant_columns.update(_TOB_DEPENDENT_COLUMNS)

    return ignored_nan_columns, ignored_constant_columns


def check_nans(
    df: pd.DataFrame,
    threshold: float = 0.05,
    *,
    ignored_columns: set[str] | None = None,
) -> list[str]:
    ignored = ignored_columns or set()
    nan_pcts = df.isna().mean()
    if ignored:
        nan_pcts = nan_pcts.drop(labels=[col for col in ignored if col in nan_pcts.index])
    failing_cols = nan_pcts[nan_pcts > threshold]
    return [
        f"Column '{col}' has {pct:.2%} NaNs (threshold {threshold:.2%})"
        for col, pct in failing_cols.items()
    ]


def check_constant_values(
    df: pd.DataFrame,
    *,
    ignored_columns: set[str] | None = None,
) -> list[str]:
    ignored = ignored_columns or set()
    num_df = df.select_dtypes(include=[np.number])
    if num_df.empty:
        return []
    nunique = num_df.nunique()
    all_nan = num_df.isna().all()
    constant_cols = nunique[(nunique <= 1) & (~all_nan)].index
    constant_cols = [col for col in constant_cols if col not in ignored]
    return [f"Column '{col}' is constant." for col in constant_cols]


def check_outliers(df: pd.DataFrame, z_threshold: float = 10.0) -> list[str]:
    cols_to_check = [
        c
        for c in df.select_dtypes(include=[np.number]).columns
        if c not in ["timestamp", "open", "high", "low", "close", "volume"]
    ]
    if not cols_to_check:
        return []

    num_df = df[cols_to_check]
    means = num_df.mean()
    stds = num_df.std()

    valid_stds = (stds > 0.0) & np.isfinite(stds)
    cols_to_check_valid = valid_stds[valid_stds].index
    if len(cols_to_check_valid) == 0:
        return []

    num_df_valid = num_df[cols_to_check_valid]
    z_scores = (num_df_valid - means[cols_to_check_valid]) / stds[cols_to_check_valid]
    outlier_pcts = (z_scores.abs() > z_threshold).mean()

    failing_cols = outlier_pcts[outlier_pcts > 0.01]
    return [
        f"Column '{col}' has {pct:.2%} extreme outliers (> {z_threshold} sigma)"
        for col, pct in failing_cols.items()
    ]


def validate_symbol(
    data_root: Path,
    run_id: str,
    symbol: str,
    *,
    timeframe: str = "5m",
    nan_threshold: float = 0.05,
    z_threshold: float = 10.0,
    reference_distributions_path: str = "train_distributions.json",
) -> dict[str, list[str]]:
    symbol_issues = {}
    feature_quality_summary = None

    # 1. Check cleaned bars
    bars_candidates = [
        run_scoped_lake_path(data_root, run_id, "cleaned", "perp", symbol, f"bars_{timeframe}"),
        data_root / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}",
    ]
    bars_dir = choose_partition_dir(bars_candidates)
    if bars_dir:
        df_bars = read_parquet(list_parquet_files(bars_dir))
        if not df_bars.empty:
            bars_issues = check_nans(df_bars, threshold=nan_threshold) + check_constant_values(
                df_bars,
                ignored_columns={"gap_len", "is_gap"},
            )
            if bars_issues:
                symbol_issues["bars"] = bars_issues

    # 2. Check features
    feature_dataset = feature_dataset_dir_name()
    features_candidates = [
        run_scoped_lake_path(
            data_root, run_id, "features", "perp", symbol, timeframe, feature_dataset
        ),
        data_root / "lake" / "features" / "perp" / symbol / timeframe / feature_dataset,
    ]
    features_dir = choose_partition_dir(features_candidates)
    if features_dir:
        df_feats = read_parquet(list_parquet_files(features_dir))
        if not df_feats.empty:
            ignored_nan_columns, ignored_constant_columns = _ignored_feature_columns(
                data_root=data_root,
                run_id=run_id,
                symbol=symbol,
                timeframe=timeframe,
            )
            feature_quality_summary = summarize_feature_quality(df_feats)
            feat_issues = (
                check_nans(df_feats, threshold=nan_threshold, ignored_columns=ignored_nan_columns)
                + check_constant_values(df_feats, ignored_columns=ignored_constant_columns)
                + check_outliers(df_feats, z_threshold=z_threshold)
            )

            # Detect feature drift
            drift_flags = detect_feature_drift(df_feats, reference_distributions_path)
            _summarize_drift_flags(symbol, drift_flags)
            for flag in drift_flags:
                feat_issues.append(
                    f"Drift detected in '{flag['feature']}': KS p-value = {flag['p_value']:.4f}"
                )

            if feat_issues:
                symbol_issues["features"] = feat_issues
    if feature_quality_summary is not None:
        symbol_issues["feature_quality_summary"] = feature_quality_summary

    return symbol_issues


def main() -> int:
    parser = argparse.ArgumentParser(description="Research Production Grade: Data Integrity Gate")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--nan_threshold", type=float, default=0.05)
    parser.add_argument("--z_threshold", type=float, default=10.0)
    parser.add_argument("--fail_on_issues", type=int, default=1)
    parser.add_argument("--timeframe", default="5m")
    args = parser.parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    data_root = get_data_root()

    manifest = start_manifest(
        f"validate_feature_integrity_{args.timeframe}", args.run_id, vars(args), [], []
    )

    all_issues = {}
    for symbol in symbols:
        LOGGER.info(f"Auditing data integrity for {symbol} on {args.timeframe}...")
        issues = validate_symbol(
            data_root,
            args.run_id,
            symbol,
            timeframe=args.timeframe,
            nan_threshold=float(args.nan_threshold),
            z_threshold=float(args.z_threshold),
        )
        if issues:
            all_issues[symbol] = issues

    status = "success"
    if all_issues:
        LOGGER.warning(f"Integrity check found issues in {len(all_issues)} symbols.")
        status = "failed" if int(args.fail_on_issues) else "warning"

    report_path = _report_path(data_root, run_id=args.run_id, timeframe=args.timeframe)
    _write_report(
        report_path,
        {
            "schema_version": "feature_integrity_report_v1",
            "run_id": args.run_id,
            "timeframe": args.timeframe,
            "nan_threshold": args.nan_threshold,
            "z_threshold": args.z_threshold,
            "fail_on_issues": int(args.fail_on_issues),
            "status": status,
            "symbols": all_issues,
        },
    )

    finalize_manifest(
        manifest,
        status,
        stats={
            "symbols_with_issues": len(all_issues),
            "report_path": str(report_path),
            "details": all_issues,
        },
    )
    return 1 if all_issues and int(args.fail_on_issues) else 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
