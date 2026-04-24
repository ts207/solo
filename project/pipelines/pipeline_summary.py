from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from project.pipelines.pipeline_defaults import (
    DATA_ROOT,
    read_table_auto,
    utc_now_iso,
)


def _data_root() -> Path:
    return Path(os.getenv("BACKTEST_DATA_ROOT", str(DATA_ROOT)))


def load_kpi_source_frame(run_id: str) -> Tuple[Optional[Any], Optional[str], Optional[Path]]:
    """Searches for a KPI source frame in data/reports."""
    report_dir = _data_root() / "reports"
    if not report_dir.exists():
        return None, None, None

    candidate_paths = [
        (
            report_dir / "promotions" / run_id / "promotion_statistical_audit.parquet",
            "promotion_audit",
        ),
        (
            report_dir / "promotions" / run_id / "promotion_statistical_audit.csv",
            "promotion_audit",
        ),
        (report_dir / "promotions" / run_id / "promotion_audit.parquet", "promotion_audit"),
        (report_dir / "promotions" / run_id / "promotion_audit.csv", "promotion_audit"),
        (report_dir / "promotions" / run_id / "promoted_candidates.parquet", "promoted_candidates"),
        (report_dir / "promotions" / run_id / "promoted_candidates.csv", "promoted_candidates"),
        (
            report_dir / "edge_candidates" / run_id / "edge_candidates_normalized.parquet",
            "edge_candidates",
        ),
        (
            report_dir / "edge_candidates" / run_id / "edge_candidates_normalized.csv",
            "edge_candidates",
        ),
        (report_dir / f"promoted_candidates_{run_id}.parquet", "promoted_candidates"),
        (report_dir / f"promotion_audit_{run_id}.csv", "promotion_audit"),
        (report_dir / f"edge_candidates_{run_id}.parquet", "edge_candidates"),
        (report_dir / f"edge_candidates_{run_id}.csv", "edge_candidates"),
    ]

    empty_match: Tuple[Optional[Any], Optional[str], Optional[Path]] = (None, None, None)
    for path, name in candidate_paths:
        df = read_table_auto(path)
        if df is None:
            continue
        if not df.empty:
            return df, name, path
        if empty_match[0] is None:
            empty_match = (df, name, path)

    return empty_match


def numeric_metric(df: Any, columns: List[str], *, aggregation: str) -> Dict[str, Any]:
    """Calculates a numeric metric from a DataFrame using specified columns and aggregation."""
    col = next((c for c in columns if c in df.columns), None)
    if col is None:
        return {"value": None, "column": "", "aggregation": aggregation, "sample_size": 0}

    series = df[col].dropna()
    if series.empty:
        return {"value": None, "column": col, "aggregation": aggregation, "sample_size": 0}

    try:
        if aggregation == "mean":
            val = series.mean()
        elif aggregation == "sum":
            val = series.sum()
        elif aggregation == "median":
            val = series.median()
        elif aggregation == "min":
            val = series.min()
        elif aggregation == "max":
            val = series.max()
        else:
            val = None

        return {
            "value": float(val) if val is not None else None,
            "column": col,
            "aggregation": aggregation,
            "sample_size": len(series),
        }
    except (ValueError, TypeError, AttributeError):
        return {
            "value": None,
            "column": col,
            "aggregation": aggregation,
            "sample_size": len(series),
        }


def bool_rate_metric(df: Any, columns: List[str]) -> Dict[str, Any]:
    """Calculates the mean rate of a boolean column."""
    col = next((c for c in columns if c in df.columns), None)
    if col is None:
        return {"value": None, "column": "", "aggregation": "mean_bool_rate", "sample_size": 0}

    series = df[col].dropna()
    if series.empty:
        return {"value": None, "column": col, "aggregation": "mean_bool_rate", "sample_size": 0}

    try:
        val = series.astype(bool).mean()
        return {
            "value": float(val),
            "column": col,
            "aggregation": "mean_bool_rate",
            "sample_size": len(series),
        }
    except (ValueError, TypeError, AttributeError):
        return {
            "value": None,
            "column": col,
            "aggregation": "mean_bool_rate",
            "sample_size": len(series),
        }


def write_run_kpi_scorecard(run_id: str, run_manifest: Dict[str, Any] | None = None) -> None:
    """Calculates and writes the KPI scorecard for a given run."""
    df, name, path = load_kpi_source_frame(run_id)
    if df is None:
        return

    source_name = str(name or "")
    completeness = "complete" if source_name == "promotion_audit" else "partial"
    scorecard = {
        "completeness": completeness,
        "source": {"name": source_name, "path": str(path)},
        "metrics": {
            "net_expectancy_bps": numeric_metric(
                df,
                [
                    "bridge_validation_stressed_after_cost_bps",
                    "net_expectancy_bps",
                    "net_expectancy",
                    "expectancy",
                ],
                aggregation="mean",
            ),
            "oos_sign_consistency": numeric_metric(
                df,
                ["sign_consistency", "oos_sign_consistency"],
                aggregation="mean",
            ),
            "turnover_proxy_mean": numeric_metric(
                df,
                ["turnover_proxy_mean"],
                aggregation="mean",
            ),
            "trade_count": numeric_metric(
                df, ["n_events", "trade_count", "n_trades"], aggregation="sum"
            ),
            "max_drawdown_pct": numeric_metric(
                df,
                ["naive_max_drawdown", "max_drawdown_pct"],
                aggregation="min",
            ),
            "win_rate": bool_rate_metric(df, ["is_win", "win"]),
            "edge_score": numeric_metric(df, ["edge_score", "score"], aggregation="mean"),
        },
        "generated_at": utc_now_iso(),
    }

    if run_manifest is not None:
        run_manifest["kpi_scorecard"] = scorecard

    out_path = _data_root() / "runs" / run_id / "kpi_scorecard.json"
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(scorecard, indent=2), encoding="utf-8")
    except OSError:
        pass


def print_artifact_summary(run_id: str) -> None:
    """Prints a summary of the found and missing artifacts for a given run."""
    print(f"\n--- Artifact Summary for {run_id} ---")
    data_root = _data_root()
    paths = {
        "Run Directory": data_root / "runs" / run_id,
        "Events Dir": data_root / "events" / run_id,
        "Reports Dir": data_root / "reports",
        "KPI Scorecard": data_root / "runs" / run_id / "kpi_scorecard.json",
    }
    for name, path in paths.items():
        status = "FOUND" if path.exists() else "MISSING"
        print(f"{name:20}: {status} ({path})")
    print("-" * 40 + "\n")
