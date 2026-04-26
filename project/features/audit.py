from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from project.io.utils import write_parquet

LOGGER = logging.getLogger(__name__)


class FeatureAuditRegistry:
    """Registry for tracking feature provenance and staleness."""

    def __init__(self):
        self.metadata: dict[str, dict[str, Any]] = {}
        self.audit_records: list[pd.DataFrame] = []

    def record_join(
        self,
        *,
        feature_cols: list[str],
        source_table: str,
        source_ts_col: str,
        join_method: str = "asof_backward",
        join_tolerance: str = "1h",
        lookback_window: str | None = None,
        min_lag: int = 0,
        age_seconds: pd.Series,
        symbol: str,
        run_id: str,
    ):
        """Record metadata and age diagnostics for a feature join."""
        for col in feature_cols:
            self.metadata[col] = {
                "source_table": source_table,
                "source_ts_col": source_ts_col,
                "join_method": join_method,
                "join_tolerance": join_tolerance,
                "lookback_window": lookback_window,
                "min_lag": min_lag,
                "run_id": run_id,
            }

        audit_df = pd.DataFrame(
            {
                "run_id": run_id,
                "symbol": symbol,
                "feature_table": source_table,
                "age_seconds": age_seconds,
            }
        )
        # Add column names as context
        audit_df["features"] = ",".join(feature_cols)
        self.audit_records.append(audit_df)

    def get_summary(self) -> dict[str, Any]:
        if not self.audit_records:
            return {}

        full_audit = pd.concat(self.audit_records, ignore_index=True)
        summary = {"feature_manifest": self.metadata, "staleness_diagnostics": {}}

        for table, group in full_audit.groupby("feature_table"):
            ages = group["age_seconds"].dropna()
            if ages.empty:
                continue

            summary["staleness_diagnostics"][str(table)] = {
                "mean": float(ages.mean()),
                "median": float(ages.median()),
                "p90": float(ages.quantile(0.9)),
                "p99": float(ages.quantile(0.99)),
                "max": float(ages.max()),
                "stale_count_1h": int((ages > 3600).sum()),
                "stale_rate_1h": float((ages > 3600).mean()),
            }

        return summary

    def write_artifacts(self, out_dir: Path):
        """Write feature manifest and audit data to disk."""
        out_dir.mkdir(parents=True, exist_ok=True)

        # 1. Feature Manifest (JSON)
        with open(out_dir / "feature_manifest.json", "w") as f:
            json.dump(self.metadata, f, indent=2)

        # 2. Join Audit (Parquet)
        if self.audit_records:
            full_audit = pd.concat(self.audit_records, ignore_index=True)
            write_parquet(full_audit, out_dir / "feature_join_audit.parquet")


def compute_feature_age(
    events_ts: pd.Series,
    feature_ts: pd.Series,
) -> pd.Series:
    """Compute age of feature data at the point of join (seconds)."""
    # events_ts and feature_ts must be UTC datetime64[ns]
    return (events_ts - feature_ts).dt.total_seconds()


def enforce_staleness_thresholds(
    age_seconds: pd.Series,
    max_staleness_seconds: float,
    feature_name: str,
    run_mode: str = "research",
) -> None:
    """Reject features if excessive stale usage is detected in confirmatory mode."""
    stale_mask = age_seconds > max_staleness_seconds
    stale_rate = stale_mask.mean()

    if stale_rate > 0.05:  # Threshold: 5% stale rows
        msg = f"Feature {feature_name} has excessive stale usage: {stale_rate:.2%} > 5.00% (max_age={max_staleness_seconds}s)"
        is_promo = str(run_mode).lower() in {"production", "certification", "promotion", "deploy"}

        if is_promo:
            raise ValueError(msg)
        else:
            LOGGER.warning(msg)
