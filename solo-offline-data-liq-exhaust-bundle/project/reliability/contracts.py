from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, Sequence

import pandas as pd

from project.io.utils import read_parquet
from project.reliability.manifest_checks import (
    validate_manifest_artifacts_exist,
    validate_manifest_core,
)
from project.reliability.schemas import (
    ArtifactSchemaSpec,
    CANDIDATE_TABLE_SCHEMA,
    EVIDENCE_BUNDLE_SUMMARY_SCHEMA,
    PORTFOLIO_LEDGER_SCHEMA,
    PROMOTION_AUDIT_SCHEMA,
    PROMOTION_DECISION_SCHEMA,
    STRATEGY_TRACE_SCHEMA,
)


def _load_df(df_or_path: pd.DataFrame | Path | str) -> pd.DataFrame:
    if isinstance(df_or_path, pd.DataFrame):
        return df_or_path.copy()
    return read_parquet(Path(df_or_path))


def _normalize_portfolio_columns(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    rename_map = {
        "portfolio_gross_pnl": "gross_pnl",
        "portfolio_net_pnl": "net_pnl",
        "portfolio_equity": "equity",
        "portfolio_equity_return": "equity_return",
        "portfolio_gross_exposure": "gross_exposure",
        "portfolio_net_exposure": "net_exposure",
        "portfolio_turnover": "turnover",
    }
    available = {
        k: v for k, v in rename_map.items() if k in frame.columns and v not in frame.columns
    }
    if available:
        frame = frame.rename(columns=available)
    return frame


def _validate_dataframe(df: pd.DataFrame, spec: ArtifactSchemaSpec) -> pd.DataFrame:
    frame = df.copy()
    missing = [c for c in spec.required_columns if c not in frame.columns]
    if missing:
        raise ValueError(f"{spec.artifact_type} missing required columns: {missing}")
    for col in spec.non_null_columns:
        if col in frame.columns and frame[col].isna().any():
            raise ValueError(f"{spec.artifact_type} has null values in required column {col}")
    for col in spec.monotonic_by:
        if col in frame.columns:
            series = pd.to_datetime(frame[col], utc=True, errors="coerce")
            if series.isna().any() or not series.is_monotonic_increasing:
                raise ValueError(
                    f"{spec.artifact_type} column {col} must be monotonic increasing timestamps"
                )
    if spec.unique_by:
        subset = [c for c in spec.unique_by if c in frame.columns]
        if subset and frame.duplicated(subset=subset).any():
            raise ValueError(f"{spec.artifact_type} has duplicate rows for unique key {subset}")
    for col, allowed in spec.enum_columns.items():
        if col in frame.columns:
            bad = set(frame[col].dropna().astype(str)) - {str(v) for v in allowed}
            if bad:
                raise ValueError(
                    f"{spec.artifact_type} column {col} has invalid values: {sorted(bad)}"
                )
    return frame


def validate_strategy_trace(df_or_path: pd.DataFrame | Path | str) -> pd.DataFrame:
    return _validate_dataframe(_load_df(df_or_path), STRATEGY_TRACE_SCHEMA)


def validate_portfolio_ledger(df_or_path: pd.DataFrame | Path | str) -> pd.DataFrame:
    return _validate_dataframe(
        _normalize_portfolio_columns(_load_df(df_or_path)), PORTFOLIO_LEDGER_SCHEMA
    )


def validate_candidate_table(df_or_path: pd.DataFrame | Path | str) -> pd.DataFrame:
    return _validate_dataframe(_load_df(df_or_path), CANDIDATE_TABLE_SCHEMA)


def validate_promotion_audit(df_or_path: pd.DataFrame | Path | str) -> pd.DataFrame:
    return _validate_dataframe(_load_df(df_or_path), PROMOTION_AUDIT_SCHEMA)


def validate_evidence_bundle_summary(df_or_path: pd.DataFrame | Path | str) -> pd.DataFrame:
    return _validate_dataframe(_load_df(df_or_path), EVIDENCE_BUNDLE_SUMMARY_SCHEMA)


def validate_promotion_decisions(df_or_path: pd.DataFrame | Path | str) -> pd.DataFrame:
    return _validate_dataframe(_load_df(df_or_path), PROMOTION_DECISION_SCHEMA)


def validate_manifest(manifest_or_path: Dict[str, Any] | Path | str) -> Dict[str, Any]:
    return validate_manifest_artifacts_exist(manifest_or_path)


def reconcile_bundle_outputs(
    bundle_jsonl_path: Path | str, summary_df_or_path: pd.DataFrame | Path | str
) -> dict[str, int]:
    bundle_path = Path(bundle_jsonl_path)
    lines = [
        json.loads(line)
        for line in bundle_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    summary = validate_evidence_bundle_summary(summary_df_or_path)
    if len(lines) != len(summary):
        raise ValueError(f"bundle count mismatch: jsonl={len(lines)} summary={len(summary)}")
    line_ids = sorted(str(item.get("candidate_id", "")) for item in lines)
    summary_ids = sorted(summary["candidate_id"].astype(str).tolist())
    if line_ids != summary_ids:
        raise ValueError("bundle candidate_id mismatch between JSONL and summary")
    return {"bundle_count": len(lines), "summary_count": len(summary)}


def reconcile_portfolio_to_traces(
    portfolio_df_or_path: pd.DataFrame | Path | str,
    trace_dfs_or_paths: Sequence[pd.DataFrame | Path | str],
) -> dict[str, float]:
    portfolio = validate_portfolio_ledger(portfolio_df_or_path)
    traces = [validate_strategy_trace(item) for item in trace_dfs_or_paths]
    if not traces:
        return {
            "portfolio_total_net_pnl": float(portfolio["net_pnl"].sum()),
            "trace_total_net_pnl": 0.0,
        }
    total = None
    for trace in traces:
        grp = trace.groupby("timestamp", sort=True)["net_pnl"].sum()
        total = grp if total is None else total.add(grp, fill_value=0.0)
    port = portfolio.set_index("timestamp")["net_pnl"]
    aligned = port.to_frame("portfolio").join(total.rename("traces"), how="outer").fillna(0.0)
    if not aligned.empty and (aligned["portfolio"] - aligned["traces"]).abs().max() > 1e-8:
        raise ValueError("portfolio net_pnl does not reconcile to trace totals")
    return {
        "portfolio_total_net_pnl": float(aligned["portfolio"].sum()),
        "trace_total_net_pnl": float(aligned["traces"].sum()),
    }


def reconcile_promoted_candidates(
    candidate_df_or_path: pd.DataFrame | Path | str,
    promotion_audit_df_or_path: pd.DataFrame | Path | str,
) -> dict[str, int]:
    candidates = validate_candidate_table(candidate_df_or_path)
    audit = validate_promotion_audit(promotion_audit_df_or_path)
    promoted = audit[audit["promotion_decision"].astype(str) == "promoted"]
    candidate_ids = set(candidates["candidate_id"].astype(str))
    missing = sorted(set(promoted["candidate_id"].astype(str)) - candidate_ids)
    if missing:
        raise ValueError(f"promoted candidates missing from candidate table: {missing}")
    return {"candidate_rows": int(len(candidates)), "promoted_rows": int(len(promoted))}


def validate_promotion_artifacts(root: Path | str) -> dict[str, Any]:
    root = Path(root)
    # Accept both names: promotion_audit (current) and promotion_statistical_audit (legacy)
    audit_path = next(iter(sorted(root.glob("promotion_statistical_audit.*"))), None)
    if audit_path is None:
        audit_path = next(iter(sorted(root.glob("promotion_audit.*"))), None)
    promoted_path = next(iter(sorted(root.glob("promoted_candidates.*"))), None)
    bundle_summary_path = next(iter(sorted(root.glob("evidence_bundle_summary.*"))), None)
    decisions_path = next(iter(sorted(root.glob("promotion_decisions.*"))), None)
    bundle_jsonl = root / "evidence_bundles.jsonl"
    if (
        audit_path is None
        or promoted_path is None
        or bundle_summary_path is None
        or decisions_path is None
        or not bundle_jsonl.exists()
    ):
        raise FileNotFoundError("promotion artifact bundle incomplete")
    audit = validate_promotion_audit(audit_path)
    promoted = _load_df(promoted_path)
    bundle_summary = validate_evidence_bundle_summary(bundle_summary_path)
    decisions = validate_promotion_decisions(decisions_path)
    bundle_info = reconcile_bundle_outputs(bundle_jsonl, bundle_summary)
    promoted_ids = set(promoted.get("candidate_id", pd.Series(dtype=str)).astype(str))
    audit_ids = set(audit.get("candidate_id", pd.Series(dtype=str)).astype(str))
    missing = sorted(promoted_ids - audit_ids)
    if missing:
        raise ValueError(f"promoted candidates missing from promotion audit: {missing}")
    return {
        "audit_rows": int(len(audit)),
        "promoted_rows": int(len(promoted)),
        "bundle_rows": bundle_info["bundle_count"],
        "decision_rows": int(len(decisions)),
        "promoted_in_audit": int(len(promoted_ids)),
    }
