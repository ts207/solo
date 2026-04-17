from __future__ import annotations

import json
from collections import Counter
from typing import Any, Dict, List

import pandas as pd


def _trace_payload(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return {}
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, dict) else {}
    return {}


def _failed_stages_from_trace(raw: Any) -> List[str]:
    payload = _trace_payload(raw)
    failed: List[str] = []
    for stage, meta in payload.items():
        if not isinstance(meta, dict):
            continue
        if meta.get("passed") is False:
            failed.append(str(stage))
    return failed


def _primary_reject_reason(row: Dict[str, Any]) -> str:
    primary = str(row.get("promotion_fail_reason_primary", "")).strip()
    if primary:
        return primary
    reject_reason = str(row.get("reject_reason", "")).strip()
    if not reject_reason:
        return ""
    return next((token for token in reject_reason.split("|") if token.strip()), "")


def _classify_rejection(row: Dict[str, Any], failed_stages: List[str]) -> str:
    primary_gate = str(row.get("promotion_fail_gate_primary", "")).strip().lower()
    primary_reason = _primary_reject_reason(row).strip().lower()
    reject_reason = str(row.get("reject_reason", "")).strip().lower()
    weakest_fail_stage = str(row.get("weakest_fail_stage", "")).strip().lower()
    combined = " ".join(
        [primary_gate, primary_reason, reject_reason, weakest_fail_stage, " ".join(failed_stages)]
    )

    if any(
        token in combined
        for token in [
            "spec hash mismatch",
            "bridge_evaluation_failed",
            "unlocked candidates",
            "schema",
            "contract",
        ]
    ):
        return "contract_failure"
    if any(
        token in combined
        for token in [
            "negative_control_missing",
            "failed_placebo_controls",
            "hypothesis_audit",
            "missing_realized_oos_path",
            "oos_insufficient_samples",
            "oos_validation",
            "confirmatory",
            "validation",
            "test_support",
            "multiplicity_strict",
        ]
    ):
        return "weak_holdout_support"
    if any(
        token in combined
        for token in [
            "expectancy",
            "after_cost",
            "turnover",
            "retail",
            "low_capital",
            "dsr",
            "economic",
            "tradable",
        ]
    ):
        return "weak_economics"
    if any(
        token in combined
        for token in [
            "baseline",
            "complexity",
            "placebo",
            "timeframe_consensus",
            "overlap",
            "profile_correlation",
            "regime_unstable",
            "scope",
        ]
    ):
        return "scope_mismatch"
    if failed_stages:
        return "scope_mismatch"
    return "unclassified"


def _recommended_next_action_for_rejection(classification: str) -> str:
    mapping = {
        "contract_failure": "repair_pipeline",
        "weak_holdout_support": "run_confirmatory",
        "weak_economics": "stop_or_reframe",
        "scope_mismatch": "narrow_scope",
        "unclassified": "review_manually",
    }
    return mapping.get(str(classification).strip().lower(), "review_manually")


def _annotate_promotion_audit_decisions(audit_df: pd.DataFrame) -> pd.DataFrame:
    if audit_df.empty:
        out = audit_df.copy()
        out["primary_reject_reason"] = pd.Series(dtype="object")
        out["failed_gate_count"] = pd.Series(dtype="int64")
        out["failed_gate_list"] = pd.Series(dtype="object")
        out["weakest_fail_stage"] = pd.Series(dtype="object")
        return out

    rows: List[Dict[str, Any]] = []
    for row in audit_df.to_dict(orient="records"):
        failed_stages = _failed_stages_from_trace(row.get("promotion_metrics_trace", {}))
        primary_gate = str(row.get("promotion_fail_gate_primary", "")).strip()
        weakest_fail_stage = failed_stages[0] if failed_stages else primary_gate
        classification = _classify_rejection(row, failed_stages)
        rows.append(
            {
                **row,
                "primary_reject_reason": _primary_reject_reason(row),
                "failed_gate_count": int(len(failed_stages)),
                "failed_gate_list": "|".join(failed_stages),
                "weakest_fail_stage": weakest_fail_stage,
                "rejection_classification": classification,
                "recommended_next_action": _recommended_next_action_for_rejection(classification),
            }
        )
    return pd.DataFrame(rows)


def _build_promotion_decision_diagnostics(audit_df: pd.DataFrame) -> Dict[str, Any]:
    if audit_df.empty:
        return {
            "candidates_total": 0,
            "promoted_count": 0,
            "rejected_count": 0,
            "primary_fail_gate_counts": {},
            "primary_reject_reason_counts": {},
            "failed_stage_counts": {},
            "rejection_classification_counts": {},
            "recommended_next_action_counts": {},
            "mean_failed_gate_count_rejected": 0.0,
            "confirmatory_field_availability": {},
        }

    decision_counts = (
        audit_df.get("promotion_decision", pd.Series(dtype="object"))
        .astype(str)
        .value_counts()
        .to_dict()
    )
    rejected = audit_df[
        audit_df.get("promotion_decision", pd.Series(dtype="object")).astype(str) == "rejected"
    ].copy()
    fail_gates = (
        rejected.get("promotion_fail_gate_primary", pd.Series(dtype="object"))
        .astype(str)
        .str.strip()
    )
    fail_reasons = (
        rejected.get("primary_reject_reason", pd.Series(dtype="object")).astype(str).str.strip()
    )
    rejection_classes = (
        rejected.get("rejection_classification", pd.Series(dtype="object")).astype(str).str.strip()
    )
    next_actions = (
        rejected.get("recommended_next_action", pd.Series(dtype="object")).astype(str).str.strip()
    )
    stage_counter: Counter[str] = Counter()
    for value in rejected.get("failed_gate_list", pd.Series(dtype="object")).astype(str):
        for token in value.split("|"):
            token = token.strip()
            if token:
                stage_counter[token] += 1

    availability: Dict[str, Dict[str, int]] = {}
    field_names = [
        "plan_row_id",
        "has_realized_oos_path",
        "bridge_certified",
        "q_value_by",
        "q_value_cluster",
        "repeated_fold_consistency",
        "structural_robustness_score",
        "robustness_panel_complete",
        "gate_regime_stability",
        "gate_structural_break",
        "num_regimes_supported",
    ]
    for field in field_names:
        if field not in audit_df.columns:
            availability[field] = {"present": 0, "missing": int(len(audit_df))}
            continue
        series = audit_df[field]
        if series.dtype == bool:
            present_mask = pd.Series(True, index=series.index)
        elif pd.api.types.is_numeric_dtype(series):
            present_mask = pd.to_numeric(series, errors="coerce").notna()
        else:
            normalized = series.astype(str).str.strip().str.lower()
            present_mask = ~(series.isna() | normalized.isin({"", "nan", "none", "null"}))
        availability[field] = {
            "present": int(present_mask.sum()),
            "missing": int((~present_mask).sum()),
        }

    return {
        "candidates_total": int(len(audit_df)),
        "promoted_count": int(decision_counts.get("promoted", 0)),
        "rejected_count": int(decision_counts.get("rejected", 0)),
        "primary_fail_gate_counts": {
            str(k): int(v) for k, v in fail_gates[fail_gates != ""].value_counts().to_dict().items()
        },
        "primary_reject_reason_counts": {
            str(k): int(v)
            for k, v in fail_reasons[fail_reasons != ""].value_counts().to_dict().items()
        },
        "failed_stage_counts": dict(sorted(stage_counter.items())),
        "rejection_classification_counts": {
            str(k): int(v)
            for k, v in rejection_classes[rejection_classes != ""].value_counts().to_dict().items()
        },
        "recommended_next_action_counts": {
            str(k): int(v)
            for k, v in next_actions[next_actions != ""].value_counts().to_dict().items()
        },
        "mean_failed_gate_count_rejected": 0.0
        if rejected.empty
        else float(
            pd.to_numeric(rejected.get("failed_gate_count", 0), errors="coerce").fillna(0).mean()
        ),
        "confirmatory_field_availability": availability,
    }
