from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from project.core.coercion import as_bool, safe_int
from project.io.utils import atomic_write_json, atomic_write_text
from project.research.contracts.stat_regime import (
    ARTIFACT_AUDIT_VERSION_PHASE1_V1,
    AUDIT_STATUS_CURRENT,
    AUDIT_STATUS_DEGRADED,
    STAT_REGIME_POST_AUDIT,
)


def _empty_artifact_frame(*columns: str) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


_EMPTY_PROMOTION_AUDIT_COLUMNS = (
    "candidate_id",
    "event_type",
    "promotion_decision",
    "promotion_track",
    "policy_version",
    "bundle_version",
    "is_reduced_evidence",
    "gate_promo_statistical",
    "gate_promo_stability",
    "gate_promo_cost_survival",
    "gate_promo_negative_control",
)
_EMPTY_BUNDLE_SUMMARY_COLUMNS = (
    "candidate_id",
    "event_type",
    "promotion_decision",
    "promotion_track",
    "policy_version",
    "bundle_version",
    "is_reduced_evidence",
)
_EMPTY_PROMOTION_DECISION_COLUMNS = _EMPTY_BUNDLE_SUMMARY_COLUMNS


def _apply_artifact_audit_stamp(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        out = df.copy()
        out["stat_regime"] = pd.Series(dtype="object")
        out["audit_status"] = pd.Series(dtype="object")
        out["artifact_audit_version"] = pd.Series(dtype="object")
        return out
    out = df.copy()
    multiplicity_degraded = out.get(
        "multiplicity_scope_degraded", pd.Series(False, index=out.index)
    )
    if not isinstance(multiplicity_degraded, pd.Series):
        multiplicity_degraded = pd.Series(multiplicity_degraded, index=out.index)
    audit_status = multiplicity_degraded.astype(bool).apply(
        lambda x: AUDIT_STATUS_DEGRADED if x else AUDIT_STATUS_CURRENT
    )
    out["stat_regime"] = STAT_REGIME_POST_AUDIT
    out["audit_status"] = audit_status
    out["artifact_audit_version"] = ARTIFACT_AUDIT_VERSION_PHASE1_V1
    return out


def _write_promotion_lineage_audit(
    *,
    out_dir: Path,
    run_id: str,
    evidence_bundles: list[dict[str, Any]],
    promoted_df: pd.DataFrame,
    live_export_diagnostics: Mapping[str, Any] | None = None,
    historical_trust: Mapping[str, Any] | None = None,
) -> dict[str, str]:
    rows: list[dict[str, Any]] = []
    promoted_ids = {
        str(row.get("candidate_id", "")).strip()
        for row in promoted_df.to_dict(orient="records")
        if str(row.get("candidate_id", "")).strip()
    }
    for bundle in evidence_bundles:
        candidate_id = str(bundle.get("candidate_id", "")).strip()
        decision = (
            bundle.get("promotion_decision", {})
            if isinstance(bundle.get("promotion_decision", {}), dict)
            else {}
        )
        metadata = (
            bundle.get("metadata", {}) if isinstance(bundle.get("metadata", {}), dict) else {}
        )
        search_burden = (
            bundle.get("search_burden", {})
            if isinstance(bundle.get("search_burden", {}), dict)
            else {}
        )
        rows.append(
            {
                "run_id": run_id,
                "candidate_id": candidate_id,
                "event_type": str(bundle.get("event_type", "")).strip(),
                "promotion_status": str(decision.get("promotion_status", "")).strip(),
                "promotion_track": str(decision.get("promotion_track", "")).strip(),
                "bundle_version": str(bundle.get("bundle_version", "")).strip(),
                "policy_version": str(bundle.get("policy_version", "")).strip(),
                "hypothesis_id": str(metadata.get("hypothesis_id", "")).strip(),
                "plan_row_id": str(metadata.get("plan_row_id", "")).strip(),
                "program_id": str(metadata.get("program_id", "")).strip(),
                "campaign_id": str(metadata.get("campaign_id", "")).strip(),
                "live_exported": candidate_id in promoted_ids,
                "search_candidates_generated": safe_int(
                    search_burden.get("search_candidates_generated", 0), 0
                ),
                "search_candidates_eligible": safe_int(
                    search_burden.get("search_candidates_eligible", 0), 0
                ),
                "search_mutations_attempted": safe_int(
                    search_burden.get("search_mutations_attempted", 0), 0
                ),
                "search_family_count": safe_int(search_burden.get("search_family_count", 0), 0),
                "search_lineage_count": safe_int(search_burden.get("search_lineage_count", 0), 0),
                "search_burden_estimated": bool(
                    as_bool(search_burden.get("search_burden_estimated", False))
                ),
                "search_scope_version": str(search_burden.get("search_scope_version", "phase1_v1")),
            }
        )
    json_path = out_dir / "promotion_lineage_audit.json"
    md_path = out_dir / "promotion_lineage_audit.md"
    payload = {
        "schema_version": "promotion_lineage_audit_v1",
        "run_id": run_id,
        "rows": rows,
        "live_export": dict(live_export_diagnostics or {}),
        "historical_trust": dict(historical_trust or {}),
    }
    atomic_write_json(json_path, payload)
    md_lines = [
        "# Promotion lineage audit",
        "",
        f"- run_id: `{run_id}`",
        f"- evidence_bundle_count: `{len(evidence_bundles)}`",
        f"- live_exported_count: `{sum(1 for row in rows if row['live_exported'])}`",
        f"- live_thesis_store: `{str((live_export_diagnostics or {}).get('output_path', ''))}`",
        f"- live_contract_json: `{str((live_export_diagnostics or {}).get('contract_json_path', ''))}`",
        f"- live_contract_md: `{str((live_export_diagnostics or {}).get('contract_md_path', ''))}`",
        f"- historical_trust_status: `{str((historical_trust or {}).get('historical_trust_status', ''))}`",
        f"- canonical_reuse_allowed: `{bool((historical_trust or {}).get('canonical_reuse_allowed', False))}`",
        f"- compat_reuse_allowed: `{bool((historical_trust or {}).get('compat_reuse_allowed', False))}`",
        "",
        "| candidate_id | event_type | promotion_status | promotion_track | program_id | campaign_id | live_exported |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        md_lines.append(
            "| {candidate_id} | {event_type} | {promotion_status} | {promotion_track} | {program_id} | {campaign_id} | {live_exported} |".format(
                **row
            )
        )
    atomic_write_text(md_path, "\n".join(md_lines) + "\n")
    return {"json_path": str(json_path), "md_path": str(md_path)}


def _write_multiplicity_scope_diagnostics(out_dir: Path, diag: dict[str, Any]) -> dict[str, str]:
    json_path = out_dir / "multiplicity_scope_diagnostics.json"
    md_path = out_dir / "multiplicity_scope_diagnostics.md"

    atomic_write_json(json_path, diag)

    md_lines = [
        "# Multiplicity Scope Diagnostics",
        "",
        f"- scope_mode: `{diag.get('scope_mode', 'unknown')}`",
        f"- scope_version: `{diag.get('scope_version', 'unknown')}`",
        f"- program_id: `{diag.get('program_id', 'unknown')}`",
        f"- campaign_id: `{diag.get('campaign_id', 'none')}`",
        "",
        "## Candidate Counts",
        f"- current_candidates_total: `{diag.get('current_candidates_total', 0)}`",
        f"- historical_candidates_total: `{diag.get('historical_candidates_total', 0)}`",
        f"- combined_candidates_total: `{diag.get('combined_candidates_total', 0)}`",
        f"- scope_keys_unique: `{diag.get('scope_keys_unique', 0)}`",
        "",
        "## Multiplicity Statistics",
        f"- num_tests_scope_avg: `{diag.get('num_tests_scope_avg', 0.0):.2f}`",
        f"- effective_q_value_avg: `{diag.get('effective_q_value_avg', 0.0):.4f}`",
        "",
        "## Degradation Status",
        f"- scope_degraded_count: `{diag.get('scope_degraded_count', 0)}`",
    ]

    context_counts = diag.get("scope_context_counts", {})
    if context_counts:
        md_lines.extend(["", "## Context Breakdown"])
        for context, count in sorted(context_counts.items()):
            md_lines.append(f"- {context}: `{count}`")

    degraded_reasons = diag.get("scope_degraded_reason_counts", {})
    if degraded_reasons:
        md_lines.extend(["", "## Degraded Reasons"])
        for reason, count in sorted(degraded_reasons.items()):
            md_lines.append(f"- {reason}: `{count}`")

    md_lines.append("")
    atomic_write_text(md_path, "\n".join(md_lines) + "\n")

    return {"json_path": str(json_path), "md_path": str(md_path)}
