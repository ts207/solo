from __future__ import annotations

import json
from typing import Any

import pandas as pd

PROMOTION_AUDIT_SCHEMA_VERSION = "v1"
PROMOTION_SUMMARY_COLUMNS = [
    "candidate_id",
    "event_type",
    "stage",
    "statistic",
    "threshold",
    "pass_fail",
    "promotion_decision",
    "promotion_track",
    "fallback_used",
    "fallback_reason",
    "reject_reason",
    "promotion_fail_gate_primary",
]


def _trace_payload(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def normalize_promotion_trace_rows(audit_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if audit_df is None or audit_df.empty:
        return pd.DataFrame(columns=PROMOTION_SUMMARY_COLUMNS)

    for row in audit_df.to_dict(orient="records"):
        base = {
            "candidate_id": str(row.get("candidate_id", "")).strip(),
            "event_type": str(row.get("event_type", row.get("event", ""))).strip(),
            "promotion_decision": str(row.get("promotion_decision", "")).strip(),
            "promotion_track": str(row.get("promotion_track", "")).strip(),
            "fallback_used": bool(row.get("fallback_used", False)),
            "fallback_reason": str(row.get("fallback_reason", "")).strip(),
            "reject_reason": str(row.get("reject_reason", "")).strip(),
            "promotion_fail_gate_primary": str(row.get("promotion_fail_gate_primary", "")).strip(),
        }
        trace_payload = _trace_payload(row.get("promotion_metrics_trace", {}))
        for stage, meta in sorted(trace_payload.items()):
            meta = meta if isinstance(meta, dict) else {}
            rows.append(
                {
                    **base,
                    "stage": str(stage).strip(),
                    "statistic": json.dumps(meta.get("observed", {}), sort_keys=True),
                    "threshold": json.dumps(meta.get("thresholds", {}), sort_keys=True),
                    "pass_fail": bool(meta.get("passed", False)),
                }
            )

    return pd.DataFrame(rows, columns=PROMOTION_SUMMARY_COLUMNS)
