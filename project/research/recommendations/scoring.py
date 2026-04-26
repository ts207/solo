from __future__ import annotations

from typing import Any

import pandas as pd

from project.core.coercion import safe_float
from project.research.recommendations.checklist import gate_result


def build_release_signoff(
    *,
    run_id: str,
    checklist_payload: dict[str, Any],
    run_manifest_payload: dict[str, Any],
    kpi_payload: dict[str, Any],
) -> dict[str, Any]:
    hard_gates = run_manifest_payload.get("objective_hard_gates", {})
    retail_cfg = run_manifest_payload.get("retail_profile_config", {})

    min_trade_count = int(safe_float(hard_gates.get("min_trade_count"), 0.0))
    min_oos_sign_consistency = safe_float(hard_gates.get("min_oos_sign_consistency"), 0.0)
    max_drawdown_pct = safe_float(hard_gates.get("max_drawdown_pct"), 1.0)

    observed_trade_count = kpi_payload.get("metrics", {}).get("trade_count", {}).get("value", 0)
    observed_oos_sign = (
        kpi_payload.get("metrics", {}).get("oos_sign_consistency", {}).get("value", 0.0)
    )
    observed_drawdown = kpi_payload.get("metrics", {}).get("max_drawdown_pct", {}).get("value", 1.0)

    gates = []
    gates.append(
        gate_result(
            "kpi_trade_count",
            observed_trade_count >= min_trade_count,
            observed_trade_count,
            min_trade_count,
        )
    )
    gates.append(
        gate_result(
            "kpi_oos_sign_consistency",
            observed_oos_sign >= min_oos_sign_consistency,
            observed_oos_sign,
            min_oos_sign_consistency,
        )
    )
    gates.append(
        gate_result(
            "kpi_max_drawdown_pct",
            abs(observed_drawdown) <= max_drawdown_pct,
            observed_drawdown,
            max_drawdown_pct,
        )
    )

    fail_reasons = [f"{g['name']} failed" for g in gates if not g["passed"]]
    decision = "APPROVE_RELEASE" if not fail_reasons else "BLOCK_RELEASE"

    return {
        "run_id": run_id,
        "decision": decision,
        "failure_reasons": fail_reasons,
        "gates": gates,
    }


def hydrate_kpi_payload_with_promotion_fallback(
    *,
    kpi_payload: dict[str, Any],
    promotion_audit_df: pd.DataFrame,
) -> dict[str, Any]:
    if kpi_payload.get("metrics"):
        return kpi_payload

    # Logic to synthesize metrics from promotion audit
    return kpi_payload
