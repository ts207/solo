from __future__ import annotations

import json
from typing import Any, Dict

import numpy as np

from project.research.promotion.promotion_gate_evaluators import (
    _confirmatory_deployable_gates,
    _confirmatory_shadow_gates,
    _evaluate_continuation_quality,
    _evaluate_control_audit_and_dsr,
    _evaluate_deploy_oos_and_low_capital,
    _evaluate_market_execution_and_stability,
    _quiet_float,
    _quiet_int,
    evaluate_sensitivity_gate,
)
from project.research.utils.decision_safety import bool_gate


def _apply_bundle_policy_result(
    base_result: Dict[str, Any],
    bundle: Dict[str, Any],
    bundle_decision: Dict[str, Any],
) -> Dict[str, Any]:
    merged = dict(base_result)
    pre_bundle_decision = str(base_result.get("promotion_decision", "rejected"))
    pre_bundle_track = str(base_result.get("promotion_track", "fallback_only"))
    pre_bundle_score = float(base_result.get("promotion_score", 0.0) or 0.0)

    merged["bundle_version"] = str(bundle.get("bundle_version", ""))
    merged["policy_version"] = str(bundle.get("policy_version", ""))
    merged["evidence_bundle_json"] = json.dumps(bundle, sort_keys=True)
    merged["pre_bundle_promotion_decision"] = pre_bundle_decision
    merged["pre_bundle_promotion_track"] = pre_bundle_track
    merged["pre_bundle_promotion_score"] = pre_bundle_score
    merged["bundle_rejection_reasons"] = "|".join(
        sorted(set(map(str, bundle_decision.get("rejection_reasons", []))))
    )
    merged["regime_flip_flag"] = bool(
        bundle.get("stability_tests", {}).get("regime_flip_flag", False)
    )
    merged["cross_symbol_sign_consistency"] = _quiet_float(
        bundle.get("stability_tests", {}).get("cross_symbol_sign_consistency", np.nan),
        np.nan,
    )
    merged["rolling_instability_score"] = _quiet_float(
        bundle.get("stability_tests", {}).get("rolling_instability_score", np.nan),
        np.nan,
    )

    gate_map = {
        "statistical": "gate_promo_statistical",
        "multiplicity_diagnostics": "gate_promo_multiplicity_diagnostics",
        "multiplicity_confirmatory": "gate_promo_multiplicity_confirmatory",
        "stability": "gate_promo_stability",
        "negative_control": "gate_promo_negative_control",
        "falsification": "gate_promo_falsification",
        "cost_survival": "gate_promo_cost_survival",
        "microstructure": "gate_promo_microstructure",
        "stressed_cost_survival": "gate_promo_stressed_cost_survival",
        "delayed_entry_stress": "gate_promo_delayed_entry_stress",
        "baseline_beats_complexity": "gate_promo_baseline_beats_complexity",
        "placebo_controls": "gate_promo_placebo_controls",
        "timeframe_consensus": "gate_promo_timeframe_consensus",
        "oos_validation": "gate_promo_oos_validation",
        "hypothesis_audit": "gate_promo_hypothesis_audit",
        "retail_viability": "gate_promo_retail_viability",
        "low_capital_viability": "gate_promo_low_capital_viability",
        "event_discipline": "gate_promo_event_discipline",
        "tob_coverage": "gate_promo_tob_coverage",
        "dsr": "gate_promo_dsr",
        "robustness": "gate_promo_robustness",
        "regime": "gate_promo_regime",
    }
    gate_results = dict(bundle_decision.get("gate_results", {}))
    for src_key, dst_key in gate_map.items():
        if src_key in gate_results:
            merged[dst_key] = str(gate_results[src_key])

    merged["promotion_decision"] = str(
        bundle_decision.get("promotion_status", merged.get("promotion_decision", "rejected"))
    )
    merged["promotion_track"] = str(
        bundle_decision.get("promotion_track", merged.get("promotion_track", "fallback_only"))
    )
    merged["promotion_score"] = float(
        bundle_decision.get("rank_score", merged.get("promotion_score", 0.0))
    )
    merged["bundle_policy_overrode_decision"] = bool(
        merged["promotion_decision"] != pre_bundle_decision
    )
    merged["bundle_policy_overrode_track"] = bool(merged["promotion_track"] != pre_bundle_track)
    merged["bundle_policy_overrode_score"] = bool(
        float(merged["promotion_score"]) != pre_bundle_score
    )

    combined_reasons = sorted(
        set(
            [r for r in str(merged.get("reject_reason", "")).split("|") if r]
            + list(bundle_decision.get("rejection_reasons", []))
        )
    )
    merged["reject_reason"] = "|".join(combined_reasons)
    primary_gate = str(merged.get("promotion_fail_gate_primary", "")).strip()
    if not primary_gate and merged["promotion_decision"] != "promoted":
        reasons = list(bundle_decision.get("rejection_reasons", []))
        if reasons:
            mapped = gate_map.get(str(reasons[0]))
            if mapped:
                merged["promotion_fail_gate_primary"] = mapped
                merged["promotion_fail_reason_primary"] = f"failed_{mapped}"
    merged["fallback_used"] = bool(merged.get("promotion_track", "fallback_only") != "standard")
    merged["fallback_reason"] = (
        ""
        if not merged["fallback_used"]
        else str(
            merged.get("fallback_reason")
            or merged.get("promotion_fail_gate_primary")
            or "non_standard_track"
        )
    )
    merged["promotion_audit"] = {
        key: value for key, value in merged.items() if key.startswith("gate_")
    }
    return merged


_BOOLEAN_COMPAT_GATES = {
    "gate_promo_dsr",
    "gate_promo_low_capital_viability",
    "gate_promo_baseline_beats_complexity",
    "gate_promo_placebo_controls",
}


def _restore_boolean_compat_gates(result: Dict[str, Any]) -> Dict[str, Any]:
    restored = dict(result)
    for key in _BOOLEAN_COMPAT_GATES:
        if key in restored:
            restored[key] = bool_gate(restored[key])
    audit = restored.get("promotion_audit")
    if isinstance(audit, dict):
        restored["promotion_audit"] = {
            key: (bool_gate(value) if key in _BOOLEAN_COMPAT_GATES else value)
            for key, value in audit.items()
        }
    return restored
