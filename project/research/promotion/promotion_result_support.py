from __future__ import annotations

from typing import Any

import numpy as np

from project.research.promotion.promotion_eligibility import _ReasonRecorder
from project.research.promotion.promotion_scoring import calculate_promotion_score


def _assemble_promotion_result(
    *,
    reasons: _ReasonRecorder,
    q_value: float,
    n_events: int,
    tob_pass: bool,
    require_retail_viability: bool,
    require_low_capital_viability: bool,
    enforce_baseline_beats_complexity: bool,
    enforce_placebo_controls: bool,
    enforce_timeframe_consensus: bool,
    statistical_pass: bool,
    cost_pass: bool,
    beats_baseline: bool,
    placebo_pass: bool,
    stability_pass: bool,
    timeframe_consensus_pass: bool,
    oos_pass: bool | None,
    microstructure_pass: bool,
    stressed_cost_pass: bool,
    delayed_entry_pass: bool,
    continuation_quality_pass: bool,
    multiplicity_diag_pass: bool,
    audit_pass: bool,
    dsr_pass: bool,
    multiplicity_pass: bool,
    robustness_pass: bool,
    regime_pass: bool,
    retail_viability_pass: bool,
    low_capital_viability_pass: bool,
    q_value_family: float,
    q_value_cluster: float,
    q_value_by: float,
    q_value_program: float,
    ss: float,
    sc: float,
    csr: float,
    control_pass: bool,
    control_rate: float | None,
    control_rate_source: str,
    tob_coverage: float,
    net_expectancy_bps: float,
    effective_cost_bps: float,
    turnover_proxy_mean: float,
    audit_statuses: list[str],
    net_expectancy_pass: bool,
    cost_budget_pass: bool,
    turnover_pass: bool,
    dsr_value: float,
    shrinkage_loso_stable: bool,
    shrinkage_borrowing_dominant: bool,
    structural_robustness_score: float,
    repeated_fold_consistency: float,
    robustness_panel_complete: bool,
    num_regimes: int,
    regime_stability_pass: bool,
    structural_break_pass: bool,
    validation_samples_raw: float,
    test_samples_raw: float,
    validation_samples_effective: int,
    test_samples_effective: int,
    oos_sample_source: str,
    oos_direction_match: bool,
    min_validation_events_required: int,
    min_test_events_required: int,
    low_capital_viability_score: float,
    low_capital_reject_codes: list[str],
    run_mode_normalized: str,
    is_deploy_mode: bool,
    is_descriptive: bool,
    is_trade_trigger: bool,
    max_q_value: float,
    promotion_profile: str,
    is_reduced_evidence: bool = False,
    benchmark_pass: bool = True,
    sensitivity_pass: bool = True,
    cell_origin_pass: bool = True,
) -> dict[str, Any]:
    # In non-deploy modes, missing OOS evidence is visible via `oos_pass_state` but
    # does not block promotion. Deploy-mode enforcement still happens upstream.
    oos_pass_for_gate: bool = (
        True if (oos_pass is None and not bool(is_deploy_mode)) else bool(oos_pass)
    )
    promoted = bool(
        statistical_pass
        and cost_pass
        and (beats_baseline or not bool(enforce_baseline_beats_complexity))
        and (placebo_pass or not bool(enforce_placebo_controls))
        and stability_pass
        and (timeframe_consensus_pass or not bool(enforce_timeframe_consensus))
        and oos_pass_for_gate
        and microstructure_pass
        and stressed_cost_pass
        and delayed_entry_pass
        and continuation_quality_pass
        and multiplicity_diag_pass
        and audit_pass
        and dsr_pass
        and multiplicity_pass
        and robustness_pass
        and regime_pass
        and benchmark_pass
        and sensitivity_pass
        and cell_origin_pass
        and (retail_viability_pass or not bool(require_retail_viability))
        and (low_capital_viability_pass or not bool(require_low_capital_viability))
    )

    promotion_track = "standard" if (promoted and tob_pass) else "fallback_only"
    promotion_decision = "promoted" if promoted else "rejected"
    promotion_score = calculate_promotion_score(
        statistical_pass=statistical_pass,
        stability_pass=stability_pass,
        cost_pass=cost_pass,
        tob_pass=tob_pass,
        oos_pass=oos_pass_for_gate,
        multiplicity_pass=multiplicity_pass,
        placebo_pass=placebo_pass,
        timeframe_consensus_pass=timeframe_consensus_pass,
    )
    primary_promo_fail = reasons.primary_promo_fail()
    fallback_used = promotion_track != "standard"
    fallback_reason = "" if not fallback_used else (primary_promo_fail or "non_standard_track")

    # Sprint 4: Explicit Promotion Layer Fields
    # Suggested classes: paper_promoted, production_promoted
    # Suggested readiness: research_inventory, paper_ready, live_review_required, live_ready

    promo_class = "paper_promoted"
    if promotion_profile == "deploy":
        promo_class = "production_promoted"

    readiness = "research_inventory"
    if promoted:
        if promo_class == "production_promoted":
            readiness = "live_review_required"
        else:
            readiness = "paper_ready"

    deploy_state_default = "paper_only"
    if promo_class == "production_promoted" and promoted:
        deploy_state_default = "live_eligible"

    inventory_reason_code = ""
    if not promoted:
        inventory_reason_code = (
            f"failed_{primary_promo_fail}"
            if primary_promo_fail
            else "below_promotion_threshold"
        )

    return {
        "promotion_decision": promotion_decision,
        "promotion_profile": str(promotion_profile),
        "promotion_track": promotion_track,
        "promotion_class": promo_class,
        "readiness_status": readiness,
        "inventory_reason_code": inventory_reason_code,
        "deployment_state_default": deploy_state_default,
        "fallback_used": bool(fallback_used),
        "fallback_reason": str(fallback_reason),
        "promotion_score": float(promotion_score),
        "reject_reason": reasons.unique_reject_reason_str(),
        "promotion_fail_gate_primary": primary_promo_fail,
        "promotion_fail_reason_primary": f"failed_{primary_promo_fail}"
        if primary_promo_fail
        else "",
        "run_mode_normalized": run_mode_normalized,
        "is_deploy_mode": bool(is_deploy_mode),
        "is_reduced_evidence": bool(is_reduced_evidence),
        "deploy_only_reject_reason": reasons.unique_deploy_only_reject_reason_str(),
        "reject_reason_categories_json": reasons.categorized_reject_json(),
        "promotion_fail_reason_categories_json": reasons.categorized_promo_fail_json(),
        "q_value": float(q_value),
        "q_value_family": float(q_value_family),
        "q_value_cluster": float(q_value_cluster),
        "q_value_by": float(q_value_by),
        "q_value_program": float(q_value_program),
        "n_events": int(n_events),
        "stability_score": float(ss),
        "sign_consistency": float(sc),
        "cost_survival_ratio": float(csr),
        "control_pass_rate": None if control_rate is None else float(control_rate),
        "control_rate_source": control_rate_source,
        "tob_coverage": float(tob_coverage),
        "validation_samples_raw": None
        if not np.isfinite(validation_samples_raw)
        else float(validation_samples_raw),
        "test_samples_raw": None if not np.isfinite(test_samples_raw) else float(test_samples_raw),
        "validation_samples": int(validation_samples_effective),
        "test_samples": int(test_samples_effective),
        "oos_sample_source": str(oos_sample_source),
        "oos_direction_match": bool(oos_direction_match),
        # Phase 1.4: expose oos_evaluated so audit output distinguishes
        # "passed OOS" from "OOS not checked" — previously both showed as "pass".
        "oos_evaluated": bool(oos_pass is not None),
        "oos_pass_state": (
            "not_evaluated" if oos_pass is None
            else ("pass" if oos_pass else "fail")
        ),
        "promotion_oos_min_validation_events": int(min_validation_events_required),
        "promotion_oos_min_test_events": int(min_test_events_required),
        "net_expectancy_bps": float(net_expectancy_bps),
        "effective_cost_bps": None
        if not np.isfinite(effective_cost_bps)
        else float(effective_cost_bps),
        "turnover_proxy_mean": None
        if not np.isfinite(turnover_proxy_mean)
        else float(turnover_proxy_mean),
        "audit_statuses": audit_statuses,
        "gate_promo_statistical": "pass" if statistical_pass else "fail",
        "gate_promo_multiplicity_diagnostics": "pass" if multiplicity_diag_pass else "fail",
        "gate_promo_multiplicity_cluster": "pass"
        if (np.isfinite(q_value_cluster) and (q_value_cluster <= float(max_q_value)))
        else "fail",
        "gate_promo_multiplicity_confirmatory": "pass" if multiplicity_pass else "fail",
        "gate_promo_stability": "pass" if stability_pass else "fail",
        "gate_promo_cost_survival": "pass" if cost_pass else "fail",
        "gate_promo_negative_control": "pass" if control_pass else "fail",
        "gate_promo_falsification": "pass" if (control_pass and placebo_pass) else "fail",
        "gate_promo_hypothesis_audit": "pass" if audit_pass else "fail",
        "gate_promo_tob_coverage": "pass" if tob_pass else "fail",
        "gate_promo_oos_validation": "pass" if oos_pass_for_gate else "fail",
        "gate_promo_microstructure": "pass" if microstructure_pass else "fail",
        "gate_promo_retail_net_expectancy": bool(net_expectancy_pass),
        "gate_promo_retail_cost_budget": bool(cost_budget_pass),
        "gate_promo_retail_turnover": bool(turnover_pass),
        "gate_promo_retail_viability": "pass" if retail_viability_pass else "fail",
        "gate_promo_low_capital_viability": bool(low_capital_viability_pass),
        "low_capital_viability_score": None
        if not np.isfinite(low_capital_viability_score)
        else float(low_capital_viability_score),
        "low_capital_reject_reason_codes": ",".join(low_capital_reject_codes),
        "dsr_value": float(dsr_value),
        "gate_promo_dsr": bool(dsr_pass),
        "shrinkage_loso_stable": bool(shrinkage_loso_stable),
        "shrinkage_borrowing_dominant": bool(shrinkage_borrowing_dominant),
        "structural_robustness_score": float(structural_robustness_score),
        "repeated_fold_consistency": float(repeated_fold_consistency),
        "robustness_panel_complete": bool(robustness_panel_complete),
        "gate_promo_robustness": "pass" if robustness_pass else "fail",
        "gate_promo_regime": "pass" if regime_pass else "fail",
        "gate_regime_stability": "pass" if regime_stability_pass else "fail",
        "num_regimes_supported": int(num_regimes),
        "gate_structural_break": "pass" if structural_break_pass else "fail",
        "gate_promo_baseline_beats_complexity": bool(beats_baseline),
        "gate_promo_placebo_controls": bool(placebo_pass),
        "gate_promo_event_discipline": "pass"
        if (not is_descriptive and is_trade_trigger)
        else "fail",
        "gate_promo_stressed_cost_survival": "pass" if stressed_cost_pass else "fail",
        "gate_promo_delayed_entry_stress": "pass" if delayed_entry_pass else "fail",
        "gate_promo_timeframe_consensus": "pass" if timeframe_consensus_pass else "fail",
        "gate_promo_continuation_quality": "pass" if continuation_quality_pass else "fail",
        "gate_promo_benchmark_certification": "pass" if benchmark_pass else "fail",
        "gate_promo_sensitivity": "pass" if sensitivity_pass else "fail",
        "gate_promo_cell_origin": "pass" if cell_origin_pass else "fail",
    }
