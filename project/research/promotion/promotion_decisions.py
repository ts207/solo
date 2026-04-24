from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import numpy as np

from project.core.coercion import as_bool
from project.core.exceptions import PromotionDecisionError
from project.events.governance import promotion_event_metadata
from project.research.promotion.promotion_decision_support import (
    _apply_bundle_policy_result,
    _evaluate_continuation_quality,
    _evaluate_control_audit_and_dsr,
    _evaluate_deploy_oos_and_low_capital,
    _evaluate_market_execution_and_stability,
    _quiet_int,
    _restore_boolean_compat_gates,
    evaluate_sensitivity_gate,
)
from project.research.promotion.promotion_eligibility import _ReasonRecorder
from project.research.promotion.promotion_result_support import _assemble_promotion_result
from project.research.promotion.promotion_scoring import _context_complexity_penalty
from project.research.promotion.promotion_thresholds import _build_bundle_policy
from project.research.utils.decision_safety import coerce_numeric_nan, finite_le
from project.research.validation.evidence_bundle import (
    build_evidence_bundle,
    evaluate_promotion_bundle,
    validate_evidence_bundle,
)

_CELL_ORIGIN_MODE = "edge_cells"
_CELL_ORIGIN_MAX_COMPLEXITY_PENALTY = 0.20
_CELL_ORIGIN_MAPPED_STATUSES = {
    "runtime_executable",
}
_CELL_ORIGIN_SUPPORTIVE_STATUSES = {
    "supportive_only_context_downgraded",
    "supportive_downgraded",
}


def _is_cell_origin_row(row: Dict[str, Any]) -> bool:
    return str(row.get("source_discovery_mode", "") or "").strip().lower() == _CELL_ORIGIN_MODE


def _has_explicit_runtime_mapping(row: Dict[str, Any]) -> bool:
    if as_bool(row.get("runtime_executable", False)):
        return True
    mapping_status = str(
        row.get("context_translation", row.get("runtime_mapping_status", ""))
        or ""
    ).strip()
    if mapping_status in _CELL_ORIGIN_MAPPED_STATUSES:
        return True
    if mapping_status in _CELL_ORIGIN_SUPPORTIVE_STATUSES:
        return bool(row.get("supportive_context") or row.get("supportive_context_json"))
    return False


def _evaluate_cell_origin_governance(
    row: Dict[str, Any],
    reasons: _ReasonRecorder,
) -> Dict[str, Any]:
    if not _is_cell_origin_row(row):
        return {
            "applies": False,
            "pass": True,
            "fail_reasons": [],
            "complexity_penalty": 0.0,
            "runtime_mapping_status": "",
        }

    fail_reasons: list[str] = []
    if not as_bool(row.get("is_representative", False)):
        fail_reasons.append("cell_origin_not_cluster_representative")
    if not as_bool(row.get("forward_pass", False)):
        fail_reasons.append("cell_origin_forward_missing")
    if not as_bool(row.get("contrast_pass", False)):
        fail_reasons.append("cell_origin_contrast_missing")

    runtime_mapped = _has_explicit_runtime_mapping(row)
    if not runtime_mapped:
        fail_reasons.append("cell_origin_runtime_mapping_missing")

    dimension_count = _quiet_int(
        row.get("context_dimension_count", row.get("context_dim_count", 0)),
        0,
    )
    complexity_penalty = _context_complexity_penalty(dimension_count)
    if complexity_penalty > _CELL_ORIGIN_MAX_COMPLEXITY_PENALTY:
        fail_reasons.append("cell_origin_complexity_excessive")

    if fail_reasons:
        for reason in fail_reasons:
            reasons.add_pair(
                reject_reason=reason,
                promo_fail_reason="gate_promo_cell_origin",
                category="cell_origin_governance",
            )

    mapping_status = str(
        row.get("context_translation", row.get("runtime_mapping_status", ""))
        or ("runtime_executable" if as_bool(row.get("runtime_executable", False)) else "")
    ).strip()
    return {
        "applies": True,
        "pass": not fail_reasons,
        "fail_reasons": fail_reasons,
        "complexity_penalty": float(complexity_penalty),
        "runtime_mapping_status": mapping_status,
    }


def _apply_cell_origin_authority(
    result: Dict[str, Any],
    cell_origin_eval: Dict[str, Any],
) -> Dict[str, Any]:
    out = dict(result)
    applies = bool(cell_origin_eval.get("applies", False))
    passed = bool(cell_origin_eval.get("pass", True))
    fail_reasons = [str(r) for r in cell_origin_eval.get("fail_reasons", []) if str(r)]

    out["cell_origin_governance_applies"] = applies
    out["cell_origin_pass"] = passed
    out["gate_promo_cell_origin"] = "pass" if passed else "fail"
    out["cell_origin_gate_reasons"] = "|".join(fail_reasons)
    out["cell_origin_complexity_penalty"] = float(
        cell_origin_eval.get("complexity_penalty", 0.0) or 0.0
    )
    out["cell_origin_runtime_mapping_status"] = str(
        cell_origin_eval.get("runtime_mapping_status", "") or ""
    )
    if applies and not passed:
        out["eligible"] = False
        out["promotion_status"] = "rejected"
        out["promotion_decision"] = "rejected"
        out["promotion_track"] = "fallback_only"
        out["fallback_used"] = True
        out["fallback_reason"] = "gate_promo_cell_origin"
        existing = [r for r in str(out.get("reject_reason", "")).split("|") if r]
        out["reject_reason"] = "|".join(sorted(set(existing + fail_reasons)))
        out["promotion_fail_gate_primary"] = (
            str(out.get("promotion_fail_gate_primary", "") or "")
            or "gate_promo_cell_origin"
        )
        out["promotion_fail_reason_primary"] = (
            str(out.get("promotion_fail_reason_primary", "") or "")
            or "failed_gate_promo_cell_origin"
        )
        rejection_reasons = list(out.get("rejection_reasons", []) or [])
        out["rejection_reasons"] = sorted(set(rejection_reasons + fail_reasons))
        gate_results = dict(out.get("gate_results", {}) or {})
        gate_results["cell_origin_governance"] = "fail"
        out["gate_results"] = gate_results
    elif applies:
        gate_results = dict(out.get("gate_results", {}) or {})
        gate_results.setdefault("cell_origin_governance", "pass")
        out["gate_results"] = gate_results
    audit = dict(out.get("promotion_audit", {}) or {})
    audit["gate_promo_cell_origin"] = out["gate_promo_cell_origin"]
    out["promotion_audit"] = audit
    return out


def _apply_authoritative_bundle_decision(
    result: Dict[str, Any],
    bundle: Dict[str, Any] | None,
    bundle_decision: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Apply bundle decision as authority for final promotion outcome fields.

    The bundle remains authoritative for final status/track/score, but
    row-level diagnostics stay attached so promotion audit consumers do not
    lose detailed reject codes or gate snapshots.
    """
    if bundle_decision is None:
        bundle_decision = dict(bundle or {})
        bundle = {}

    out = _apply_bundle_policy_result(result, bundle, bundle_decision)

    out["eligible"] = bool(bundle_decision["eligible"])
    out["promotion_status"] = str(bundle_decision["promotion_status"])
    out["promotion_decision"] = str(bundle_decision["promotion_status"])
    out["promotion_track"] = str(bundle_decision["promotion_track"])
    out["promotion_score"] = float(bundle_decision["rank_score"])
    out["rank_score"] = float(bundle_decision["rank_score"])
    out["rejection_reasons"] = list(bundle_decision.get("rejection_reasons", []))
    out["gate_results"] = dict(bundle_decision.get("gate_results", {}))

    return out


def evaluate_row(
    *,
    row: Dict[str, Any],
    hypothesis_index: Dict[str, Dict[str, Any]],
    negative_control_summary: Dict[str, Any],
    max_q_value: float,
    min_events: int,
    min_stability_score: float,
    min_sign_consistency: float,
    min_cost_survival_ratio: float,
    max_negative_control_pass_rate: float,
    min_tob_coverage: float,
    require_hypothesis_audit: bool,
    allow_missing_negative_controls: bool,
    min_net_expectancy_bps: float = 0.0,
    max_fee_plus_slippage_bps: float | None = None,
    max_daily_turnover_multiple: float | None = None,
    require_retail_viability: bool = False,
    require_low_capital_viability: bool = False,
    require_multiplicity_diagnostics: bool = False,
    min_dsr: float = 0.0,
    promotion_confirmatory_gates: Dict[str, Any] | None = None,
    promotion_profile: str = "deploy",
    enforce_baseline_beats_complexity: bool = True,
    enforce_placebo_controls: bool = True,
    enforce_timeframe_consensus: bool = True,
    enforce_regime_stability: bool = True,
    require_scope_level_multiplicity: bool = True,
    allow_multiplicity_scope_degraded: bool = True,
    use_effective_q_value: bool = True,
    policy_version: str = "phase4_pr5_v1",
    bundle_version: str = "phase4_bundle_v1",
    is_reduced_evidence: bool = False,
    benchmark_certification: Dict[str, Any] | None = None,
    run_id: str | None = None,
    data_root: Path | None = None,
) -> Dict[str, Any]:
    try:
        reasons = _ReasonRecorder.create()
        event_type = str(row.get("event_type", row.get("event", ""))).strip() or "UNKNOWN_EVENT"

        # Benchmark Certification Gate
        bench_pass = True
        if benchmark_certification:
            bench_pass = bool(benchmark_certification.get("passed", False))
            if not bench_pass:
                reasons.add_pair(
                    reject_reason=f"benchmark_{benchmark_certification.get('status', 'failed')}",
                    promo_fail_reason="gate_promo_benchmark_certification",
                    category="benchmark_integrity",
                )

        # Sensitivity Gate (Phase 2)
        sensitivity_max_score = 0.4
        if promotion_confirmatory_gates:
            sensitivity_max_score = float(
                promotion_confirmatory_gates.get("sensitivity_max_score", 0.4)
            )

        sensitivity_status, sensitivity_msg = "pass", ""
        if run_id and data_root:
            from pathlib import Path as _Path

            sensitivity_status, sensitivity_msg = evaluate_sensitivity_gate(
                row,
                run_id=run_id,
                data_root=_Path(data_root),
                max_sensitivity_score=sensitivity_max_score,
            )

        if sensitivity_status == "fail":
            reasons.add_reject(f"sensitivity_high ({sensitivity_msg})", category="stability")
            reasons.add_promo_fail("gate_promo_sensitivity", category="stability")

        sensitivity_pass = sensitivity_status == "pass"

        plan_row_id = str(row.get("plan_row_id", "")).strip()
        n_events = _quiet_int(row.get("n_events", row.get("sample_size", 0)), 0)
        q_value = coerce_numeric_nan(row.get("q_value"))
        q_value_program = coerce_numeric_nan(row.get("q_value_program"))

        governance = promotion_event_metadata(event_type, row)
        is_descriptive = bool(governance.get("event_is_descriptive", False))
        is_trade_trigger = bool(governance.get("event_is_trade_trigger", True))
        if is_descriptive or not is_trade_trigger:
            reasons.add_pair(
                reject_reason=(
                    str(governance.get("promotion_block_reason", "")).replace("=", "_")
                    or "descriptive_only_event"
                ),
                promo_fail_reason="gate_promo_event_discipline",
                category="event_discipline",
            )
        if bool(governance.get("requires_stronger_evidence", False)) and bool(is_reduced_evidence):
            reasons.add_pair(
                reject_reason="stronger_evidence_required",
                promo_fail_reason="gate_promo_event_discipline",
                category="event_discipline",
            )

        q_value_available = bool(np.isfinite(q_value))
        program_q_value_available = bool(np.isfinite(q_value_program))
        q_value_scope = coerce_numeric_nan(row.get("q_value_scope"))
        scope_q_value_available = bool(np.isfinite(q_value_scope))

        multiplicity_scope_degraded = as_bool(row.get("multiplicity_scope_degraded", False))
        use_effective_q = bool(use_effective_q_value)
        scope_metadata_present = any(
            [
                "q_value_scope" in row,
                "multiplicity_scope_mode" in row,
                "multiplicity_scope_key" in row,
                "multiplicity_scope_version" in row,
                "multiplicity_scope_degraded" in row,
            ]
        )

        upstream_effective_q_value = coerce_numeric_nan(row.get("effective_q_value"))
        values_for_effective_q = [
            v
            for v in [q_value, q_value_program, q_value_scope, upstream_effective_q_value]
            if np.isfinite(v)
        ]
        if values_for_effective_q:
            effective_q_value = max(values_for_effective_q)
        else:
            effective_q_value = q_value

        if use_effective_q:
            effective_q_value_for_check = effective_q_value
        else:
            effective_q_value_for_check = q_value if q_value_available else effective_q_value

        statistical_pass = (
            q_value_available
            and finite_le(effective_q_value_for_check, max_q_value)
            and (n_events >= int(min_events))
        )

        if not statistical_pass:
            if not q_value_available:
                reasons.add_pair(
                    reject_reason="statistical_missing_q_value",
                    promo_fail_reason="gate_promo_statistical_q_value",
                    category="statistical_significance",
                )
            elif program_q_value_available and not finite_le(q_value_program, max_q_value):
                reasons.add_pair(
                    reject_reason="statistical_program_q_value",
                    promo_fail_reason="gate_promo_statistical_program_q_value",
                    category="statistical_significance",
                )
            elif scope_q_value_available and not finite_le(q_value_scope, max_q_value):
                reasons.add_pair(
                    reject_reason="statistical_scope_q_value",
                    promo_fail_reason="gate_promo_multiplicity_scope",
                    category="statistical_significance",
                )
            reasons.add_pair(
                reject_reason="statistical_significance",
                promo_fail_reason="gate_promo_statistical",
                category="statistical_significance",
            )

        require_scope_multiplicity = bool(require_scope_level_multiplicity)

        if (
            require_scope_multiplicity
            and scope_metadata_present
            and not scope_q_value_available
            and not multiplicity_scope_degraded
        ):
            reasons.add_pair(
                reject_reason="multiplicity_scope_missing",
                promo_fail_reason="gate_promo_multiplicity_scope",
                category="multiplicity_scope",
            )
        elif (
            require_scope_multiplicity
            and scope_metadata_present
            and multiplicity_scope_degraded
            and not allow_multiplicity_scope_degraded
        ):
            reasons.add_pair(
                reject_reason="multiplicity_scope_degraded_not_allowed",
                promo_fail_reason="gate_promo_multiplicity_scope",
                category="multiplicity_scope",
            )

        market_eval = _evaluate_market_execution_and_stability(
            row=row,
            min_tob_coverage=min_tob_coverage,
            min_net_expectancy_bps=min_net_expectancy_bps,
            max_fee_plus_slippage_bps=max_fee_plus_slippage_bps,
            max_daily_turnover_multiple=max_daily_turnover_multiple,
            require_retail_viability=require_retail_viability,
            min_cost_survival_ratio=min_cost_survival_ratio,
            min_stability_score=min_stability_score,
            min_sign_consistency=min_sign_consistency,
            enforce_baseline_beats_complexity=enforce_baseline_beats_complexity,
            enforce_placebo_controls=enforce_placebo_controls,
            enforce_timeframe_consensus=enforce_timeframe_consensus,
            reasons=reasons,
        )
        control_eval = _evaluate_control_audit_and_dsr(
            row=row,
            event_type=event_type,
            plan_row_id=plan_row_id,
            hypothesis_index=hypothesis_index,
            negative_control_summary=negative_control_summary,
            max_negative_control_pass_rate=max_negative_control_pass_rate,
            allow_missing_negative_controls=allow_missing_negative_controls,
            require_multiplicity_diagnostics=require_multiplicity_diagnostics,
            require_hypothesis_audit=require_hypothesis_audit,
            min_dsr=min_dsr,
            reasons=reasons,
        )
        deploy_eval = _evaluate_deploy_oos_and_low_capital(
            row=row,
            max_q_value=max_q_value,
            promotion_confirmatory_gates=promotion_confirmatory_gates,
            require_low_capital_viability=require_low_capital_viability,
            reasons=reasons,
        )
        continuation_eval = _evaluate_continuation_quality(
            row=row,
            stability_pass=market_eval["stability_pass"],
            oos_pass=deploy_eval["oos_pass"],
            microstructure_pass=market_eval["microstructure_pass"],
            dsr_pass=control_eval["dsr_pass"],
            reasons=reasons,
        )
        cell_origin_eval = _evaluate_cell_origin_governance(row, reasons)

        result = _assemble_promotion_result(
            reasons=reasons,
            q_value=q_value,
            n_events=n_events,
            tob_pass=market_eval["tob_pass"],
            require_retail_viability=require_retail_viability,
            require_low_capital_viability=require_low_capital_viability,
            enforce_baseline_beats_complexity=enforce_baseline_beats_complexity,
            enforce_placebo_controls=enforce_placebo_controls,
            enforce_timeframe_consensus=enforce_timeframe_consensus,
            statistical_pass=statistical_pass,
            cost_pass=market_eval["cost_pass"],
            beats_baseline=market_eval["beats_baseline"],
            placebo_pass=market_eval["placebo_pass"],
            stability_pass=market_eval["stability_pass"],
            timeframe_consensus_pass=market_eval["timeframe_consensus_pass"],
            oos_pass=deploy_eval["oos_pass"],
            microstructure_pass=market_eval["microstructure_pass"],
            stressed_cost_pass=market_eval["stressed_cost_pass"],
            delayed_entry_pass=market_eval["delayed_entry_pass"],
            continuation_quality_pass=continuation_eval["continuation_quality_pass"],
            multiplicity_diag_pass=control_eval["multiplicity_diag_pass"],
            audit_pass=control_eval["audit_pass"],
            dsr_pass=control_eval["dsr_pass"],
            multiplicity_pass=deploy_eval["multiplicity_pass"],
            robustness_pass=deploy_eval["robustness_pass"],
            regime_pass=deploy_eval["regime_pass"],
            retail_viability_pass=market_eval["retail_viability_pass"],
            low_capital_viability_pass=deploy_eval["low_capital_viability_pass"],
            q_value_family=deploy_eval["q_value_family"],
            q_value_cluster=deploy_eval["q_value_cluster"],
            q_value_by=deploy_eval["q_value_by"],
            q_value_program=deploy_eval["q_value_program"],
            ss=market_eval["ss"],
            sc=market_eval["sc"],
            csr=market_eval["csr"],
            control_pass=control_eval["control_pass"],
            control_rate=control_eval["control_rate"],
            control_rate_source=control_eval["control_rate_source"],
            tob_coverage=market_eval["tob_coverage"],
            net_expectancy_bps=market_eval["net_expectancy_bps"],
            effective_cost_bps=market_eval["effective_cost_bps"],
            turnover_proxy_mean=market_eval["turnover_proxy_mean"],
            audit_statuses=control_eval["audit_statuses"],
            net_expectancy_pass=market_eval["net_expectancy_pass"],
            cost_budget_pass=market_eval["cost_budget_pass"],
            turnover_pass=market_eval["turnover_pass"],
            dsr_value=control_eval["dsr_value"],
            shrinkage_loso_stable=deploy_eval["shrinkage_loso_stable"],
            shrinkage_borrowing_dominant=deploy_eval["shrinkage_borrowing_dominant"],
            structural_robustness_score=deploy_eval["structural_robustness_score"],
            repeated_fold_consistency=deploy_eval["repeated_fold_consistency"],
            robustness_panel_complete=deploy_eval["robustness_panel_complete"],
            num_regimes=deploy_eval["num_regimes"],
            regime_stability_pass=deploy_eval["regime_stability_pass"],
            structural_break_pass=deploy_eval["structural_break_pass"],
            validation_samples_raw=deploy_eval["validation_samples_raw"],
            test_samples_raw=deploy_eval["test_samples_raw"],
            validation_samples_effective=deploy_eval["validation_samples_effective"],
            test_samples_effective=deploy_eval["test_samples_effective"],
            oos_sample_source=deploy_eval["oos_sample_source"],
            oos_direction_match=deploy_eval["oos_direction_match"],
            min_validation_events_required=deploy_eval["min_validation_events_required"],
            min_test_events_required=deploy_eval["min_test_events_required"],
            low_capital_viability_score=deploy_eval["low_capital_viability_score"],
            low_capital_reject_codes=deploy_eval["low_capital_reject_codes"],
            run_mode_normalized=deploy_eval["run_mode_normalized"],
            is_deploy_mode=deploy_eval["is_deploy_mode"],
            is_descriptive=is_descriptive,
            is_trade_trigger=is_trade_trigger,
            max_q_value=max_q_value,
            promotion_profile=promotion_profile,
            is_reduced_evidence=is_reduced_evidence,
            benchmark_pass=bench_pass,
            sensitivity_pass=sensitivity_pass,
            cell_origin_pass=bool(cell_origin_eval["pass"]),
        )
        result.update(
            {
                "cell_origin_governance_applies": bool(cell_origin_eval["applies"]),
                "cell_origin_pass": bool(cell_origin_eval["pass"]),
                "cell_origin_gate_reasons": "|".join(cell_origin_eval["fail_reasons"]),
                "cell_origin_complexity_penalty": float(
                    cell_origin_eval["complexity_penalty"]
                ),
                "cell_origin_runtime_mapping_status": str(
                    cell_origin_eval["runtime_mapping_status"]
                ),
            }
        )
        result["is_continuation_template_family"] = continuation_eval[
            "is_continuation_template_family"
        ]
        result["gate_bridge_tradable"] = "pass" if continuation_eval["bridge_tradable"] else "fail"
        result.update(
            {
                "event_contract_tier": str(governance.get("tier", "")),
                "event_operational_role": str(governance.get("operational_role", "")),
                "event_deployment_disposition": str(governance.get("deployment_disposition", "")),
                "event_runtime_category": str(governance.get("runtime_category", "")),
                "event_is_descriptive": bool(is_descriptive),
                "event_is_trade_trigger": bool(is_trade_trigger),
                "event_requires_stronger_evidence": bool(
                    governance.get("requires_stronger_evidence", False)
                ),
            }
        )

        merged_for_bundle = dict(row)
        merged_for_bundle.update(result)
        policy = _build_bundle_policy(
            max_q_value=max_q_value,
            min_events=min_events,
            min_stability_score=min_stability_score,
            min_sign_consistency=min_sign_consistency,
            min_cost_survival_ratio=min_cost_survival_ratio,
            max_negative_control_pass_rate=max_negative_control_pass_rate,
            min_tob_coverage=min_tob_coverage,
            require_hypothesis_audit=require_hypothesis_audit,
            allow_missing_negative_controls=allow_missing_negative_controls,
            require_multiplicity_diagnostics=require_multiplicity_diagnostics,
            require_retail_viability=require_retail_viability,
            require_low_capital_viability=require_low_capital_viability,
            promotion_profile=promotion_profile,
            enforce_baseline_beats_complexity=enforce_baseline_beats_complexity,
            enforce_placebo_controls=enforce_placebo_controls,
            enforce_timeframe_consensus=enforce_timeframe_consensus,
            enforce_regime_stability=enforce_regime_stability,
            require_scope_level_multiplicity=require_scope_level_multiplicity,
            allow_multiplicity_scope_degraded=allow_multiplicity_scope_degraded,
            use_effective_q_value=use_effective_q_value,
            policy_version=policy_version,
            bundle_version=bundle_version,
        )
        bundle = build_evidence_bundle(
            merged_for_bundle,
            control_rate=control_eval["control_rate"],
            max_negative_control_pass_rate=float(max_negative_control_pass_rate),
            allow_missing_negative_controls=bool(allow_missing_negative_controls),
            policy_version=policy.policy_version,
            bundle_version=policy.bundle_version,
        )
        validate_evidence_bundle(bundle)
        bundle_decision = evaluate_promotion_bundle(bundle, policy)
        bundle["promotion_decision"] = dict(bundle_decision)
        bundle["rejection_reasons"] = list(bundle_decision.get("rejection_reasons", []))
        result = _apply_authoritative_bundle_decision(result, bundle, bundle_decision)
        result = _apply_cell_origin_authority(result, cell_origin_eval)
        return _restore_boolean_compat_gates(result)
    except Exception as e:
        if isinstance(e, PromotionDecisionError):
            raise
        raise PromotionDecisionError(
            f"Failed to evaluate promotion for candidate {row.get('candidate_id')}: {e}"
        ) from e
