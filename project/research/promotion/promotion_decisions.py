from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

from project.core.coercion import as_bool
from project.core.exceptions import PromotionDecisionError
from project.events.governance import promotion_event_metadata
from project.research.promotion.promotion_decision_support import (
    _apply_bundle_policy_result,
    _restore_boolean_compat_gates,
)
from project.research.promotion.promotion_eligibility import _ReasonRecorder
from project.research.promotion.promotion_gate_evaluators import (
    _evaluate_continuation_quality,
    _evaluate_control_audit_and_dsr,
    _evaluate_deploy_oos_and_low_capital,
    _evaluate_market_execution_and_stability,
    _quiet_int,
    evaluate_sensitivity_gate,
)
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




def _generic_template_promotion_block(row: dict[str, Any]) -> tuple[bool, str]:
    template_id = str(row.get("template_id", row.get("template", "")) or "").strip()
    if not template_id:
        return False, ""
    try:
        from project.domain.compiled_registry import get_domain_registry
        operator = get_domain_registry().get_operator(template_id)
        raw = operator.raw if operator is not None and isinstance(operator.raw, dict) else {}
        status = str(raw.get("contract_status", "")).strip().lower()
        if status == "abstract_template_family":
            return True, "generic_template_not_promotable"
    except Exception:
        pass
    if template_id in {"mean_reversion", "continuation", "exhaustion_reversal", "reversal_or_squeeze"}:
        return True, "generic_template_not_promotable"
    if template_id.startswith("generic_"):
        return True, "generic_template_not_promotable"
    return False, ""

def _apply_generic_template_authority(result: dict[str, Any], reason: str) -> dict[str, Any]:
    if not reason:
        return result
    out = dict(result)
    out["eligible"] = False
    out["promotion_status"] = "rejected"
    out["promotion_decision"] = "rejected"
    out["promotion_track"] = "fallback_only"
    out["fallback_used"] = True
    out["fallback_reason"] = reason
    existing = [r for r in str(out.get("reject_reason", "")).split("|") if r]
    out["reject_reason"] = "|".join(sorted(set(existing + [reason])))
    out["promotion_fail_gate_primary"] = str(out.get("promotion_fail_gate_primary", "") or "gate_promo_template_contract")
    out["promotion_fail_reason_primary"] = str(out.get("promotion_fail_reason_primary", "") or "failed_gate_promo_template_contract")
    rejection_reasons = list(out.get("rejection_reasons", []) or [])
    out["rejection_reasons"] = sorted(set(rejection_reasons + [reason]))
    gate_results = dict(out.get("gate_results", {}) or {})
    gate_results["template_contract"] = "fail"
    out["gate_results"] = gate_results
    out["gate_promo_template_contract"] = "fail"
    audit = dict(out.get("promotion_audit", {}) or {})
    audit["gate_promo_template_contract"] = "fail"
    out["promotion_audit"] = audit
    return out


def _compatibility_promotion_block(row: dict[str, Any]) -> tuple[bool, str]:
    """Block promotion when upstream event-template compatibility says non-promotable.

    Cell discovery and feasibility now emit structured compatibility lineage.
    Promotion must treat those fields as authoritative rather than advisory.
    """
    status = str(row.get("compatibility_status", "") or "").strip().lower()
    raw_allowed = row.get("compatibility_promotion_allowed", None)
    allowed = True
    if raw_allowed is not None:
        allowed = as_bool(raw_allowed)
    if status in {"forbidden", "research_only"}:
        allowed = False
    reason_codes = str(row.get("compatibility_reason_codes", "") or "").strip()
    if not allowed:
        if reason_codes:
            primary = reason_codes.split("|", 1)[0].strip()
            return True, primary or "compatibility_promotion_block"
        if status:
            return True, f"compatibility_{status}_not_promotable"
        return True, "compatibility_promotion_block"
    return False, ""


def _side_policy_resolution_block(row: dict[str, Any]) -> tuple[bool, str]:
    """Require explicit directional semantics for promotion-path rows.

    Generic discovery rows may omit side metadata, but promoted theses need an
    auditable mapping from event polarity + template side_policy to trade side.
    """
    label_target = str(row.get("label_target", row.get("template_label_target", "fwd_return_h")) or "").strip().lower()
    if label_target == "gate":
        return False, ""
    direction = str(row.get("direction", row.get("trade_direction", "")) or "").strip().lower()
    side_policy = str(row.get("side_policy", row.get("template_side_policy", "")) or "").strip().lower()
    event_side = str(row.get("event_side", row.get("resolved_event_side", "")) or "").strip().lower()
    event_direction = row.get("event_direction", row.get("resolved_event_direction", None))

    if direction in {"long", "short"}:
        return False, ""
    if side_policy in {"directional", "contrarian", "both"}:
        # Side-policy driven rows need event polarity unless an explicit direction exists.
        if event_side in {"bullish", "bearish", "long", "short", "up", "down"}:
            return False, ""
        try:
            if float(event_direction) != 0.0:
                return False, ""
        except (TypeError, ValueError):
            pass
        return True, "side_policy_resolution_missing_event_polarity"
    return True, "side_policy_resolution_missing"



def _mechanism_evidence_block(row: dict[str, Any]) -> tuple[bool, str]:
    label = str(row.get("mechanism_label", row.get("template_mechanism_label", "")) or "").strip().lower()
    if not label or label in {"none", "unavailable"}:
        # Do not force legacy rows without mechanism labels yet; concrete mechanism templates should emit one.
        template_id = str(row.get("template_id", row.get("template", "")) or "").strip()
        mechanism_templates = {
            "basis_repair", "basis_convergence", "basis_funding_convergence", "desync_repair",
            "liquidity_refill_repair", "overshoot_repair", "range_reversion", "vol_decay_mean_reversion",
            "forced_flow_rebound", "long_flush_rebound", "positioning_flush_reversal",
            "breakout_followthrough", "trend_continuation", "volatility_expansion_follow",
        }
        if template_id in mechanism_templates:
            return True, "mechanism_label_missing"
        return False, ""
    valid_raw = row.get("mechanism_valid", None)
    if valid_raw is not None and not as_bool(valid_raw):
        return True, "mechanism_invalid"
    try:
        rate = float(row.get("mechanism_success_rate"))
    except (TypeError, ValueError):
        return True, "mechanism_success_missing"
    thresholds = {
        "basis_zscore_delta_h": 0.55,
        "basis_convergence": 0.55,
        "liquidity_normalization_confirmed": 0.55,
        "deviation_recontracts": 0.55,
        "volatility_decay": 0.55,
        "trend_continuation_confirmed": 0.52,
        "post_entry_directional_expansion": 0.52,
        "post_climax_reversal": 0.53,
        "forced_flow_rebound": 0.53,
    }
    threshold = thresholds.get(label, 0.50)
    if rate < threshold:
        return True, "mechanism_success_below_threshold"
    return False, ""


def _apply_hard_promotion_block(result: dict[str, Any], *, reason: str, gate: str) -> dict[str, Any]:
    if not reason:
        return result
    out = dict(result)
    out["eligible"] = False
    out["promotion_status"] = "rejected"
    out["promotion_decision"] = "rejected"
    out["promotion_track"] = "fallback_only"
    out["fallback_used"] = True
    out["fallback_reason"] = gate
    existing = [r for r in str(out.get("reject_reason", "")).split("|") if r]
    out["reject_reason"] = "|".join(sorted(set(existing + [reason])))
    out["promotion_fail_gate_primary"] = str(out.get("promotion_fail_gate_primary", "") or gate)
    out["promotion_fail_reason_primary"] = str(out.get("promotion_fail_reason_primary", "") or f"failed_{gate}")
    rejection_reasons = list(out.get("rejection_reasons", []) or [])
    out["rejection_reasons"] = sorted(set(rejection_reasons + [reason]))
    gate_results = dict(out.get("gate_results", {}) or {})
    gate_results[gate.replace("gate_promo_", "")] = "fail"
    out["gate_results"] = gate_results
    out[gate] = "fail"
    audit = dict(out.get("promotion_audit", {}) or {})
    audit[gate] = "fail"
    out["promotion_audit"] = audit
    return out



def _data_quality_promotion_block(row: dict[str, Any]) -> tuple[bool, str]:
    """Hard-block promotion when required data quality is not production-grade.

    The data-quality state may appear either as a direct column, in context
    metadata, or inside required/supportive context mappings. Treat stale and
    missing-required-feature as hard blockers; synthetic-only remains research
    only and therefore blocks promotion/runtime.
    """
    blocked = {
        "stale": "data_quality_stale",
        "missing_required_feature": "data_quality_missing_required_feature",
        "synthetic_only": "data_quality_synthetic_only_research_only",
    }
    candidates: list[str] = []
    for key in (
        "data_quality_state",
        "context_data_quality_state",
        "required_data_quality_state",
    ):
        value = row.get(key)
        if value not in (None, ""):
            candidates.append(str(value).strip().lower())
    for mapping_key in ("context", "required_context", "supportive_context"):
        value = row.get(mapping_key)
        if isinstance(value, dict):
            dq = value.get("data_quality_state")
            if dq not in (None, ""):
                candidates.append(str(dq).strip().lower())
    for value in candidates:
        if value in blocked:
            return True, blocked[value]
    return False, ""


def _semantic_promotion_gate(row: dict[str, Any]) -> dict[str, Any]:
    """Return a first-class semantic promotion verdict.

    This consolidates the hard contract checks that make an evaluated candidate
    safe to interpret as a promotable thesis, independent of statistical score.
    """
    reasons: list[str] = []

    anchor_role = str(row.get("anchor_role", row.get("event_anchor_role", "")) or "").strip().lower()
    if anchor_role in {"context_filter", "risk_guard", "execution_guard", "temporal_guard", "research_only"}:
        reasons.append("anchor_role_not_tradeable")

    context_timing = str(row.get("context_timing", row.get("context_eval_timing", "trigger")) or "").strip().lower()
    if not context_timing:
        reasons.append("context_timing_missing")

    template_blocked, template_reason = _generic_template_promotion_block(row)
    if template_blocked:
        reasons.append(template_reason or "template_abstract_not_promotable")

    compatibility_blocked, compatibility_reason = _compatibility_promotion_block(row)
    if compatibility_blocked:
        reasons.append(compatibility_reason or "compatibility_blocks_promotion")

    side_blocked, side_reason = _side_policy_resolution_block(row)
    if side_blocked:
        reasons.append(side_reason or "event_side_required_missing")

    data_blocked, data_reason = _data_quality_promotion_block(row)
    if data_blocked:
        reasons.append(data_reason or "data_quality_blocks_promotion")

    mechanism_blocked, mechanism_reason = _mechanism_evidence_block(row)
    if mechanism_blocked:
        reasons.append(mechanism_reason or "mechanism_evidence_missing")

    clean = []
    for r in reasons:
        if r and r not in clean:
            clean.append(r)
    return {
        "semantic_pass": not clean,
        "semantic_reasons": clean,
        "semantic_reason_codes": "|".join(clean),
    }

def _is_cell_origin_row(row: dict[str, Any]) -> bool:
    return str(row.get("source_discovery_mode", "") or "").strip().lower() == _CELL_ORIGIN_MODE


def _has_explicit_runtime_mapping(row: dict[str, Any]) -> bool:
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
    row: dict[str, Any],
    reasons: _ReasonRecorder,
) -> dict[str, Any]:
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
    result: dict[str, Any],
    cell_origin_eval: dict[str, Any],
) -> dict[str, Any]:
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
    result: dict[str, Any],
    bundle: dict[str, Any] | None,
    bundle_decision: dict[str, Any] | None = None,
) -> dict[str, Any]:
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
    row: dict[str, Any],
    hypothesis_index: dict[str, dict[str, Any]],
    negative_control_summary: dict[str, Any],
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
    promotion_confirmatory_gates: dict[str, Any] | None = None,
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
    benchmark_certification: dict[str, Any] | None = None,
    run_id: str | None = None,
    data_root: Path | None = None,
) -> dict[str, Any]:
    try:
        reasons = _ReasonRecorder.create()
        event_type = str(row.get("event_type", row.get("event", ""))).strip() or "UNKNOWN_EVENT"
        semantic_gate = _semantic_promotion_gate(row)
        for semantic_reason in semantic_gate["semantic_reasons"]:
            reasons.add_pair(
                reject_reason=semantic_reason,
                promo_fail_reason="gate_promo_semantic",
                category="semantic_contract",
            )

        generic_template_blocked, generic_template_reason = _generic_template_promotion_block(row)
        if generic_template_blocked:
            reasons = _ReasonRecorder.create() if "reasons" not in locals() else reasons

        if generic_template_blocked:
            reasons.add_pair(
                reject_reason=generic_template_reason,
                promo_fail_reason="gate_promo_template_contract",
                category="template_contract",
            )

        compatibility_blocked, compatibility_reason = _compatibility_promotion_block(row)
        if compatibility_blocked:
            reasons.add_pair(
                reject_reason=compatibility_reason,
                promo_fail_reason="gate_promo_compatibility",
                category="compatibility",
            )

        side_policy_blocked, side_policy_reason = _side_policy_resolution_block(row)
        if side_policy_blocked:
            reasons.add_pair(
                reject_reason=side_policy_reason,
                promo_fail_reason="gate_promo_side_policy_resolution",
                category="side_policy_resolution",
            )

        mechanism_blocked, mechanism_reason = _mechanism_evidence_block(row)
        if mechanism_blocked:
            reasons.add_pair(
                reject_reason=mechanism_reason,
                promo_fail_reason="gate_promo_mechanism_evidence",
                category="mechanism_evidence",
            )

        # Benchmark certification is diagnostic-only. Keep the status in the
        # promotion audit, but do not block candidate promotion on the presence
        # or health of the benchmark bundle.
        benchmark_certification_enforced = False
        benchmark_certification_status = ""
        benchmark_certification_message = ""
        bench_pass = True
        if benchmark_certification:
            benchmark_certification_status = str(benchmark_certification.get("status", "")).strip()
            benchmark_certification_message = str(benchmark_certification.get("message", "")).strip()
            bench_pass = True

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
                "benchmark_certification_enforced": bool(benchmark_certification_enforced),
                "benchmark_certification_status": benchmark_certification_status,
                "benchmark_certification_message": benchmark_certification_message,
                "cell_origin_governance_applies": bool(cell_origin_eval["applies"]),
                "cell_origin_pass": bool(cell_origin_eval["pass"]),
                "cell_origin_gate_reasons": "|".join(cell_origin_eval["fail_reasons"]),
                "cell_origin_complexity_penalty": float(
                    cell_origin_eval["complexity_penalty"]
                ),
                "cell_origin_runtime_mapping_status": str(
                    cell_origin_eval["runtime_mapping_status"]
                ),
                "semantic_pass": bool(semantic_gate["semantic_pass"]),
                "semantic_reasons": list(semantic_gate["semantic_reasons"]),
                "semantic_reason_codes": str(semantic_gate["semantic_reason_codes"]),
                "gate_promo_semantic": "pass" if bool(semantic_gate["semantic_pass"]) else "fail",
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
        result = _apply_generic_template_authority(
            result, generic_template_reason if generic_template_blocked else ""
        )
        result = _apply_hard_promotion_block(
            result,
            reason=compatibility_reason if compatibility_blocked else "",
            gate="gate_promo_compatibility",
        )
        result = _apply_hard_promotion_block(
            result,
            reason=side_policy_reason if side_policy_blocked else "",
            gate="gate_promo_side_policy_resolution",
        )
        data_quality_blocked, data_quality_reason = _data_quality_promotion_block(row)
        result = _apply_hard_promotion_block(
            result,
            reason=data_quality_reason if data_quality_blocked else "",
            gate="gate_promo_data_quality",
        )
        result = _apply_hard_promotion_block(
            result,
            reason=mechanism_reason if mechanism_blocked else "",
            gate="gate_promo_mechanism_evidence",
        )
        result = _apply_hard_promotion_block(
            result,
            reason=semantic_gate["semantic_reason_codes"] if not bool(semantic_gate["semantic_pass"]) else "",
            gate="gate_promo_semantic",
        )
        result["semantic_pass"] = bool(semantic_gate["semantic_pass"])
        result["semantic_reasons"] = list(semantic_gate["semantic_reasons"])
        result["semantic_reason_codes"] = str(semantic_gate["semantic_reason_codes"])
        return _restore_boolean_compat_gates(result)
    except Exception as e:
        if isinstance(e, PromotionDecisionError):
            raise
        raise PromotionDecisionError(
            f"Failed to evaluate promotion for candidate {row.get('candidate_id')}: {e}"
        ) from e
