from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from project.core.coercion import as_bool, safe_float, safe_int
from project.eval.selection_bias import deflated_sharpe_ratio as _deflated_sharpe_ratio
from project.research.helpers.viability import evaluate_retail_constraints
from project.research.promotion.multi_timeframe import evaluate_timeframe_consensus
from project.research.promotion.promotion_eligibility import (
    _ReasonRecorder,
    _has_explicit_oos_samples,
    _is_deploy_mode,
    control_rate_details_for_event,
    cost_survival_ratio,
    sign_consistency,
)
from project.research.promotion.promotion_scoring import stability_score
from project.research.utils.decision_safety import (
    bool_gate,
    coerce_numeric_nan,
    finite_ge,
)
from project.research.utils.returns_oos import normalize_returns_oos_combined


log = logging.getLogger(__name__)


def _quiet_float(value: Any, default: float) -> float:
    if value is None or (isinstance(value, float) and not np.isfinite(value)):
        return float(default)
    coerced = safe_float(value, default)
    return float(default if coerced is None else coerced)


def _quiet_int(value: Any, default: int) -> int:
    if value is None or (isinstance(value, float) and not np.isfinite(value)):
        return int(default)
    coerced = safe_int(value, default)
    return int(default if coerced is None else coerced)


def _parse_returns_oos(values: Any) -> pd.Series:
    try:
        normalized = normalize_returns_oos_combined(values)
    except ValueError:
        return pd.Series(dtype=float)
    return pd.to_numeric(pd.Series(normalized, dtype="float64"), errors="coerce").dropna()


def _confirmatory_shadow_gates(
    promotion_confirmatory_gates: Dict[str, Any] | None,
) -> Dict[str, Any]:
    gates = promotion_confirmatory_gates or {}
    shadow = gates.get("shadow", {})
    return shadow if isinstance(shadow, dict) else {}


def _confirmatory_deployable_gates(
    promotion_confirmatory_gates: Dict[str, Any] | None,
) -> Dict[str, Any]:
    gates = promotion_confirmatory_gates or {}
    deployable = gates.get("deployable", {})
    return deployable if isinstance(deployable, dict) else {}


_CONTINUATION_TEMPLATE_VERBS = {
    "continuation",
    "trend_continuation",
    "momentum_fade",
    "pullback_entry",
    "only_if_funding",
}


def _is_continuation_template_family(row: Dict[str, Any]) -> bool:
    template_verb = str(row.get("template_verb", "")).strip().lower()
    return template_verb in _CONTINUATION_TEMPLATE_VERBS


def _evaluate_continuation_quality(
    *,
    row: Dict[str, Any],
    stability_pass: bool,
    oos_pass: bool | None,
    microstructure_pass: bool,
    dsr_pass: bool,
    reasons: _ReasonRecorder,
) -> Dict[str, Any]:
    is_continuation_template_family = _is_continuation_template_family(row)
    bridge_tradable = bool_gate(row.get("gate_bridge_tradable"))
    continuation_quality_pass = True
    if is_continuation_template_family and bridge_tradable:
        # Phase 1.4: treat oos_pass=None as False for continuation quality gate —
        # continuation strategies require confirmed OOS support.
        oos_pass_resolved: bool = bool(oos_pass) if oos_pass is not None else False
        continuation_quality_pass = bool(
            stability_pass and oos_pass_resolved and microstructure_pass and dsr_pass
        )
        if not continuation_quality_pass:
            if not stability_pass:
                reasons.add_reject(
                    "continuation_quality_stability", category="continuation_quality"
                )
            if not oos_pass_resolved:
                reasons.add_reject(
                    "continuation_quality_oos_validation", category="continuation_quality"
                )
            if not microstructure_pass:
                reasons.add_reject(
                    "continuation_quality_microstructure", category="continuation_quality"
                )
            if not dsr_pass:
                reasons.add_reject("continuation_quality_dsr", category="continuation_quality")
            reasons.add_promo_fail(
                "gate_promo_continuation_quality", category="continuation_quality"
            )
    return {
        "is_continuation_template_family": bool(is_continuation_template_family),
        "bridge_tradable": bool(bridge_tradable),
        "continuation_quality_pass": bool(continuation_quality_pass),
    }


def _evaluate_market_execution_and_stability(
    *,
    row: Dict[str, Any],
    min_tob_coverage: float,
    min_net_expectancy_bps: float,
    max_fee_plus_slippage_bps: float | None,
    max_daily_turnover_multiple: float | None,
    require_retail_viability: bool,
    min_cost_survival_ratio: float,
    min_stability_score: float,
    min_sign_consistency: float,
    enforce_baseline_beats_complexity: bool,
    enforce_placebo_controls: bool,
    enforce_timeframe_consensus: bool,
    reasons: _ReasonRecorder,
) -> Dict[str, Any]:
    retail_eval = evaluate_retail_constraints(
        row,
        min_tob_coverage=float(min_tob_coverage),
        min_net_expectancy_bps=float(min_net_expectancy_bps),
        max_fee_plus_slippage_bps=max_fee_plus_slippage_bps,
        max_daily_turnover_multiple=max_daily_turnover_multiple,
    )
    tob_coverage = coerce_numeric_nan(retail_eval.get("tob_coverage"))
    net_expectancy_bps = coerce_numeric_nan(retail_eval.get("net_expectancy_bps"))
    effective_cost_bps = _quiet_float(retail_eval.get("effective_cost_bps"), np.nan)
    turnover_proxy_mean = _quiet_float(retail_eval.get("turnover_proxy_mean"), np.nan)

    tob_pass = bool_gate(retail_eval.get("gate_tob_coverage"))
    net_expectancy_pass = bool_gate(retail_eval.get("gate_net_expectancy"))
    cost_budget_pass = bool_gate(retail_eval.get("gate_cost_budget"))
    turnover_pass = bool_gate(retail_eval.get("gate_turnover"))
    retail_viability_pass = bool(net_expectancy_pass and cost_budget_pass and turnover_pass)

    if bool(require_retail_viability) and not retail_viability_pass:
        if not net_expectancy_pass:
            reasons.add_pair(
                reject_reason="retail_net_expectancy",
                promo_fail_reason="gate_promo_retail_net_expectancy",
                category="retail_viability",
            )
        if not cost_budget_pass:
            reasons.add_pair(
                reject_reason="retail_cost_budget",
                promo_fail_reason="gate_promo_retail_cost_budget",
                category="retail_viability",
            )
        if not turnover_pass:
            reasons.add_pair(
                reject_reason="retail_turnover",
                promo_fail_reason="gate_promo_retail_turnover",
                category="retail_viability",
            )

    csr = cost_survival_ratio(row)
    cost_pass = finite_ge(csr, min_cost_survival_ratio)
    if not cost_pass:
        reasons.add_pair(
            reject_reason="cost_survival",
            promo_fail_reason="gate_promo_cost_survival",
            category="cost_realism",
        )

    baseline_expectancy = coerce_numeric_nan(row.get("baseline_expectancy_bps"))
    baseline_available = bool(np.isfinite(baseline_expectancy))
    beats_baseline = bool(
        True
        if not baseline_available
        else (np.isfinite(net_expectancy_bps) and (net_expectancy_bps > baseline_expectancy * 1.1))
    )
    if bool(enforce_baseline_beats_complexity) and baseline_available and not beats_baseline:
        reasons.add_pair(
            reject_reason="failed_baseline_comparison",
            promo_fail_reason="gate_promo_baseline_beats_complexity",
            category="baseline_comparison",
        )

    shift_placebo_pass = bool_gate(row.get("pass_shift_placebo"))
    random_placebo_pass = bool_gate(row.get("pass_random_entry_placebo"))
    direction_placebo_pass = bool_gate(row.get("pass_direction_reversal_placebo"))
    placebo_pass = shift_placebo_pass and random_placebo_pass and direction_placebo_pass
    if bool(enforce_placebo_controls) and not placebo_pass:
        reasons.add_pair(
            reject_reason="failed_placebo_controls",
            promo_fail_reason="gate_promo_placebo_controls",
            category="falsification",
        )

    sc = sign_consistency(row)
    ss = stability_score(row, sc)
    gate_stability = bool_gate(row.get("gate_stability"))
    gate_delay_robustness = bool_gate(row.get("gate_delay_robustness"))
    stability_pass = (
        gate_stability
        and finite_ge(ss, min_stability_score)
        and finite_ge(sc, min_sign_consistency)
        and gate_delay_robustness
    )
    if not stability_pass:
        if not gate_stability:
            reasons.add_pair(
                reject_reason="stability_gate",
                promo_fail_reason="gate_promo_stability_gate",
                category="stability",
            )
        if ss < float(min_stability_score):
            reasons.add_pair(
                reject_reason="stability_score",
                promo_fail_reason="gate_promo_stability_score",
                category="stability",
            )
        if sc < float(min_sign_consistency):
            reasons.add_pair(
                reject_reason="stability_sign_consistency",
                promo_fail_reason="gate_promo_stability_sign_consistency",
                category="stability",
            )
        if not gate_delay_robustness:
            reasons.add_pair(
                reject_reason="delay_robustness_fail",
                promo_fail_reason="gate_promo_delay_robustness_fail",
                category="stability",
            )

    consensus_eval = evaluate_timeframe_consensus(
        base_timeframe="5m",
        alternate_timeframes=["1m", "15m"],
        row=row,
        min_consensus_ratio=0.3,
    )
    timeframe_consensus_pass = bool(consensus_eval["pass_consensus"])
    if bool(enforce_timeframe_consensus) and not timeframe_consensus_pass:
        reasons.add_pair(
            reject_reason="timeframe_consensus_fail",
            promo_fail_reason="gate_promo_timeframe_consensus",
            category="timeframe_consensus",
        )

    microstructure_pass = bool_gate(row.get("gate_bridge_microstructure"))
    if not microstructure_pass:
        reasons.add_pair(
            reject_reason="microstructure_risk",
            promo_fail_reason="gate_promo_microstructure",
            category="microstructure",
        )

    stressed_cost_pass = bool_gate(row.get("gate_after_cost_stressed_positive"))
    if not stressed_cost_pass:
        reasons.add_pair(
            reject_reason="stressed_cost_survival_fail",
            promo_fail_reason="gate_promo_stressed_cost_survival",
            category="stress_tests",
        )

    delayed_entry_pass = bool_gate(row.get("gate_delayed_entry_stress"))
    if not delayed_entry_pass:
        reasons.add_pair(
            reject_reason="delayed_entry_fragility",
            promo_fail_reason="gate_promo_delayed_entry_stress",
            category="stress_tests",
        )

    return {
        "tob_coverage": tob_coverage,
        "net_expectancy_bps": net_expectancy_bps,
        "effective_cost_bps": effective_cost_bps,
        "turnover_proxy_mean": turnover_proxy_mean,
        "tob_pass": tob_pass,
        "net_expectancy_pass": net_expectancy_pass,
        "cost_budget_pass": cost_budget_pass,
        "turnover_pass": turnover_pass,
        "retail_viability_pass": retail_viability_pass,
        "csr": csr,
        "cost_pass": cost_pass,
        "beats_baseline": beats_baseline,
        "placebo_pass": placebo_pass,
        "sc": sc,
        "ss": ss,
        "stability_pass": stability_pass,
        "timeframe_consensus_pass": timeframe_consensus_pass,
        "microstructure_pass": microstructure_pass,
        "stressed_cost_pass": stressed_cost_pass,
        "delayed_entry_pass": delayed_entry_pass,
    }


def _evaluate_control_audit_and_dsr(
    *,
    row: Dict[str, Any],
    event_type: str,
    plan_row_id: str,
    hypothesis_index: Dict[str, Dict[str, Any]],
    negative_control_summary: Dict[str, Any],
    max_negative_control_pass_rate: float,
    allow_missing_negative_controls: bool,
    require_multiplicity_diagnostics: bool,
    require_hypothesis_audit: bool,
    min_dsr: float,
    reasons: _ReasonRecorder,
) -> Dict[str, Any]:
    control_details = control_rate_details_for_event(
        row=row, event_type=event_type, summary=negative_control_summary
    )
    control_rate = control_details["rate"]
    control_rate_source = str(control_details["source"])
    if control_rate is None:
        control_pass = bool(allow_missing_negative_controls)
        if not control_pass:
            reasons.add_pair(
                reject_reason="negative_control_missing",
                promo_fail_reason="gate_promo_negative_control_missing",
                category="negative_controls",
            )
    else:
        control_pass = control_rate <= float(max_negative_control_pass_rate)
        if not control_pass:
            reasons.add_pair(
                reject_reason="negative_control_fail",
                promo_fail_reason="gate_promo_negative_control_fail",
                category="negative_controls",
            )

    q_value_by = _quiet_float(row.get("q_value_by"), np.nan)
    q_value_cluster = _quiet_float(row.get("q_value_cluster"), np.nan)
    multiplicity_diag_available = bool(np.isfinite(q_value_by) and np.isfinite(q_value_cluster))
    multiplicity_diag_pass = bool(
        (not require_multiplicity_diagnostics) or multiplicity_diag_available
    )
    if not multiplicity_diag_pass:
        reasons.add_pair(
            reject_reason="multiplicity_diagnostics_missing",
            promo_fail_reason="gate_promo_multiplicity_diagnostics",
            category="multiplicity_diagnostics",
        )

    audit_pass = True
    audit_statuses: List[str] = []
    if plan_row_id:
        audit_info = hypothesis_index.get(plan_row_id)
        if audit_info:
            audit_statuses = list(audit_info.get("statuses", []))
            audit_pass = bool(audit_info.get("executed", False))
            if not audit_pass:
                reasons.add_pair(
                    reject_reason="hypothesis_not_executed",
                    promo_fail_reason="gate_promo_hypothesis_not_executed",
                    category="hypothesis_audit",
                )
        elif require_hypothesis_audit:
            audit_pass = False
            reasons.add_pair(
                reject_reason="hypothesis_missing_audit",
                promo_fail_reason="gate_promo_hypothesis_missing_audit",
                category="hypothesis_audit",
            )
    elif require_hypothesis_audit:
        audit_pass = False
        reasons.add_pair(
            reject_reason="hypothesis_missing_plan_row_id",
            promo_fail_reason="gate_promo_hypothesis_missing_plan_row_id",
            category="hypothesis_audit",
        )

    dsr_value = 0.0
    dsr_pass = True
    returns_oos = _parse_returns_oos(row.get("returns_oos_combined"))
    if len(returns_oos) >= 10:
        # Fallback order for DSR trials: broader effective multiplicity count
        raw_n_trials = 0
        used_col = "none"
        for col in [
            "num_tests_effective",
            "num_tests_campaign",
            "num_tests_family",
            "num_tests_event_family",
        ]:
            val = _quiet_int(row.get(col, 0), 0)
            if val >= 1:
                raw_n_trials = val
                used_col = col
                break

        if raw_n_trials < 1:
            if float(min_dsr) > 0:
                if (
                    _quiet_int(row.get("num_tests_event_family", 0), 0) == 0
                    and "num_tests_event_family" in row
                ):
                    log.warning(
                        "num_tests_event_family=0: DSR n_trials fallback to 1. "
                        "Selection penalty will be underestimated."
                    )
                else:
                    log.warning(
                        "_evaluate_control_audit_and_dsr: No multiplicity test-count columns found (effective, campaign, family)."
                        " DSR will use n_trials=1 (PSR equivalence). Selection penalty will be underestimated."
                    )
        else:
            log.info(
                "_evaluate_control_audit_and_dsr: DSR using n_trials=%d from '%s'",
                raw_n_trials,
                used_col,
            )

        n_trials = max(1, raw_n_trials)
        dsr_value = float(_deflated_sharpe_ratio(pd.Series(returns_oos), n_trials=n_trials))
    elif float(min_dsr) > 0.0:
        dsr_pass = False
        reasons.add_reject("missing_realized_oos_path", category="dsr")

    if float(min_dsr) > 0.0:
        dsr_pass = dsr_pass and (dsr_value >= float(min_dsr))
        if not dsr_pass:
            if "missing_realized_oos_path" not in reasons.reject_reasons:
                reasons.add_reject("dsr_below_threshold", category="dsr")
            reasons.add_promo_fail("gate_promo_dsr", category="dsr")

    return {
        "control_rate": control_rate,
        "control_rate_source": control_rate_source,
        "control_pass": control_pass,
        "q_value_by": q_value_by,
        "q_value_cluster": q_value_cluster,
        "multiplicity_diag_pass": multiplicity_diag_pass,
        "audit_pass": audit_pass,
        "audit_statuses": audit_statuses,
        "dsr_value": dsr_value,
        "dsr_pass": dsr_pass,
    }


def evaluate_sensitivity_gate(
    candidate: Dict[str, Any],
    *,
    run_id: str,
    data_root: Path,
    max_sensitivity_score: float = 0.4,
) -> tuple[str, str]:
    """Fail candidates whose performance is highly sensitive to parameter choice.

    Reads the sensitivity report written by eval/sensitivity.py for this run.
    If the report is absent, the gate passes (fail-open for research mode).
    """
    sensitivity_path = (
        data_root / "reports" / "sensitivity" / run_id / "sensitivity_summary.parquet"
    )
    if not sensitivity_path.exists():
        return "pass", "sensitivity report absent — gate skipped"

    try:
        df = pd.read_parquet(sensitivity_path)
    except Exception:
        return "pass", "sensitivity report unreadable — gate skipped"

    candidate_id = str(candidate.get("candidate_id", "")).strip()
    if candidate_id and "candidate_id" in df.columns:
        row = df[df["candidate_id"] == candidate_id]
        if not row.empty:
            score = float(row.iloc[0].get("sensitivity_score", 0.0) or 0.0)
            if score > max_sensitivity_score:
                return "fail", f"sensitivity_score={score:.3f} > {max_sensitivity_score}"
    return "pass", "within sensitivity tolerance"


def _evaluate_deploy_oos_and_low_capital(
    *,
    row: Dict[str, Any],
    max_q_value: float,
    promotion_confirmatory_gates: Dict[str, Any] | None,
    require_low_capital_viability: bool,
    reasons: _ReasonRecorder,
) -> Dict[str, Any]:
    q_value_family = _quiet_float(row.get("q_value_family"), np.nan)
    q_value_cluster = _quiet_float(row.get("q_value_cluster"), np.nan)
    q_value_by = _quiet_float(row.get("q_value_by"), np.nan)
    q_value_program = _quiet_float(row.get("q_value_program"), np.nan)
    shrinkage_loso_stable = as_bool(row.get("shrinkage_loso_stable", False))
    shrinkage_borrowing_dominant = as_bool(row.get("shrinkage_borrowing_dominant", False))
    structural_robustness_score = _quiet_float(row.get("structural_robustness_score"), np.nan)
    repeated_fold_consistency = _quiet_float(row.get("repeated_fold_consistency"), np.nan)
    robustness_panel_complete = as_bool(row.get("robustness_panel_complete", False))
    regime_counts = row.get("regime_counts", {})
    if isinstance(regime_counts, str):
        try:
            regime_counts = json.loads(regime_counts)
        except Exception:
            regime_counts = {}
    if not isinstance(regime_counts, dict):
        regime_counts = {}
    num_regimes = len([r for r, c in regime_counts.items() if _quiet_int(c, 0) >= 10])
    regime_stability_pass = as_bool(row.get("gate_regime_stability", False))
    structural_break_pass = as_bool(row.get("gate_structural_break", False))

    is_deploy = _is_deploy_mode(row)
    multiplicity_pass, robustness_pass, regime_pass = True, True, True
    if is_deploy:
        deployable_gates = _confirmatory_deployable_gates(promotion_confirmatory_gates)
        cluster_pass = (
            np.isfinite(q_value_cluster) and (q_value_cluster <= float(max_q_value))
        ) or as_bool(row.get("waiver_bounded_correlation", False))
        by_pass = np.isfinite(q_value_by) and (q_value_by <= float(max_q_value) * 2.0)
        if not cluster_pass:
            reasons.add_reject(
                "multiplicity_cluster_q", category="deploy_confirmatory", deploy_only=True
            )
            multiplicity_pass = False
        if not by_pass:
            reasons.add_reject(
                "multiplicity_by_diagnostic", category="deploy_confirmatory", deploy_only=True
            )
            multiplicity_pass = False
        if not shrinkage_loso_stable:
            reasons.add_reject(
                "shrinkage_loso_unstable", category="deploy_confirmatory", deploy_only=True
            )
            multiplicity_pass = False
        if shrinkage_borrowing_dominant:
            reasons.add_reject(
                "shrinkage_borrowing_dominant",
                category="deploy_confirmatory",
                deploy_only=True,
            )
            multiplicity_pass = False
        if not robustness_panel_complete:
            reasons.add_reject(
                "robustness_panel_incomplete", category="deploy_confirmatory", deploy_only=True
            )
            robustness_pass = False
        elif (not np.isfinite(structural_robustness_score)) or structural_robustness_score < 0.6:
            reasons.add_reject(
                "robustness_structural_low", category="deploy_confirmatory", deploy_only=True
            )
            robustness_pass = False
        if (not np.isfinite(repeated_fold_consistency)) or repeated_fold_consistency < 0.5:
            reasons.add_reject(
                "temporal_consistency_low", category="deploy_confirmatory", deploy_only=True
            )
            robustness_pass = False

        min_regimes_req = int(deployable_gates.get("min_regimes_supported", 2))
        if num_regimes < min_regimes_req and not as_bool(row.get("is_regime_specific", False)):
            reasons.add_reject(
                f"regime_thin_support (regimes={num_regimes} < {min_regimes_req})",
                category="deploy_confirmatory",
                deploy_only=True,
            )
            regime_pass = False
        if not regime_stability_pass:
            reasons.add_reject(
                "regime_instability", category="deploy_confirmatory", deploy_only=True
            )
            regime_pass = False
        if not structural_break_pass:
            reasons.add_reject(
                "structural_break_detected",
                category="deploy_confirmatory",
                deploy_only=True,
            )
            regime_pass = False
        if not as_bool(row.get("bridge_certified", False)):
            reasons.add_reject(
                "bridge_uncertified", category="deploy_confirmatory", deploy_only=True
            )
            robustness_pass = False

    bridge_validation_trades = safe_int(
        row.get("bridge_validation_trades", 0),
        0,
        context=(
            "field=bridge_validation_trades "
            f"candidate_id={row.get('candidate_id', '') or 'unknown'} "
            f"run_id={row.get('run_id', '') or 'unknown'} "
            f"source_artifact={row.get('_source_artifact', '') or 'unknown'}"
        ),
    )
    validation_samples_raw = coerce_numeric_nan(row.get("validation_samples"))
    test_samples_raw = coerce_numeric_nan(row.get("test_samples"))
    validation_samples = (
        int(validation_samples_raw)
        if np.isfinite(validation_samples_raw)
        else int(bridge_validation_trades)
    )
    bridge_oos_gate = None
    if "gate_oos_validation" in row:
        bridge_oos_gate = bool_gate(row.get("gate_oos_validation"))
    test_samples = int(test_samples_raw) if np.isfinite(test_samples_raw) else 0
    oos_sample_source = (
        "row.validation_samples"
        if np.isfinite(validation_samples_raw)
        else ("row.bridge_validation_trades" if bridge_validation_trades > 0 else "missing")
    )
    # Phase 1.4 FIX: oos_pass must be None (not evaluated) rather than True when
    # no explicit OOS samples are present. Candidates without out-of-sample evidence
    # must NOT receive a passing OOS mark — this was silently allowing promotions
    # without any OOS validation. In deploy mode, oos_not_evaluated is a hard block.
    oos_pass: bool | None = None  # None = not evaluated; True = pass; False = fail
    oos_evaluated: bool = False
    direction_match = True
    min_val_events = 0
    min_test_events = 0
    if bridge_oos_gate is not None:
        oos_evaluated = True
    if _has_explicit_oos_samples(row):
        oos_evaluated = True
        shadow_gates = _confirmatory_shadow_gates(promotion_confirmatory_gates)
        min_val_events = int(shadow_gates.get("min_oos_event_count", 20))
        min_test_events = int(shadow_gates.get("min_oos_event_count", 20))
        train_effect = coerce_numeric_nan(row.get("mean_train_return", row.get("effect_raw")))
        val_effect = coerce_numeric_nan(row.get("mean_validation_return"))
        test_effect = coerce_numeric_nan(row.get("mean_test_return"))
        if abs(train_effect) > 1e-9:
            if abs(val_effect) > 1e-9 and np.sign(val_effect) != np.sign(train_effect):
                direction_match = False
            if abs(test_effect) > 1e-9 and np.sign(test_effect) != np.sign(train_effect):
                direction_match = False
        oos_pass = validation_samples >= min_val_events
        if np.isfinite(test_samples_raw):
            oos_pass = oos_pass and (test_samples >= min_test_events)
        oos_pass = oos_pass and direction_match
        if bridge_oos_gate is not None:
            oos_pass = oos_pass and bridge_oos_gate
        if not oos_pass:
            if validation_samples < min_val_events or (
                np.isfinite(test_samples_raw) and test_samples < min_test_events
            ):
                reasons.add_reject(
                    f"oos_insufficient_samples (val={validation_samples}, test={test_samples})",
                    category="oos_validation",
                )
            if not direction_match:
                reasons.add_reject("oos_direction_flip", category="oos_validation")
            if bridge_oos_gate is False:
                reasons.add_reject("oos_validation_fail", category="oos_validation")
            reasons.add_promo_fail("gate_promo_oos_validation", category="oos_validation")
    elif bridge_oos_gate is not None:
        oos_pass = bool(bridge_oos_gate)
        if not oos_pass:
            reasons.add_reject("oos_validation_fail", category="oos_validation")
            reasons.add_promo_fail("gate_promo_oos_validation", category="oos_validation")
    elif is_deploy:
        # In deploy mode, absence of OOS evidence is a hard block — not a pass.
        oos_pass = False
        reasons.add_reject("oos_not_evaluated", category="oos_validation")
        reasons.add_promo_fail("gate_promo_oos_not_evaluated", category="oos_validation")
    # In research (non-deploy) mode with no OOS samples: oos_pass stays None.
    # Downstream _assemble_promotion_result treats None as "not checked" — it will
    # not block research-mode promotion but will be visible in the audit output.

    low_capital_viability_pass = bool_gate(row.get("gate_bridge_low_capital_viability"))
    low_capital_viability_score = _quiet_float(
        row.get("low_capital_viability_score", np.nan), np.nan
    )
    low_capital_reject_codes = [
        token.strip()
        for token in str(row.get("low_capital_reject_reason_codes", "")).split(",")
        if token.strip()
    ]
    if bool(require_low_capital_viability) and not low_capital_viability_pass:
        reasons.add_reject("low_capital_viability", category="low_capital_viability")
        for code in low_capital_reject_codes:
            reasons.add_reject(code.lower(), category="low_capital_viability")
        reasons.add_promo_fail("gate_promo_low_capital_viability", category="low_capital_viability")

    return {
        "run_mode_normalized": str(row.get("run_mode", "")).strip().lower(),
        "is_deploy_mode": is_deploy,
        "deploy_only_reject_reasons": list(reasons.deploy_only_reject_reasons),
        "q_value_family": q_value_family,
        "q_value_cluster": q_value_cluster,
        "q_value_by": q_value_by,
        "q_value_program": q_value_program,
        "shrinkage_loso_stable": shrinkage_loso_stable,
        "shrinkage_borrowing_dominant": shrinkage_borrowing_dominant,
        "structural_robustness_score": structural_robustness_score,
        "repeated_fold_consistency": repeated_fold_consistency,
        "robustness_panel_complete": robustness_panel_complete,
        "num_regimes": num_regimes,
        "regime_stability_pass": regime_stability_pass,
        "structural_break_pass": structural_break_pass,
        "multiplicity_pass": multiplicity_pass,
        "robustness_pass": robustness_pass,
        "regime_pass": regime_pass,
        "validation_samples_raw": validation_samples_raw,
        "test_samples_raw": test_samples_raw,
        "validation_samples_effective": validation_samples,
        "test_samples_effective": test_samples,
        "oos_sample_source": oos_sample_source,
        "oos_direction_match": direction_match,
        "min_validation_events_required": int(min_val_events),
        "min_test_events_required": int(min_test_events),
        "oos_pass": oos_pass,
        "oos_evaluated": oos_evaluated,
        "low_capital_viability_pass": low_capital_viability_pass,
        "low_capital_viability_score": low_capital_viability_score,
        "low_capital_reject_codes": low_capital_reject_codes,
    }
