from __future__ import annotations

import json
from typing import Any

import numpy as np
import pandas as pd

from project.core.coercion import as_bool
from project.research.promotion.promotion_reporting_support import (
    _quiet_float,
    _quiet_int,
    resolve_promotion_tier,
)
from project.research.utils.decision_safety import bool_gate


def build_negative_control_diagnostics(
    *,
    audit_df: pd.DataFrame,
    negative_control_summary: dict[str, Any],
    max_negative_control_pass_rate: float,
    allow_missing_negative_controls: bool,
) -> dict[str, Any]:
    global_input = negative_control_summary.get("global", {})
    by_event_input = negative_control_summary.get("by_event", {})
    if not isinstance(global_input, dict):
        global_input = {}
    if not isinstance(by_event_input, dict):
        by_event_input = {}

    output: dict[str, Any] = {
        "policy": {
            "max_negative_control_pass_rate": float(max_negative_control_pass_rate),
            "allow_missing_negative_controls": bool(allow_missing_negative_controls),
        },
        "inputs": {
            "has_global_summary": bool(global_input),
            "global_summary": dict(global_input),
            "has_by_event_summary": bool(by_event_input),
            "by_event_summary_events": sorted(str(k) for k in by_event_input.keys()),
        },
        "audit": {
            "candidates_total": len(audit_df),
            "control_rate_missing_count": 0,
            "promoted_with_missing_control_rate": 0,
        },
        "by_event": {},
    }

    if audit_df.empty:
        return output

    rates = pd.to_numeric(audit_df.get("control_pass_rate"), errors="coerce")
    output["audit"]["control_rate_missing_count"] = int(rates.isna().sum())
    promoted_mask = (
        audit_df.get("promotion_decision", pd.Series(dtype="object")).astype(str) == "promoted"
    )
    output["audit"]["promoted_with_missing_control_rate"] = int(
        (promoted_mask & rates.isna()).sum()
    )

    event_col = (
        "event_type"
        if "event_type" in audit_df.columns
        else ("event" if "event" in audit_df.columns else None)
    )
    if event_col is None:
        return output

    by_event_rows: dict[str, dict[str, Any]] = {}
    for event_type, sub in audit_df.groupby(event_col, sort=True):
        sub_rates = pd.to_numeric(sub.get("control_pass_rate"), errors="coerce")
        sources = (
            sub.get("control_rate_source", pd.Series(dtype="object"))
            .astype(str)
            .fillna("")
            .value_counts()
            .to_dict()
        )
        by_event_rows[str(event_type)] = {
            "candidate_count": len(sub),
            "promoted_count": int(
                (
                    sub.get("promotion_decision", pd.Series(dtype="object")).astype(str)
                    == "promoted"
                ).sum()
            ),
            "control_rate_missing_count": int(sub_rates.isna().sum()),
            "control_rate_mean": None
            if sub_rates.dropna().empty
            else float(sub_rates.dropna().mean()),
            "control_rate_max": None
            if sub_rates.dropna().empty
            else float(sub_rates.dropna().max()),
            "control_rate_source_counts": {str(k): int(v) for k, v in sources.items()},
        }
    output["by_event"] = by_event_rows
    return output


def build_promotion_statistical_audit(
    *,
    audit_df: pd.DataFrame,
    max_q_value: float,
    min_stability_score: float,
    min_sign_consistency: float,
    min_cost_survival_ratio: float,
    max_negative_control_pass_rate: float,
    min_tob_coverage: float,
    min_net_expectancy_bps: float,
    max_fee_plus_slippage_bps: float | None,
    max_daily_turnover_multiple: float | None,
    require_hypothesis_audit: bool,
    allow_missing_negative_controls: bool,
    require_retail_viability: bool,
    require_low_capital_viability: bool,
) -> pd.DataFrame:
    cols = [
        "candidate_id",
        "event_type",
        "template_verb",
        "promotion_decision",
        "promotion_track",
        "is_reduced_evidence",
        "fallback_used",
        "fallback_reason",
        "promotion_tier",
        "promotion_fail_gate_primary",
        "promotion_fail_reason_primary",
        "reject_reason",
        "bundle_rejection_reasons",
        "q_value",
        "q_value_by",
        "q_value_cluster",
        "q_value_program",
        "q_value_scope",
        "effective_q_value",
        "num_tests_family",
        "num_tests_campaign",
        "num_tests_effective",
        "num_tests_scope",
        "multiplicity_scope_mode",
        "multiplicity_scope_key",
        "multiplicity_scope_version",
        "multiplicity_scope_degraded",
        "multiplicity_scope_reason",
        "n_events",
        "promotion_min_events_threshold",
        "stability_score",
        "sign_consistency",
        "cost_survival_ratio",
        "control_pass_rate",
        "control_rate_source",
        "tob_coverage",
        "validation_samples_raw",
        "test_samples_raw",
        "validation_samples",
        "test_samples",
        "oos_sample_source",
        "oos_direction_match",
        "promotion_oos_min_validation_events",
        "promotion_oos_min_test_events",
        "bridge_validation_trades",
        "baseline_expectancy_bps",
        "net_expectancy_bps",
        "effective_cost_bps",
        "turnover_proxy_mean",
        "plan_row_id",
        "bridge_certified",
        "has_realized_oos_path",
        "repeated_fold_consistency",
        "structural_robustness_score",
        "robustness_panel_complete",
        "gate_regime_stability",
        "gate_structural_break",
        "num_regimes_supported",
        "low_capital_viability_score",
        "low_capital_reject_reason_codes",
        "promotion_score",
        "gate_bridge_tradable",
        "gate_promo_statistical",
        "gate_promo_multiplicity_diagnostics",
        "gate_promo_multiplicity_confirmatory",
        "gate_promo_stability",
        "gate_promo_cost_survival",
        "gate_promo_negative_control",
        "gate_promo_falsification",
        "gate_promo_hypothesis_audit",
        "gate_promo_tob_coverage",
        "gate_promo_oos_validation",
        "gate_promo_microstructure",
        "gate_promo_retail_viability",
        "gate_promo_low_capital_viability",
        "gate_promo_baseline_beats_complexity",
        "gate_promo_timeframe_consensus",
        "gate_promo_event_discipline",
        "gate_promo_continuation_quality",
        "gate_promo_dsr",
        "gate_promo_robustness",
        "gate_promo_regime",
        "regime_flip_flag",
        "cross_symbol_sign_consistency",
        "rolling_instability_score",
        "bundle_version",
        "policy_version",
        "evidence_bundle_json",
        "promotion_gate_evidence_json",
        "promotion_metrics_trace",
        "search_proposals_attempted",
        "search_candidates_generated",
        "search_candidates_scored",
        "search_candidates_eligible",
        "search_parameterizations_attempted",
        "search_mutations_attempted",
        "search_directions_tested",
        "search_confirmations_attempted",
        "search_trigger_variants_attempted",
        "search_family_count",
        "search_lineage_count",
        "search_scope_version",
        "search_burden_estimated",
    ]
    if audit_df.empty:
        return pd.DataFrame(columns=cols)

    records: list[dict[str, Any]] = []
    for row in audit_df.to_dict(orient="records"):
        decision = str(row.get("promotion_decision", "")).strip()
        primary_fail = str(row.get("promotion_fail_gate_primary", "")).strip()
        reason = str(row.get("reject_reason", "")).strip()
        if decision == "rejected" and not primary_fail:
            primary_fail = (
                "gate_promo_redundancy" if "redundancy_gate" in reason else "gate_promo_unknown"
            )
        primary_fail_reason = str(row.get("promotion_fail_reason_primary", "")).strip()
        if primary_fail and not primary_fail_reason:
            primary_fail_reason = f"failed_{primary_fail}"

        min_events_threshold = _quiet_int(
            row.get("promotion_min_events_threshold", row.get("n_events", 0)), 0
        )
        trace = {
            "statistical": {
                "passed": bool(row.get("gate_promo_statistical") == "pass"),
                "observed": {
                    "q_value": _quiet_float(row.get("q_value"), np.nan),
                    "n_events": _quiet_int(row.get("n_events"), 0),
                },
                "thresholds": {
                    "max_q_value": float(max_q_value),
                    "min_events": int(min_events_threshold),
                },
            },
            "stability": {
                "passed": bool(row.get("gate_promo_stability") == "pass"),
                "observed": {
                    "stability_score": _quiet_float(row.get("stability_score"), np.nan),
                    "sign_consistency": _quiet_float(row.get("sign_consistency"), np.nan),
                },
                "thresholds": {
                    "min_stability_score": float(min_stability_score),
                    "min_sign_consistency": float(min_sign_consistency),
                },
            },
            "cost_survival": {
                "passed": bool(row.get("gate_promo_cost_survival") == "pass"),
                "observed": {
                    "cost_survival_ratio": _quiet_float(row.get("cost_survival_ratio"), np.nan)
                },
                "thresholds": {"min_cost_survival_ratio": float(min_cost_survival_ratio)},
            },
            "negative_control": {
                "passed": bool(row.get("gate_promo_negative_control") == "pass"),
                "observed": {
                    "control_pass_rate": _quiet_float(row.get("control_pass_rate"), np.nan),
                    "control_rate_source": str(row.get("control_rate_source", "")),
                },
                "thresholds": {
                    "max_negative_control_pass_rate": float(max_negative_control_pass_rate),
                    "allow_missing_negative_controls": bool(allow_missing_negative_controls),
                },
            },
            "oos_validation": {
                "passed": bool(row.get("gate_promo_oos_validation") == "pass"),
                "observed": {
                    "validation_samples_raw": _quiet_float(
                        row.get("validation_samples_raw"), np.nan
                    ),
                    "test_samples_raw": _quiet_float(row.get("test_samples_raw"), np.nan),
                    "validation_samples": _quiet_int(row.get("validation_samples"), 0),
                    "test_samples": _quiet_int(row.get("test_samples"), 0),
                    "bridge_validation_trades": _quiet_int(
                        row.get("bridge_validation_trades"), 0
                    ),
                    "oos_sample_source": str(row.get("oos_sample_source", "")),
                    "oos_direction_match": bool(as_bool(row.get("oos_direction_match", False))),
                    "has_realized_oos_path": bool(as_bool(row.get("has_realized_oos_path", False))),
                },
                "thresholds": {
                    "min_validation_samples": _quiet_int(
                        row.get("promotion_oos_min_validation_events"), 1
                    ),
                    "min_test_samples": _quiet_int(
                        row.get("promotion_oos_min_test_events"), 0
                    ),
                },
            },
            "continuation_quality": {
                "passed": bool(row.get("gate_promo_continuation_quality") == "pass"),
                "observed": {
                    "template_verb": str(row.get("template_verb", "")),
                    "gate_bridge_tradable": str(row.get("gate_bridge_tradable", "fail")),
                },
                "thresholds": {
                    "continuation_family_requires_stability_oos_microstructure_dsr": True
                },
            },
            "retail": {
                "passed": bool(bool_gate(row.get("gate_promo_retail_viability"))),
                "observed": {
                    "tob_coverage": _quiet_float(row.get("tob_coverage"), np.nan),
                    "net_expectancy_bps": _quiet_float(row.get("net_expectancy_bps"), np.nan),
                    "effective_cost_bps": _quiet_float(row.get("effective_cost_bps"), np.nan),
                    "turnover_proxy_mean": _quiet_float(row.get("turnover_proxy_mean"), np.nan),
                },
                "thresholds": {
                    "min_tob_coverage": float(min_tob_coverage),
                    "min_net_expectancy_bps": float(min_net_expectancy_bps),
                    "max_fee_plus_slippage_bps": None
                    if max_fee_plus_slippage_bps is None
                    else float(max_fee_plus_slippage_bps),
                    "max_daily_turnover_multiple": None
                    if max_daily_turnover_multiple is None
                    else float(max_daily_turnover_multiple),
                    "require_retail_viability": bool(require_retail_viability),
                },
            },
            "low_capital": {
                "passed": bool(bool_gate(row.get("gate_promo_low_capital_viability"))),
                "observed": {
                    "low_capital_viability_score": _quiet_float(
                        row.get("low_capital_viability_score"), np.nan
                    ),
                    "low_capital_reject_reason_codes": [
                        token.strip()
                        for token in str(row.get("low_capital_reject_reason_codes", "")).split(",")
                        if token.strip()
                    ],
                },
                "thresholds": {
                    "require_low_capital_viability": bool(require_low_capital_viability)
                },
            },
            "hypothesis_audit": {
                "passed": bool(bool_gate(row.get("gate_promo_hypothesis_audit"))),
                "observed": {"plan_row_id": str(row.get("plan_row_id", "")).strip()},
                "thresholds": {"require_hypothesis_audit": bool(require_hypothesis_audit)},
            },
            "deploy_confirmatory": {
                "passed": bool(
                    bool(as_bool(row.get("bridge_certified", False)))
                    and bool(as_bool(row.get("robustness_panel_complete", False)))
                    and bool(as_bool(row.get("gate_regime_stability", False)))
                    and bool(as_bool(row.get("gate_structural_break", False)))
                ),
                "observed": {
                    "bridge_certified": bool(as_bool(row.get("bridge_certified", False))),
                    "robustness_panel_complete": bool(
                        as_bool(row.get("robustness_panel_complete", False))
                    ),
                    "repeated_fold_consistency": _quiet_float(
                        row.get("repeated_fold_consistency"), np.nan
                    ),
                    "structural_robustness_score": _quiet_float(
                        row.get("structural_robustness_score"), np.nan
                    ),
                    "gate_regime_stability": bool(as_bool(row.get("gate_regime_stability", False))),
                    "gate_structural_break": bool(as_bool(row.get("gate_structural_break", False))),
                    "num_regimes_supported": _quiet_int(row.get("num_regimes_supported"), 0),
                },
                "thresholds": {},
            },
            "bundle_policy": {
                "passed": bool(str(row.get("promotion_decision", "")).strip() == "promoted"),
                "observed": {
                    "regime_flip_flag": bool(as_bool(row.get("regime_flip_flag", False))),
                    "cross_symbol_sign_consistency": _quiet_float(
                        row.get("cross_symbol_sign_consistency"), np.nan
                    ),
                    "rolling_instability_score": _quiet_float(
                        row.get("rolling_instability_score"), np.nan
                    ),
                },
                "thresholds": {
                    "bundle_version": str(row.get("bundle_version", "")),
                    "policy_version": str(row.get("policy_version", "")),
                },
            },
        }

        records.append(
            {
                "candidate_id": str(row.get("candidate_id", "")).strip(),
                "event_type": str(row.get("event_type", row.get("event", ""))).strip(),
                "is_reduced_evidence": bool(row.get("is_reduced_evidence", False)),
                "template_verb": str(row.get("template_verb", "")).strip(),
                "promotion_decision": decision,
                "promotion_track": str(row.get("promotion_track", "")).strip(),
                "fallback_used": bool(as_bool(row.get("fallback_used", False))),
                "fallback_reason": str(row.get("fallback_reason", "")).strip(),
                "promotion_tier": str(
                    row.get("promotion_tier", resolve_promotion_tier(row))
                ).strip(),
                "promotion_fail_gate_primary": primary_fail,
                "promotion_fail_reason_primary": primary_fail_reason,
                "reject_reason": reason,
                "q_value": _quiet_float(row.get("q_value"), np.nan),
                "q_value_by": _quiet_float(row.get("q_value_by"), np.nan),
                "q_value_cluster": _quiet_float(row.get("q_value_cluster"), np.nan),
                "q_value_program": _quiet_float(row.get("q_value_program"), np.nan),
                "n_events": _quiet_int(row.get("n_events"), 0),
                "promotion_min_events_threshold": int(min_events_threshold),
                "stability_score": _quiet_float(row.get("stability_score"), np.nan),
                "sign_consistency": _quiet_float(row.get("sign_consistency"), np.nan),
                "cost_survival_ratio": _quiet_float(row.get("cost_survival_ratio"), np.nan),
                "control_pass_rate": _quiet_float(row.get("control_pass_rate"), np.nan),
                "control_rate_source": str(row.get("control_rate_source", "")),
                "tob_coverage": _quiet_float(row.get("tob_coverage"), np.nan),
                "validation_samples_raw": _quiet_float(row.get("validation_samples_raw"), np.nan),
                "test_samples_raw": _quiet_float(row.get("test_samples_raw"), np.nan),
                "validation_samples": _quiet_int(row.get("validation_samples"), 0),
                "test_samples": _quiet_int(row.get("test_samples"), 0),
                "oos_sample_source": str(row.get("oos_sample_source", "")),
                "oos_direction_match": bool(as_bool(row.get("oos_direction_match", False))),
                "promotion_oos_min_validation_events": _quiet_int(
                    row.get("promotion_oos_min_validation_events"), 0
                ),
                "promotion_oos_min_test_events": _quiet_int(
                    row.get("promotion_oos_min_test_events"), 0
                ),
                "bridge_validation_trades": _quiet_int(row.get("bridge_validation_trades"), 0),
                "baseline_expectancy_bps": _quiet_float(
                    row.get("baseline_expectancy_bps"), np.nan
                ),
                "net_expectancy_bps": _quiet_float(row.get("net_expectancy_bps"), np.nan),
                "effective_cost_bps": _quiet_float(row.get("effective_cost_bps"), np.nan),
                "turnover_proxy_mean": _quiet_float(row.get("turnover_proxy_mean"), np.nan),
                "plan_row_id": str(row.get("plan_row_id", "")).strip(),
                "bridge_certified": bool(as_bool(row.get("bridge_certified", False))),
                "has_realized_oos_path": bool(as_bool(row.get("has_realized_oos_path", False))),
                "repeated_fold_consistency": _quiet_float(
                    row.get("repeated_fold_consistency"), np.nan
                ),
                "structural_robustness_score": _quiet_float(
                    row.get("structural_robustness_score"), np.nan
                ),
                "robustness_panel_complete": bool(
                    as_bool(row.get("robustness_panel_complete", False))
                ),
                "gate_regime_stability": bool(as_bool(row.get("gate_regime_stability", False))),
                "gate_structural_break": bool(as_bool(row.get("gate_structural_break", False))),
                "num_regimes_supported": _quiet_int(row.get("num_regimes_supported"), 0),
                "low_capital_viability_score": _quiet_float(
                    row.get("low_capital_viability_score"), np.nan
                ),
                "low_capital_reject_reason_codes": str(
                    row.get("low_capital_reject_reason_codes", "")
                ),
                "promotion_score": _quiet_float(row.get("promotion_score"), np.nan),
                "gate_bridge_tradable": str(row.get("gate_bridge_tradable", "fail")),
                "gate_promo_statistical": str(row.get("gate_promo_statistical", "fail")),
                "gate_promo_multiplicity_diagnostics": str(
                    row.get("gate_promo_multiplicity_diagnostics", "fail")
                ),
                "gate_promo_multiplicity_confirmatory": str(
                    row.get("gate_promo_multiplicity_confirmatory", "fail")
                ),
                "gate_promo_stability": str(row.get("gate_promo_stability", "fail")),
                "gate_promo_cost_survival": str(row.get("gate_promo_cost_survival", "fail")),
                "gate_promo_negative_control": str(row.get("gate_promo_negative_control", "fail")),
                "gate_promo_hypothesis_audit": str(row.get("gate_promo_hypothesis_audit", "fail")),
                "gate_promo_tob_coverage": str(row.get("gate_promo_tob_coverage", "fail")),
                "gate_promo_oos_validation": str(row.get("gate_promo_oos_validation", "fail")),
                "gate_promo_microstructure": str(row.get("gate_promo_microstructure", "fail")),
                "gate_promo_retail_viability": str(row.get("gate_promo_retail_viability", "fail")),
                "gate_promo_low_capital_viability": str(
                    row.get("gate_promo_low_capital_viability", "fail")
                ),
                "gate_promo_baseline_beats_complexity": str(
                    row.get("gate_promo_baseline_beats_complexity", "fail")
                ),
                "gate_promo_timeframe_consensus": str(
                    row.get("gate_promo_timeframe_consensus", "fail")
                ),
                "gate_promo_event_discipline": str(row.get("gate_promo_event_discipline", "fail")),
                "gate_promo_continuation_quality": str(
                    row.get("gate_promo_continuation_quality", "fail")
                ),
                "gate_promo_dsr": str(row.get("gate_promo_dsr", "fail")),
                "gate_promo_robustness": str(row.get("gate_promo_robustness", "fail")),
                "gate_promo_regime": str(row.get("gate_promo_regime", "fail")),
                "gate_promo_falsification": str(
                    row.get(
                        "gate_promo_falsification", row.get("gate_promo_negative_control", "fail")
                    )
                ),
                "regime_flip_flag": bool(as_bool(row.get("regime_flip_flag", False))),
                "cross_symbol_sign_consistency": _quiet_float(
                    row.get("cross_symbol_sign_consistency"), np.nan
                ),
                "rolling_instability_score": _quiet_float(
                    row.get("rolling_instability_score"), np.nan
                ),
                "bundle_rejection_reasons": str(row.get("bundle_rejection_reasons", "")).strip(),
                "bundle_version": str(row.get("bundle_version", "")).strip(),
                "policy_version": str(row.get("policy_version", "")).strip(),
                "evidence_bundle_json": str(row.get("evidence_bundle_json", "")),
                "promotion_gate_evidence_json": json.dumps(trace, sort_keys=True),
                "promotion_metrics_trace": json.dumps(trace, sort_keys=True),
                "q_value_scope": _quiet_float(row.get("q_value_scope"), np.nan),
                "effective_q_value": _quiet_float(row.get("effective_q_value"), np.nan),
                "num_tests_family": _quiet_int(row.get("num_tests_family"), 0),
                "num_tests_campaign": _quiet_int(row.get("num_tests_campaign"), 0),
                "num_tests_effective": _quiet_int(row.get("num_tests_effective"), 0),
                "num_tests_scope": _quiet_int(row.get("num_tests_scope"), 0),
                "multiplicity_scope_mode": str(row.get("multiplicity_scope_mode", "")).strip(),
                "multiplicity_scope_key": str(row.get("multiplicity_scope_key", "")).strip(),
                "multiplicity_scope_version": str(row.get("multiplicity_scope_version", "")).strip(),
                "multiplicity_scope_degraded": bool(as_bool(row.get("multiplicity_scope_degraded", False))),
                "multiplicity_scope_reason": str(row.get("multiplicity_scope_reason", "")).strip(),
                "search_proposals_attempted": _quiet_int(row.get("search_proposals_attempted"), 0),
                "search_candidates_generated": _quiet_int(row.get("search_candidates_generated"), 0),
                "search_candidates_scored": _quiet_int(row.get("search_candidates_scored"), 0),
                "search_candidates_eligible": _quiet_int(row.get("search_candidates_eligible"), 0),
                "search_parameterizations_attempted": _quiet_int(row.get("search_parameterizations_attempted"), 0),
                "search_mutations_attempted": _quiet_int(row.get("search_mutations_attempted"), 0),
                "search_directions_tested": _quiet_int(row.get("search_directions_tested"), 0),
                "search_confirmations_attempted": _quiet_int(row.get("search_confirmations_attempted"), 0),
                "search_trigger_variants_attempted": _quiet_int(row.get("search_trigger_variants_attempted"), 0),
                "search_family_count": _quiet_int(row.get("search_family_count"), 0),
                "search_lineage_count": _quiet_int(row.get("search_lineage_count"), 0),
                "search_scope_version": str(row.get("search_scope_version", "")).strip(),
                "search_burden_estimated": bool(as_bool(row.get("search_burden_estimated", False))),
            }
        )
    return pd.DataFrame(records, columns=cols)
