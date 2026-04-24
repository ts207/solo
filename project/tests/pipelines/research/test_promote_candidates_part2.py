from __future__ import annotations

import json
from types import SimpleNamespace

import pandas as pd
import pytest

from project.research.promotion import evaluate_row as _evaluate_row_impl
from project.research.promotion.promotion_reporting import (
    apply_portfolio_overlap_gate as _apply_portfolio_overlap_gate,
)
from project.research.promotion.promotion_reporting import (
    assign_and_validate_promotion_tiers as _assign_and_validate_promotion_tiers,
)
from project.research.promotion.promotion_reporting import (
    build_negative_control_diagnostics as _build_negative_control_diagnostics,
)
from project.research.promotion.promotion_reporting import (
    build_promotion_capital_footprint as _build_promotion_capital_footprint,
)
from project.research.promotion.promotion_reporting import (
    build_promotion_statistical_audit as _build_promotion_statistical_audit,
)
from project.research.promotion.promotion_reporting import (
    portfolio_diversification_violations as _portfolio_diversification_violations,
)

_LEGACY_PASS_FAIL_GATES = {
    "gate_promo_dsr",
    "gate_promo_low_capital_viability",
    "gate_promo_baseline_beats_complexity",
    "gate_promo_placebo_controls",
}


def _legacy_gate_value(value):
    if isinstance(value, bool):
        return "pass" if value else "fail"
    return value


def _evaluate_row(*args, **kwargs):
    result = _evaluate_row_impl(*args, **kwargs)
    for key in _LEGACY_PASS_FAIL_GATES:
        if key in result:
            result[key] = _legacy_gate_value(result[key])
    audit = result.get("promotion_audit")
    if isinstance(audit, dict):
        result["promotion_audit"] = {
            key: (_legacy_gate_value(value) if key in _LEGACY_PASS_FAIL_GATES else value)
            for key, value in audit.items()
        }
    return result


def test_promote_candidate_rejects_retail_viability_when_required():
    out = _evaluate_row(
        row={
            "event_type": "VOL_SHOCK",
            "candidate_id": "cand_retail",
            "plan_row_id": "p1",
            "q_value": 0.01,
            "n_events": 300,
            "effect_shrunk_state": 0.01,
            "std_return": 0.01,
            "gate_stability": True,
            "val_t_stat": 2.5,
            "oos1_t_stat": 2.0,
            "gate_after_cost_positive": True,
            "gate_after_cost_stressed_positive": True,
            "gate_bridge_after_cost_positive_validation": True,
            "gate_bridge_after_cost_stressed_positive_validation": True,
            "bridge_validation_after_cost_bps": 1.5,
            "avg_dynamic_cost_bps": 15.0,
            "turnover_proxy_mean": 9.0,
            "gate_delay_robustness": True,
            "validation_samples": 120,
            "tob_coverage": 0.9,
        },
        hypothesis_index={"p1": {"statuses": ["executed"], "executed": True}},
        negative_control_summary={"by_event": {"VOL_SHOCK": {"pass_rate_after_bh": 0.0}}},
        max_q_value=0.10,
        min_events=100,
        min_stability_score=0.05,
        min_sign_consistency=0.60,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.01,
        min_tob_coverage=0.8,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=False,
        min_net_expectancy_bps=4.0,
        max_fee_plus_slippage_bps=10.0,
        max_daily_turnover_multiple=4.0,
        require_retail_viability=True,
    )
    assert out["promotion_decision"] == "rejected"
    assert out["gate_promo_retail_viability"] == "fail"
    assert "retail_net_expectancy" in out["reject_reason"]
    assert "retail_cost_budget" in out["reject_reason"]
    assert "retail_turnover" in out["reject_reason"]


def test_promote_candidate_rejects_low_capital_viability_when_required():
    out = _evaluate_row(
        row={
            "event_type": "VOL_SHOCK",
            "candidate_id": "cand_low_cap",
            "plan_row_id": "p1",
            "q_value": 0.01,
            "n_events": 300,
            "effect_shrunk_state": 0.01,
            "std_return": 0.01,
            "gate_stability": True,
            "val_t_stat": 2.5,
            "oos1_t_stat": 2.0,
            "gate_after_cost_positive": True,
            "gate_after_cost_stressed_positive": True,
            "gate_bridge_after_cost_positive_validation": True,
            "gate_bridge_after_cost_stressed_positive_validation": True,
            "gate_bridge_low_capital_viability": False,
            "low_capital_viability_score": 0.33,
            "low_capital_reject_reason_codes": "LOW_CAP_COST_SURVIVAL_2X,LOW_CAP_LATENCY_STRESS",
            "gate_delay_robustness": True,
            "validation_samples": 120,
            "tob_coverage": 0.9,
        },
        hypothesis_index={"p1": {"statuses": ["executed"], "executed": True}},
        negative_control_summary={"by_event": {"VOL_SHOCK": {"pass_rate_after_bh": 0.0}}},
        max_q_value=0.10,
        min_events=100,
        min_stability_score=0.05,
        min_sign_consistency=0.60,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.01,
        min_tob_coverage=0.8,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=False,
        require_low_capital_viability=True,
    )
    assert out["promotion_decision"] == "rejected"
    assert out["gate_promo_low_capital_viability"] == "fail"
    assert "low_capital_viability" in out["reject_reason"]
    assert "low_cap_cost_survival_2x" in out["reject_reason"]


def test_negative_control_diagnostics_reports_event_coverage():
    audit_df = pd.DataFrame(
        [
            {
                "event_type": "VOL_SHOCK",
                "promotion_decision": "promoted",
                "control_pass_rate": 0.001,
                "control_rate_source": "summary.by_event.VOL_SHOCK.pass_rate_after_bh",
            },
            {
                "event_type": "VOL_SHOCK",
                "promotion_decision": "rejected",
                "control_pass_rate": None,
                "control_rate_source": "missing",
            },
            {
                "event_type": "LIQUIDITY_VACUUM",
                "promotion_decision": "rejected",
                "control_pass_rate": 0.02,
                "control_rate_source": "summary.global.pass_rate_after_bh",
            },
        ]
    )
    diagnostics = _build_negative_control_diagnostics(
        audit_df=audit_df,
        negative_control_summary={
            "global": {"pass_rate_after_bh": 0.01},
            "by_event": {"VOL_SHOCK": {"pass_rate_after_bh": 0.001}},
        },
        max_negative_control_pass_rate=0.01,
        allow_missing_negative_controls=False,
    )

    assert diagnostics["audit"]["candidates_total"] == 3
    assert diagnostics["audit"]["control_rate_missing_count"] == 1
    assert diagnostics["by_event"]["VOL_SHOCK"]["candidate_count"] == 2
    assert diagnostics["by_event"]["VOL_SHOCK"]["promoted_count"] == 1


def test_build_promotion_statistical_audit_populates_primary_fail_gate_and_trace():
    audit_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_promoted",
                "event_type": "VOL_SHOCK",
                "promotion_decision": "promoted",
                "promotion_track": "standard",
                "promotion_fail_gate_primary": "",
                "promotion_fail_reason_primary": "",
                "reject_reason": "",
                "q_value": 0.01,
                "n_events": 180,
                "promotion_min_events_threshold": 100,
                "stability_score": 0.7,
                "sign_consistency": 1.0,
                "cost_survival_ratio": 1.0,
                "control_pass_rate": 0.001,
                "control_rate_source": "summary.by_event.VOL_SHOCK.pass_rate_after_bh",
                "tob_coverage": 0.92,
                "validation_samples": 50,
                "net_expectancy_bps": 8.0,
                "effective_cost_bps": 4.0,
                "turnover_proxy_mean": 1.2,
                "promotion_score": 1.0,
                "gate_promo_statistical": True,
                "gate_promo_stability": True,
                "gate_promo_cost_survival": True,
                "gate_promo_negative_control": True,
                "gate_promo_hypothesis_audit": True,
                "gate_promo_tob_coverage": True,
                "gate_promo_oos_validation": True,
                "gate_promo_retail_viability": True,
            },
            {
                "candidate_id": "cand_redundant",
                "event_type": "VOL_SHOCK",
                "promotion_decision": "rejected",
                "promotion_track": "fallback_only",
                "promotion_fail_gate_primary": "",
                "promotion_fail_reason_primary": "",
                "reject_reason": "redundancy_gate",
                "q_value": 0.03,
                "n_events": 90,
                "promotion_min_events_threshold": 120,
                "stability_score": 0.2,
                "sign_consistency": 0.6,
                "cost_survival_ratio": 0.5,
                "control_pass_rate": 0.05,
                "control_rate_source": "summary.global.pass_rate_after_bh",
                "tob_coverage": 0.4,
                "validation_samples": 40,
                "net_expectancy_bps": -2.0,
                "effective_cost_bps": 15.0,
                "turnover_proxy_mean": 8.0,
                "promotion_score": 0.25,
                "gate_promo_statistical": False,
                "gate_promo_stability": False,
                "gate_promo_cost_survival": False,
                "gate_promo_negative_control": False,
                "gate_promo_hypothesis_audit": True,
                "gate_promo_tob_coverage": False,
                "gate_promo_oos_validation": True,
                "gate_promo_retail_viability": False,
            },
            {
                "candidate_id": "cand_unknown",
                "event_type": "LIQUIDITY_VACUUM",
                "promotion_decision": "rejected",
                "promotion_track": "fallback_only",
                "promotion_fail_gate_primary": "",
                "promotion_fail_reason_primary": "",
                "reject_reason": "",
                "q_value": 0.5,
                "n_events": 10,
                "promotion_min_events_threshold": 50,
                "stability_score": 0.1,
                "sign_consistency": 0.2,
                "cost_survival_ratio": 0.2,
                "control_pass_rate": None,
                "control_rate_source": "missing",
                "tob_coverage": 0.1,
                "validation_samples": 0,
                "net_expectancy_bps": -10.0,
                "effective_cost_bps": 20.0,
                "turnover_proxy_mean": 10.0,
                "promotion_score": 0.0,
                "gate_promo_statistical": False,
                "gate_promo_stability": False,
                "gate_promo_cost_survival": False,
                "gate_promo_negative_control": False,
                "gate_promo_hypothesis_audit": False,
                "gate_promo_tob_coverage": False,
                "gate_promo_oos_validation": False,
                "gate_promo_retail_viability": False,
            },
        ]
    )
    out = _build_promotion_statistical_audit(
        audit_df=audit_df,
        max_q_value=0.10,
        min_stability_score=0.05,
        min_sign_consistency=0.67,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.01,
        min_tob_coverage=0.8,
        min_net_expectancy_bps=2.0,
        max_fee_plus_slippage_bps=10.0,
        max_daily_turnover_multiple=5.0,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=False,
        require_retail_viability=True,
        require_low_capital_viability=True,
    )

    assert len(out) == 3
    redundant_row = out[out["candidate_id"] == "cand_redundant"].iloc[0]
    assert redundant_row["promotion_fail_gate_primary"] == "gate_promo_redundancy"
    assert redundant_row["promotion_fail_reason_primary"] == "failed_gate_promo_redundancy"

    unknown_row = out[out["candidate_id"] == "cand_unknown"].iloc[0]
    assert unknown_row["promotion_fail_gate_primary"] == "gate_promo_unknown"
    assert unknown_row["promotion_fail_reason_primary"] == "failed_gate_promo_unknown"

    trace = json.loads(str(redundant_row["promotion_metrics_trace"]))
    assert trace["statistical"]["thresholds"]["max_q_value"] == 0.10
    assert trace["negative_control"]["thresholds"]["allow_missing_negative_controls"] is False
    assert trace["retail"]["thresholds"]["min_tob_coverage"] == 0.8
    assert "promotion_gate_evidence_json" in out.columns


def test_build_promotion_statistical_audit_avoids_warning_spam_for_missing_optional_fields(caplog):
    audit_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_sparse",
                "event_type": "VOL_SHOCK",
                "promotion_decision": "rejected",
                "promotion_track": "fallback_only",
                "promotion_fail_gate_primary": "",
                "promotion_fail_reason_primary": "",
                "reject_reason": "",
                "q_value": None,
                "n_events": 0,
                "promotion_min_events_threshold": 50,
            }
        ]
    )

    with caplog.at_level("WARNING"):
        out = _build_promotion_statistical_audit(
            audit_df=audit_df,
            max_q_value=0.10,
            min_stability_score=0.05,
            min_sign_consistency=0.67,
            min_cost_survival_ratio=0.75,
            max_negative_control_pass_rate=0.01,
            min_tob_coverage=0.8,
            min_net_expectancy_bps=2.0,
            max_fee_plus_slippage_bps=10.0,
            max_daily_turnover_multiple=5.0,
            require_hypothesis_audit=True,
            allow_missing_negative_controls=False,
            require_retail_viability=True,
            require_low_capital_viability=True,
        )

    assert len(out) == 1
    assert "safe_float: failed to convert None" not in caplog.text
    assert "safe_int: failed to convert None" not in caplog.text


def test_build_promotion_capital_footprint_report():
    promoted_df = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "event_type": "VOL_SHOCK",
                "promotion_track": "standard",
                "capacity_proxy": 0.5,
                "turnover_proxy_mean": 1.0,
            },
            {
                "candidate_id": "c2",
                "event_type": "LIQUIDITY_VACUUM",
                "promotion_track": "fallback_only",
                "capacity_proxy": None,
                "turnover_proxy_mean": 6.0,
            },
        ]
    )
    contract = SimpleNamespace(
        target_account_size_usd=10000.0,
        capital_budget_usd=5000.0,
        effective_per_position_notional_cap_usd=2500.0,
        max_concurrent_positions=2,
        max_daily_turnover_multiple=4.0,
    )
    out, summary = _build_promotion_capital_footprint(
        promoted_df=promoted_df,
        contract=contract,
    )
    assert len(out) == 2
    c1 = out[out["candidate_id"] == "c1"].iloc[0]
    assert c1["usage_signal_source"] == "capacity_proxy"
    assert abs(float(c1["estimated_position_notional_usd"]) - 2500.0) < 1e-9
    c2 = out[out["candidate_id"] == "c2"].iloc[0]
    assert c2["usage_signal_source"] == "turnover_ratio"
    assert abs(float(c2["slot_pressure_fraction"]) - 2.0) < 1e-9
    assert summary["slot_pressure_over_limit_count"] == 1


def test_apply_portfolio_overlap_gate_drops_high_overlap_candidates():
    promoted_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_a",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "condition": "all",
                "action": "long",
                "direction_rule": "long_only",
                "horizon": 5,
                "selection_score": 9.0,
            },
            {
                "candidate_id": "cand_b",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "condition": "all",
                "action": "long",
                "direction_rule": "long_only",
                "horizon": 5,
                "selection_score": 8.0,
            },
            {
                "candidate_id": "cand_c",
                "event_type": "LIQUIDITY_VACUUM",
                "symbol": "ETHUSDT",
                "condition": "vol_regime_high",
                "action": "short",
                "direction_rule": "short_only",
                "horizon": 15,
                "selection_score": 7.0,
            },
        ]
    )

    kept_df, dropped_df = _apply_portfolio_overlap_gate(
        promoted_df=promoted_df,
        max_overlap_ratio=0.80,
    )

    assert kept_df["candidate_id"].tolist() == ["cand_a", "cand_c"]
    assert dropped_df["candidate_id"].tolist() == ["cand_b"]
    dropped = dropped_df.iloc[0].to_dict()
    assert dropped["overlap_with_candidate_id"] == "cand_a"
    assert float(dropped["overlap_score"]) >= 0.80


def test_portfolio_diversification_violations_detect_overlap_and_correlation():
    promoted_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_a",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "condition": "all",
                "action": "long",
                "direction_rule": "long_only",
                "horizon": 5,
                "delay_expectancy_map": '{"0": 0.10, "4": 0.20, "8": 0.30}',
            },
            {
                "candidate_id": "cand_b",
                "event_type": "VOL_SHOCK",
                "symbol": "BTCUSDT",
                "condition": "all",
                "action": "long",
                "direction_rule": "long_only",
                "horizon": 5,
                "delay_expectancy_map": '{"0": 0.11, "4": 0.21, "8": 0.31}',
            },
            {
                "candidate_id": "cand_c",
                "event_type": "LIQUIDITY_VACUUM",
                "symbol": "ETHUSDT",
                "condition": "vol_regime_high",
                "action": "short",
                "direction_rule": "short_only",
                "horizon": 15,
                "delay_expectancy_map": '{"0": -0.05, "4": 0.01, "8": -0.02}',
            },
        ]
    )

    diagnostics = _portfolio_diversification_violations(
        promoted_df=promoted_df,
        max_profile_correlation=0.85,
        max_overlap_ratio=0.80,
    )

    assert diagnostics["pair_count_total"] == 3
    assert diagnostics["correlation_violation_count"] >= 1
    assert diagnostics["overlap_violation_count"] >= 1


def test_assign_and_validate_promotion_tiers_maps_expected_tiers():
    audit_df = pd.DataFrame(
        [
                {
                    "candidate_id": "deployable_1",
                    "promotion_decision": "promoted",
                    "promotion_track": "standard",
                    "gate_promo_retail_viability": True,
                    "gate_promo_redundancy": True,
                    "gate_bridge_tradable": True,
                    "effective_q_value": 0.01,
                    "test_q_value": 0.01,
                    "n_events": 250,
                    "validation_samples": 100,
                    "test_samples": 100,
                    "gate_promo_dsr": "pass",
                    "dsr_value": 0.8,
                    "cost_survival_ratio": 1.2,
                    "gate_regime_stability": True,
                    "num_regimes_supported": 3,
                    "gate_promo_robustness": "pass",
                    "gate_promo_multiplicity_confirmatory": "pass",
                    "gate_promo_multiplicity_diagnostics": "pass",
                },
            {
                "candidate_id": "shadow_1",
                "promotion_decision": "promoted",
                "promotion_track": "fallback_only",
                "gate_promo_retail_viability": True,
                "gate_promo_redundancy": True,
                "gate_bridge_tradable": True,
            },
            {
                "candidate_id": "research_1",
                "promotion_decision": "rejected",
                "promotion_track": "fallback_only",
                "gate_promo_retail_viability": False,
                "gate_promo_redundancy": False,
            },
        ]
    )
    promoted_df = pd.DataFrame(
        [
                {
                    "candidate_id": "deployable_1",
                    "promotion_decision": "promoted",
                    "promotion_track": "standard",
                    "gate_promo_retail_viability": True,
                    "gate_promo_redundancy": True,
                    "gate_bridge_tradable": True,
                    "effective_q_value": 0.01,
                    "test_q_value": 0.01,
                    "n_events": 250,
                    "validation_samples": 100,
                    "test_samples": 100,
                    "gate_promo_dsr": "pass",
                    "dsr_value": 0.8,
                    "cost_survival_ratio": 1.2,
                    "gate_regime_stability": True,
                    "num_regimes_supported": 3,
                    "gate_promo_robustness": "pass",
                    "gate_promo_multiplicity_confirmatory": "pass",
                    "gate_promo_multiplicity_diagnostics": "pass",
                },
            {
                "candidate_id": "shadow_1",
                "promotion_decision": "promoted",
                "promotion_track": "fallback_only",
                "gate_promo_retail_viability": True,
                "gate_promo_redundancy": True,
                "gate_bridge_tradable": True,
            },
        ]
    )

    audit_out, promoted_out, tier_counts = _assign_and_validate_promotion_tiers(
        audit_df=audit_df,
        promoted_df=promoted_df,
        require_retail_viability=True,
    )

    tier_by_id = dict(
        zip(
            audit_out["candidate_id"].astype(str).tolist(),
            audit_out["promotion_tier"].astype(str).tolist(),
        )
    )
    assert tier_by_id["deployable_1"] == "live_eligible"
    assert tier_by_id["shadow_1"] == "paper_eligible"
    assert tier_by_id["research_1"] == "research_promoted"
    assert sorted(promoted_out["promotion_tier"].unique().tolist()) == [
        "live_eligible",
        "paper_eligible",
    ]
    assert tier_counts == {"live_eligible": 1, "paper_eligible": 1, "research_promoted": 1}


def test_assign_and_validate_promotion_tiers_rejects_research_rows_in_promoted_output():
    audit_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_bad",
                "promotion_decision": "rejected",
                "promotion_track": "fallback_only",
                "gate_promo_retail_viability": False,
                "gate_promo_redundancy": False,
            }
        ]
    )
    promoted_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_bad",
                "promotion_decision": "rejected",
                "promotion_track": "fallback_only",
                "gate_promo_retail_viability": False,
                "gate_promo_redundancy": False,
            }
        ]
    )

    with pytest.raises(ValueError, match="promoted output cannot contain tier=research_promoted"):
        _assign_and_validate_promotion_tiers(
            audit_df=audit_df,
            promoted_df=promoted_df,
            require_retail_viability=True,
        )
