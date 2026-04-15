from __future__ import annotations

import json

from types import SimpleNamespace

import pandas as pd
import pytest

from project.research.promotion import evaluate_row as _evaluate_row_impl
from project.research.promotion.promotion_reporting import (
    apply_portfolio_overlap_gate as _apply_portfolio_overlap_gate,
    assign_and_validate_promotion_tiers as _assign_and_validate_promotion_tiers,
    build_promotion_capital_footprint as _build_promotion_capital_footprint,
    build_negative_control_diagnostics as _build_negative_control_diagnostics,
    build_promotion_statistical_audit as _build_promotion_statistical_audit,
    portfolio_diversification_violations as _portfolio_diversification_violations,
    stabilize_promoted_output_schema as _stabilize_promoted_output_schema,
)
from project.research.services.promotion_service import (
    _load_bridge_metrics,
    _load_dynamic_min_events_by_event,
    _merge_bridge_metrics,
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


def _eval_row(**overrides):
    row = {
        "event_type": "VOL_SHOCK",
        "candidate_id": "cand_1",
        "plan_row_id": "p1",
        "q_value": 0.01,
        "n_events": 250,
        "effect_shrunk_state": 0.02,
        "std_return": 0.01,
        "gate_stability": True,
        "val_t_stat": 2.5,
        "oos1_t_stat": 2.0,
        "gate_after_cost_positive": True,
        "gate_after_cost_stressed_positive": True,
        "gate_bridge_after_cost_positive_validation": True,
        "gate_bridge_after_cost_stressed_positive_validation": False,
        "gate_delay_robustness": True,
        "validation_samples": 100,
        "baseline_expectancy_bps": 5.0,
        "bridge_validation_after_cost_bps": 20.0,
        "pass_shift_placebo": True,
        "pass_random_entry_placebo": True,
        "pass_direction_reversal_placebo": True,
        "event_is_descriptive": False,
        "event_is_trade_trigger": True,
        "gate_delayed_entry_stress": True,
        "gate_bridge_microstructure": True,
        "net_expectancy_bps": 20.0,
    }
    row.update(overrides)
    return _evaluate_row(
        row=row,
        hypothesis_index={"p1": {"statuses": ["executed"], "executed": True}},
        negative_control_summary={"by_event": {"VOL_SHOCK": {"pass_rate_after_bh": 0.0}}},
        max_q_value=0.10,
        min_events=100,
        min_stability_score=0.05,
        min_sign_consistency=0.60,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.01,
        min_tob_coverage=0.0,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=False,
    )


def test_promote_candidate_happy_path():
    out = _eval_row()
    assert out["promotion_decision"] == "promoted"
    assert out["reject_reason"] == ""
    assert out["gate_promo_statistical"] == "pass"
    assert out["gate_promo_stability"] == "pass"
    assert out["gate_promo_cost_survival"] == "pass"
    assert out["gate_promo_negative_control"] == "pass"
    assert out["gate_promo_hypothesis_audit"] == "pass"


def test_load_bridge_metrics_prefers_versioned_enriched_snapshot(tmp_path):
    bridge_root = tmp_path / "bridge_eval"
    event_dir = bridge_root / "VOL_SHOCK"
    event_dir.mkdir(parents=True, exist_ok=True)
    (event_dir / "bridge_candidate_metrics.csv").write_text(
        "candidate_id,event_type,gate_bridge_tradable\nc1,VOL_SHOCK,0\n",
        encoding="utf-8",
    )
    (event_dir / "phase2_candidates_bridge_eval_v1.csv").write_text(
        "candidate_id,event_type,gate_bridge_tradable,bridge_validation_after_cost_bps\n"
        "c1,VOL_SHOCK,1,12.5\n",
        encoding="utf-8",
    )

    out = _load_bridge_metrics(bridge_root)
    assert len(out) == 1
    assert bool(out.iloc[0]["gate_bridge_tradable"]) is True
    assert float(out.iloc[0]["bridge_validation_after_cost_bps"]) == 12.5


def test_load_bridge_metrics_reads_bridge_evaluation_parquet(tmp_path):
    bridge_root = tmp_path / "bridge_eval"
    event_dir = bridge_root / "VOL_SHOCK"
    event_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "event_type": "VOL_SHOCK",
                "gate_bridge_tradable": True,
                "bridge_validation_after_cost_bps": 9.5,
            }
        ]
    ).to_parquet(event_dir / "bridge_evaluation.parquet", index=False)

    out = _load_bridge_metrics(bridge_root)
    assert len(out) == 1
    assert bool(out.iloc[0]["gate_bridge_tradable"]) is True
    assert float(out.iloc[0]["bridge_validation_after_cost_bps"]) == 9.5


def test_merge_bridge_metrics_overrides_phase2_bridge_fields():
    phase2_df = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "event_type": "VOL_SHOCK",
                "gate_bridge_tradable": False,
            }
        ]
    )
    bridge_df = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "event_type": "VOL_SHOCK",
                "gate_bridge_tradable": True,
                "bridge_validation_after_cost_bps": 7.0,
            }
        ]
    )
    merged = _merge_bridge_metrics(phase2_df=phase2_df, bridge_df=bridge_df)
    assert bool(merged.iloc[0]["gate_bridge_tradable"]) is True
    assert float(merged.iloc[0]["bridge_validation_after_cost_bps"]) == 7.0


def test_stabilize_promoted_output_schema_keeps_contract_columns_when_empty():
    audit_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "run_id": "r1",
                "symbol": "BTCUSDT",
                "event": "VOL_SHOCK",
                "event_type": "VOL_SHOCK",
                "promotion_decision": "rejected",
                "promotion_tier": "research",
                "selection_score": 0.1,
                "n_events": 100,
                "gate_bridge_tradable": False,
            }
        ]
    )
    promoted_df = pd.DataFrame(columns=["promotion_tier"])
    out = _stabilize_promoted_output_schema(promoted_df=promoted_df, audit_df=audit_df)
    assert out.empty
    for col in [
        "candidate_id",
        "event_type",
        "status",
        "promotion_decision",
        "promotion_tier",
        "selection_score",
        "gate_bridge_tradable",
    ]:
        assert col in out.columns


def test_load_dynamic_min_events_by_event_uses_source_event_and_max_threshold(tmp_path):
    spec_dir = tmp_path / "spec" / "states"
    spec_dir.mkdir(parents=True, exist_ok=True)
    (spec_dir / "state_registry.yaml").write_text(
        (
            "version: 1\n"
            "kind: state_registry\n"
            "defaults:\n"
            "  min_events: 200\n"
            "states:\n"
            "  - state_id: A\n"
            "    family: VOLATILITY_TRANSITION\n"
            "    source_event_type: VOL_SHOCK\n"
            "    min_events: 250\n"
            "  - state_id: B\n"
            "    family: VOLATILITY_TRANSITION\n"
            "    source_event_type: VOL_SHOCK\n"
            "    min_events: 300\n"
            "  - state_id: C\n"
            "    family: LIQUIDITY_DISLOCATION\n"
            "    source_event_type: LIQUIDITY_VACUUM\n"
        ),
        encoding="utf-8",
    )

    out = _load_dynamic_min_events_by_event(tmp_path)
    assert out["VOL_SHOCK"] == 300
    assert out["LIQUIDITY_VACUUM"] == 200


def test_load_dynamic_min_events_by_event_logs_warning_on_invalid_yaml(tmp_path, caplog):
    spec_dir = tmp_path / "spec" / "states"
    spec_dir.mkdir(parents=True, exist_ok=True)
    (spec_dir / "state_registry.yaml").write_text(
        "version: [\n",
        encoding="utf-8",
    )
    with caplog.at_level("WARNING"):
        out = _load_dynamic_min_events_by_event(tmp_path)
    assert out == {}
    assert any("Failed loading state_registry" in rec.message for rec in caplog.records)


def test_promote_candidate_rejects_cost_and_controls():
    out = _eval_row(
        gate_after_cost_positive=False,
        gate_after_cost_stressed_positive=False,
        gate_bridge_after_cost_positive_validation=False,
        gate_bridge_after_cost_stressed_positive_validation=False,
        control_pass_rate=0.25,
    )
    assert out["promotion_decision"] == "rejected"
    assert "cost_survival" in out["reject_reason"]
    assert "negative_control_fail" in out["reject_reason"]


def test_promote_candidate_research_profile_softens_baseline_placebo_and_timeframe():
    common = {
        "row": {
            "event_type": "BASIS_DISLOCATION",
            "candidate_id": "cand_research",
            "plan_row_id": "p1",
            "q_value": 0.01,
            "n_events": 80,
            "effect_shrunk_state": 0.01,
            "std_return": 0.01,
            "gate_stability": True,
            "val_t_stat": 2.5,
            "oos1_t_stat": 2.0,
            "gate_after_cost_positive": True,
            "gate_after_cost_stressed_positive": True,
            "gate_bridge_after_cost_positive_validation": True,
            "gate_bridge_after_cost_stressed_positive_validation": True,
            "bridge_validation_after_cost_bps": 2.0,
            "baseline_expectancy_bps": 5.0,
            "pass_shift_placebo": False,
            "pass_random_entry_placebo": False,
            "pass_direction_reversal_placebo": False,
            "gate_delay_robustness": True,
            "validation_samples": 40,
            "test_samples": 25,
            "mean_validation_return": 0.01,
            "mean_test_return": 0.01,
            "gate_bridge_microstructure": True,
            "gate_delayed_entry_stress": True,
            "gate_timeframe_consensus": False,
            "event_is_descriptive": False,
            "event_is_trade_trigger": True,
        },
        "hypothesis_index": {"p1": {"statuses": ["executed"], "executed": True}},
        "negative_control_summary": {
            "by_event": {"BASIS_DISLOCATION": {"pass_rate_after_bh": 0.0}}
        },
        "max_q_value": 0.10,
        "min_events": 50,
        "min_stability_score": 0.05,
        "min_sign_consistency": 0.60,
        "min_cost_survival_ratio": 0.75,
        "max_negative_control_pass_rate": 0.01,
        "min_tob_coverage": 0.0,
        "require_hypothesis_audit": True,
        "allow_missing_negative_controls": False,
    }

    deploy_out = _evaluate_row(
        **common,
        promotion_profile="deploy",
        enforce_baseline_beats_complexity=True,
        enforce_placebo_controls=True,
        enforce_timeframe_consensus=True,
    )
    research_out = _evaluate_row(
        **common,
        promotion_profile="research",
        enforce_baseline_beats_complexity=False,
        enforce_placebo_controls=False,
        enforce_timeframe_consensus=False,
    )

    assert deploy_out["promotion_decision"] == "rejected"
    assert research_out["promotion_decision"] == "promoted"
    assert research_out["promotion_profile"] == "research"
    assert deploy_out["gate_promo_baseline_beats_complexity"] == "fail"
    assert deploy_out["gate_promo_placebo_controls"] == "fail"
    assert deploy_out["gate_promo_timeframe_consensus"] == "fail"


def test_promote_candidate_rejects_missing_hypothesis_audit():
    out = _evaluate_row(
        row={
            "event_type": "VOL_SHOCK",
            "candidate_id": "cand_2",
            "plan_row_id": "missing",
            "q_value": 0.01,
            "n_events": 200,
            "effect_shrunk_state": 0.01,
            "std_return": 0.01,
            "gate_stability": True,
            "gate_after_cost_positive": True,
            "gate_after_cost_stressed_positive": True,
            "gate_bridge_after_cost_positive_validation": True,
            "gate_bridge_after_cost_stressed_positive_validation": True,
            "gate_delay_robustness": True,
        },
        hypothesis_index={},
        negative_control_summary={"pass_rate_after_bh": 0.0},
        max_q_value=0.10,
        min_events=100,
        min_stability_score=0.05,
        min_sign_consistency=0.0,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.01,
        min_tob_coverage=0.0,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=True,
    )
    assert out["promotion_decision"] == "rejected"
    assert "hypothesis_missing_audit" in out["reject_reason"]


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
    assert tier_by_id["deployable_1"] == "deployable"
    assert tier_by_id["shadow_1"] == "shadow"
    assert tier_by_id["research_1"] == "research"
    assert sorted(promoted_out["promotion_tier"].unique().tolist()) == [
        "deployable",
        "shadow",
    ]
    assert tier_counts == {"deployable": 1, "shadow": 1, "research": 1}


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

    with pytest.raises(ValueError, match="promoted output cannot contain tier=research"):
        _assign_and_validate_promotion_tiers(
            audit_df=audit_df,
            promoted_df=promoted_df,
            require_retail_viability=True,
        )
