from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from project.core.exceptions import DataIntegrityError
from project.research.promotion.core import build_promotion_statistical_audit
from project.research.validation.evidence_bundle import (
    PromotionPolicy,
    build_evidence_bundle,
    bundle_to_flat_record,
    evaluate_promotion_bundle,
    serialize_evidence_bundles,
)
from project.research.validation.regime_tests import build_stability_result_from_row


def test_build_stability_result_from_row_detects_regime_flip_and_symbol_consistency():
    row = {
        "effect_shrunk_state": 0.02,
        "std_return": 0.01,
        "val_t_stat": 2.0,
        "oos1_t_stat": 1.5,
        "gate_regime_stability": True,
        "gate_delay_robustness": True,
        "regime_mean_map": {"low": 10.0, "high": -5.0},
        "regime_counts": {"low": 40, "high": 35},
        "symbol_expectancy_map": {"BTCUSDT": 8.0, "ETHUSDT": 3.0},
        "mean_train_return": 0.010,
        "mean_validation_return": 0.012,
        "mean_test_return": 0.009,
    }
    result = build_stability_result_from_row(row)
    payload = result.to_dict()
    assert payload["regime_flip_flag"] is True
    assert payload["cross_symbol_sign_consistency"] == 1.0
    assert payload["rolling_instability_score"] >= 0.0
    assert payload["details"]["by_regime"]["low"]["n_obs"] == 40


def test_build_stability_result_from_row_raises_on_malformed_regime_mapping():
    row = {
        "effect_shrunk_state": 0.02,
        "std_return": 0.01,
        "expectancy_by_regime_bps": "{not valid json",
    }

    with pytest.raises(DataIntegrityError, match="Failed to parse stability mapping JSON"):
        build_stability_result_from_row(row)


def test_evidence_bundle_policy_and_serialization(tmp_path: Path):
    row = {
        "candidate_id": "cand_1",
        "run_id": "r1",
        "event_type": "VOL_SHOCK",
        "n_events": 250,
        "validation_samples": 120,
        "test_samples": 80,
        "estimate_bps": 18.0,
        "stderr_bps": 4.0,
        "ci_low_bps": 8.0,
        "ci_high_bps": 28.0,
        "q_value": 0.02,
        "q_value_by": 0.04,
        "q_value_cluster": 0.03,
        "effect_shrunk_state": 0.02,
        "std_return": 0.01,
        "val_t_stat": 2.5,
        "oos1_t_stat": 2.1,
        "gate_stability": True,
        "gate_delay_robustness": True,
        "gate_timeframe_consensus": True,
        "gate_bridge_microstructure": True,
        "gate_after_cost_stressed_positive": True,
        "gate_promo_hypothesis_audit": True,
        "gate_promo_oos_validation": True,
        "gate_promo_retail_viability": True,
        "gate_promo_baseline_beats_complexity": True,
        "gate_delayed_entry_stress": True,
        "gate_promo_placebo_controls": True,
        "gate_promo_dsr": True,
        "gate_promo_robustness": True,
        "gate_promo_regime": True,
        "gate_promo_multiplicity_confirmatory": True,
        "cost_survival_ratio": 1.0,
        "tob_coverage": 0.95,
        "net_expectancy_bps": 16.0,
        "pass_shift_placebo": True,
        "pass_random_entry_placebo": True,
        "pass_direction_reversal_placebo": True,
        "event_is_descriptive": False,
        "event_is_trade_trigger": True,
    }
    policy = PromotionPolicy(
        max_q_value=0.10,
        min_events=100,
        min_stability_score=0.05,
        min_sign_consistency=0.50,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.05,
        min_tob_coverage=0.80,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=True,
        require_multiplicity_diagnostics=True,
        require_retail_viability=True,
    )
    bundle = build_evidence_bundle(
        row,
        control_rate=0.0,
        max_negative_control_pass_rate=policy.max_negative_control_pass_rate,
        allow_missing_negative_controls=policy.allow_missing_negative_controls,
        policy_version=policy.policy_version,
        bundle_version=policy.bundle_version,
    )
    decision = evaluate_promotion_bundle(bundle, policy)
    bundle["promotion_decision"] = decision
    assert decision["promotion_status"] == "promoted"
    assert decision["promotion_track"] == "standard"
    flat = bundle_to_flat_record(bundle)
    assert flat["promotion_decision"] == "promoted"
    assert "plan_row_id" in flat
    assert "bridge_certified" in flat
    assert "q_value_by" in flat
    assert "q_value_cluster" in flat
    out = tmp_path / "bundles.jsonl"
    serialize_evidence_bundles([bundle], out)
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["candidate_id"] == "cand_1"
    assert payload["promotion_decision"]["promotion_status"] == "promoted"


def test_build_evidence_bundle_accepts_vectorized_returns_oos_combined():
    row = {
        "candidate_id": "cand_vector",
        "event_type": "VOL_SHOCK",
        "returns_oos_combined": np.array([0.1] * 12),
    }

    bundle = build_evidence_bundle(row)

    assert bundle["metadata"]["has_realized_oos_path"] is True



def test_build_evidence_bundle_accepts_legacy_serialized_returns_oos_combined():
    row = {
        "candidate_id": "cand_legacy",
        "event_type": "VOL_SHOCK",
        "returns_oos_combined": "[np.float64(0.1), np.float64(0.2)]",
    }

    bundle = build_evidence_bundle(row)

    assert bundle["metadata"]["has_realized_oos_path"] is False
    assert bundle["sample_definition"]["n_events"] == 0


def test_build_evidence_bundle_rejects_malformed_serialized_returns_oos_combined():
    row = {
        "candidate_id": "cand_bad_text",
        "event_type": "VOL_SHOCK",
        "returns_oos_combined": "not a valid vector",
    }

    with pytest.raises(ValueError, match="serialized as text"):
        build_evidence_bundle(row)


def test_build_evidence_bundle_rejects_object_returns_oos_combined():
    row = {
        "candidate_id": "cand_bad",
        "event_type": "VOL_SHOCK",
        "returns_oos_combined": {"unexpected": 1},
    }

    with pytest.raises(ValueError, match="array-like"):
        build_evidence_bundle(row)


def test_build_promotion_statistical_audit_retains_bundle_reporting_fields():
    audit_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "promotion_decision": "promoted",
                "promotion_track": "standard",
                "promotion_tier": "deployable",
                "fallback_used": False,
                "fallback_reason": "",
                "promotion_fail_gate_primary": "",
                "promotion_fail_reason_primary": "",
                "reject_reason": "",
                "bundle_rejection_reasons": "",
                "q_value": 0.02,
                "q_value_by": 0.03,
                "q_value_cluster": 0.04,
                "q_value_program": 0.05,
                "n_events": 250,
                "promotion_min_events_threshold": 100,
                "stability_score": 2.0,
                "sign_consistency": 1.0,
                "cost_survival_ratio": 1.0,
                "control_pass_rate": 0.0,
                "control_rate_source": "candidate_row",
                "tob_coverage": 0.95,
                "validation_samples_raw": 120.0,
                "test_samples_raw": 80.0,
                "validation_samples": 120,
                "test_samples": 80,
                "oos_sample_source": "row.validation_samples",
                "oos_direction_match": True,
                "promotion_oos_min_validation_events": 50,
                "promotion_oos_min_test_events": 50,
                "bridge_validation_trades": 120,
                "baseline_expectancy_bps": 4.0,
                "net_expectancy_bps": 16.0,
                "effective_cost_bps": 3.0,
                "turnover_proxy_mean": 1.5,
                "plan_row_id": "plan_1",
                "bridge_certified": True,
                "has_realized_oos_path": True,
                "repeated_fold_consistency": 0.8,
                "structural_robustness_score": 0.9,
                "robustness_panel_complete": True,
                "gate_regime_stability": True,
                "gate_structural_break": True,
                "num_regimes_supported": 3,
                "promotion_score": 1.0,
                "gate_promo_statistical": True,
                "gate_promo_multiplicity_diagnostics": True,
                "gate_promo_multiplicity_confirmatory": True,
                "gate_promo_stability": True,
                "gate_promo_cost_survival": True,
                "gate_promo_negative_control": True,
                "gate_promo_falsification": True,
                "gate_promo_hypothesis_audit": True,
                "gate_promo_tob_coverage": True,
                "gate_promo_oos_validation": True,
                "gate_promo_microstructure": True,
                "gate_promo_retail_viability": True,
                "gate_promo_low_capital_viability": True,
                "gate_promo_baseline_beats_complexity": True,
                "gate_promo_timeframe_consensus": True,
                "gate_promo_event_discipline": True,
                "gate_promo_dsr": True,
                "gate_promo_robustness": True,
                "gate_promo_regime": True,
                "regime_flip_flag": False,
                "cross_symbol_sign_consistency": 1.0,
                "rolling_instability_score": 0.1,
                "bundle_version": "phase4_bundle_v1",
                "policy_version": "phase4_pr5_v1",
                "evidence_bundle_json": "{}",
            }
        ]
    )
    out = build_promotion_statistical_audit(
        audit_df=audit_df,
        max_q_value=0.10,
        min_stability_score=0.05,
        min_sign_consistency=0.50,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.05,
        min_tob_coverage=0.80,
        min_net_expectancy_bps=5.0,
        max_fee_plus_slippage_bps=10.0,
        max_daily_turnover_multiple=5.0,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=False,
        require_retail_viability=True,
        require_low_capital_viability=True,
    )
    assert "bundle_version" in out.columns
    assert "policy_version" in out.columns
    assert "evidence_bundle_json" in out.columns
    assert "gate_promo_falsification" in out.columns
    assert "plan_row_id" in out.columns
    assert "bridge_certified" in out.columns
    assert "q_value_by" in out.columns
    assert "q_value_cluster" in out.columns
    assert "baseline_expectancy_bps" in out.columns
    assert "gate_promo_baseline_beats_complexity" in out.columns
    assert "gate_promo_multiplicity_confirmatory" in out.columns
    assert "validation_samples_raw" in out.columns
    assert "test_samples_raw" in out.columns
    assert "test_samples" in out.columns
    assert "oos_sample_source" in out.columns
    assert "oos_direction_match" in out.columns
    assert "promotion_gate_evidence_json" in out.columns
    assert out.iloc[0]["promotion_decision"] == "promoted"
    trace = json.loads(out.iloc[0]["promotion_gate_evidence_json"])
    assert trace["oos_validation"]["observed"]["validation_samples_raw"] == 120.0
    assert trace["oos_validation"]["observed"]["validation_samples"] == 120
    assert trace["oos_validation"]["observed"]["test_samples_raw"] == 80.0
    assert trace["oos_validation"]["observed"]["test_samples"] == 80
    assert trace["oos_validation"]["observed"]["oos_sample_source"] == "row.validation_samples"
    assert trace["oos_validation"]["observed"]["oos_direction_match"] is True
    assert trace["oos_validation"]["thresholds"]["min_validation_samples"] == 50
    assert trace["oos_validation"]["thresholds"]["min_test_samples"] == 50
