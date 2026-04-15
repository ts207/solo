from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import project.research.services.promotion_service as svc
import project.research.validation.result_writer as validation_writer
from project.research.validation.contracts import (
    ValidationBundle,
    ValidatedCandidateRecord,
    ValidationDecision,
    ValidationMetrics,
)
from project.research.validation.result_writer import (
    write_promotion_ready_candidates,
    write_validation_bundle,
)


def _run_promotion(tmp_path, **overrides):
    config = svc.PromotionConfig(
        run_id="r1",
        symbols="",
        out_dir=tmp_path / "promotions",
        max_q_value=0.10,
        min_events=100,
        min_stability_score=0.05,
        min_sign_consistency=0.67,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.01,
        min_tob_coverage=0.60,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=False,
        require_multiplicity_diagnostics=False,
        min_dsr=0.5,
        max_overlap_ratio=0.80,
        max_profile_correlation=0.90,
        allow_discovery_promotion=False,
        program_id="default_program",
        retail_profile="capital_constrained",
        objective_name="",
        objective_spec=None,
        retail_profiles_spec=None,
    )
    if overrides:
        config = svc.PromotionConfig(**(config.__dict__ | overrides))
    return svc.execute_promotion(config)


def _write_validated_candidate_artifacts(tmp_path: Path, run_id: str, candidate_id: str) -> None:
    bundle = ValidationBundle(
        run_id=run_id,
        created_at="2026-01-01T00:00:00Z",
        validated_candidates=[
            ValidatedCandidateRecord(
                candidate_id=candidate_id,
                decision=ValidationDecision(
                    status="validated",
                    candidate_id=candidate_id,
                    run_id=run_id,
                    program_id="default_program",
                    reason_codes=[],
                ),
                metrics=ValidationMetrics(sample_count=100, q_value=0.01, stability_score=0.8),
            )
        ],
        rejected_candidates=[],
        inconclusive_candidates=[],
        summary_stats={"total": 1, "validated": 1},
        effect_stability_report={},
    )
    for base_dir in (
        tmp_path / "reports" / "validation" / run_id,
        tmp_path / "validation" / run_id,
        tmp_path.parent / "validation" / run_id,
    ):
        write_validation_bundle(bundle, base_dir=base_dir)
        write_promotion_ready_candidates(bundle, base_dir=base_dir)


def _valid_evidence_bundle_json(
    *, run_id: str, candidate_id: str, event_type: str = "VOL_SHOCK", symbol: str = "BTCUSDT"
) -> str:
    return json.dumps(
        {
            "candidate_id": candidate_id,
            "primary_event_id": event_type,
            "event_family": event_type,
            "event_type": event_type,
            "run_id": run_id,
            "sample_definition": {
                "n_events": 100,
                "validation_samples": 50,
                "test_samples": 50,
                "symbol": symbol,
            },
            "split_definition": {
                "split_scheme_id": "confirmatory",
                "purge_bars": 1,
                "embargo_bars": 1,
                "bar_duration_minutes": 5,
            },
            "effect_estimates": {
                "estimate": 0.08,
                "estimate_bps": 8.0,
                "stderr": 0.02,
                "stderr_bps": 2.0,
            },
            "uncertainty_estimates": {
                "ci_low": 0.02,
                "ci_high": 0.14,
                "ci_low_bps": 2.0,
                "ci_high_bps": 14.0,
                "p_value_raw": 0.01,
                "q_value": 0.01,
                "q_value_by": 0.01,
                "q_value_cluster": 0.01,
                "n_obs": 100,
                "n_clusters": 8,
            },
            "stability_tests": {
                "sign_consistency": 1.0,
                "stability_score": 0.9,
                "regime_stability_pass": True,
                "timeframe_consensus_pass": True,
                "delay_robustness_pass": True,
            },
            "falsification_results": {
                "shift_placebo_pass": True,
                "random_placebo_pass": True,
                "direction_reversal_pass": True,
                "negative_control_pass": True,
                "passes_control": True,
            },
            "cost_robustness": {
                "cost_survival_ratio": 1.0,
                "net_expectancy_bps": 6.0,
                "effective_cost_bps": 2.0,
                "turnover_proxy_mean": 1.0,
                "tob_coverage": 0.9,
                "tob_coverage_pass": True,
                "stressed_cost_pass": True,
                "retail_net_expectancy_pass": True,
                "retail_cost_budget_pass": True,
                "retail_turnover_pass": True,
            },
            "multiplicity_adjustment": {
                "correction_family_id": "default_program",
                "correction_method": "bh",
                "p_value_adj": 0.01,
                "p_value_adj_by": 0.01,
                "p_value_adj_holm": 0.01,
                "q_value_program": 0.01,
                "q_value_scope": 0.01,
                "effective_q_value": 0.01,
            },
            "metadata": {
                "plan_row_id": "plan-1",
                "hypothesis_id": "hyp-1",
                "tob_coverage": 0.9,
                "repeated_fold_consistency": 1.0,
                "structural_robustness_score": 0.8,
            },
            "promotion_decision": {
                "promotion_status": "promoted",
                "promotion_track": "standard",
                "eligible": True,
                "rank_score": 1.0,
            },
        }
    )


def test_run_promotion_service_smoke(monkeypatch, tmp_path):
    monkeypatch.setattr(svc, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(validation_writer, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(
        svc,
        "load_run_manifest",
        lambda run_id: {"run_mode": "confirmatory", "discovery_profile": "standard"},
    )
    monkeypatch.setattr(
        svc,
        "resolve_objective_profile_contract",
        lambda **kwargs: SimpleNamespace(
            min_net_expectancy_bps=5.0,
            max_fee_plus_slippage_bps=10.0,
            max_daily_turnover_multiple=5.0,
            require_retail_viability=False,
            require_low_capital_contract=False,
        ),
    )
    monkeypatch.setattr(svc, "ontology_spec_hash", lambda root: "hash")
    monkeypatch.setattr(svc, "_load_gates_spec", lambda root: {"promotion_confirmatory_gates": {}})
    monkeypatch.setattr(svc, "_load_negative_control_summary", lambda run_id: {})
    monkeypatch.setattr(svc, "_load_dynamic_min_events_by_event", lambda run_id: {})

    cand_path = tmp_path / "reports" / "edge_candidates" / "r1"
    cand_path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "family": "VOL_SHOCK",
                "n_events": 100,
                "stability_score": 0.8,
                "sign_consistency": 0.9,
                "cost_survival_ratio": 1.0,
                "net_expectancy_bps": 6.0,
                "q_value": 0.01,
                "confirmatory_locked": True,
                "frozen_spec_hash": "hash",
            }
        ]
    ).to_parquet(cand_path / "edge_candidates_normalized.parquet", index=False)
    burden_path = tmp_path / "reports" / "phase2" / "r1"
    burden_path.mkdir(parents=True, exist_ok=True)
    (burden_path / "search_burden_summary.json").write_text(
        json.dumps(
            {
                "search_burden_estimated": False,
                "search_candidates_generated": 10,
                "search_candidates_eligible": 5,
                "search_mutations_attempted": 0,
                "search_family_count": 4,
                "search_lineage_count": 5,
                "search_scope_version": "phase1_v1",
            }
        ),
        encoding="utf-8",
    )
    _write_validated_candidate_artifacts(tmp_path, "r1", "cand_1")

    audit_df = pd.DataFrame(
        [
            {
                    "candidate_id": "cand_1",
                    "event_type": "VOL_SHOCK",
                    "promotion_decision": "promoted",
                    "promotion_track": "standard",
                    "promotion_metrics_trace": "{}",
                    "evidence_bundle_json": _valid_evidence_bundle_json(
                        run_id="r1", candidate_id="cand_1"
                    ),
                }
            ]
        )
    promoted_df = pd.DataFrame(
        [{"candidate_id": "cand_1", "event_type": "VOL_SHOCK", "status": "PROMOTED"}]
    )
    monkeypatch.setattr(
        svc,
        "promote_candidates",
        lambda **kwargs: (audit_df.copy(), promoted_df.copy(), {"promoted": 1}),
    )
    monkeypatch.setattr(svc, "build_promotion_statistical_audit", lambda **kwargs: audit_df.copy())
    monkeypatch.setattr(
        svc, "stabilize_promoted_output_schema", lambda promoted_df, audit_df: promoted_df.copy()
    )

    result = _run_promotion(tmp_path)
    assert result.exit_code == 0
    assert any((tmp_path / "promotions").glob("promotion_audit.*"))
    assert any((tmp_path / "promotions").glob("promoted_candidates.*"))
    assert (tmp_path / "promotions" / "evidence_bundles.jsonl").exists()
    assert (tmp_path / "live" / "theses" / "r1" / "promoted_theses.json").exists()
    assert "primary_reject_reason" in result.audit_df.columns
    assert "failed_gate_count" in result.audit_df.columns
    assert "decision_summary" in result.diagnostics
    assert result.diagnostics["live_thesis_export"]["thesis_count"] == 1
    assert result.diagnostics["live_thesis_export"]["contract_json_path"].endswith(
        "promoted_thesis_contracts.json"
    )
    assert result.diagnostics["live_thesis_export"]["contract_md_path"].endswith(
        "promoted_thesis_contracts.md"
    )


def test_run_promotion_service_hydrates_modern_edge_candidate_aliases(monkeypatch, tmp_path):
    monkeypatch.setattr(svc, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(validation_writer, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(
        svc,
        "load_run_manifest",
        lambda run_id: {"run_mode": "confirmatory", "discovery_profile": "standard"},
    )
    monkeypatch.setattr(
        svc,
        "resolve_objective_profile_contract",
        lambda **kwargs: SimpleNamespace(
            min_net_expectancy_bps=5.0,
            max_fee_plus_slippage_bps=10.0,
            max_daily_turnover_multiple=5.0,
            require_retail_viability=False,
            require_low_capital_contract=False,
        ),
    )
    monkeypatch.setattr(svc, "ontology_spec_hash", lambda root: "hash")
    monkeypatch.setattr(svc, "_load_gates_spec", lambda root: {"promotion_confirmatory_gates": {}})
    monkeypatch.setattr(svc, "_load_negative_control_summary", lambda run_id: {})
    monkeypatch.setattr(svc, "_load_dynamic_min_events_by_event", lambda run_id: {})

    cand_path = tmp_path / "reports" / "edge_candidates" / "r1"
    cand_path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "LIQUIDATION_CASCADE_PROXY",
                "event_family": "POSITIONING_EXTREMES",
                "n_events": 100,
                "stability_score": 0.8,
                "sign_consistency": 0.8,
                "q_value": 0.01,
                "after_cost_expectancy_per_trade": 0.0049,
                "stressed_after_cost_expectancy_per_trade": 0.0047,
                "gate_after_cost_positive": True,
                "gate_after_cost_stressed_positive": True,
                "confirmatory_locked": True,
                "frozen_spec_hash": "hash",
            }
        ]
    ).to_parquet(cand_path / "edge_candidates_normalized.parquet", index=False)
    burden_path = tmp_path / "reports" / "phase2" / "r1"
    burden_path.mkdir(parents=True, exist_ok=True)
    (burden_path / "search_burden_summary.json").write_text(
        json.dumps(
            {
                "search_burden_estimated": False,
                "search_candidates_generated": 10,
                "search_candidates_eligible": 5,
                "search_mutations_attempted": 0,
                "search_family_count": 4,
                "search_lineage_count": 5,
                "search_scope_version": "phase1_v1",
            }
        ),
        encoding="utf-8",
    )
    _write_validated_candidate_artifacts(tmp_path, "r1", "cand_1")

    captured: dict[str, pd.DataFrame] = {}

    def _promote_candidates(**kwargs):
        captured["candidates_df"] = kwargs["candidates_df"].copy()
        audit_df = pd.DataFrame(
            [
                {
                    "candidate_id": "cand_1",
                    "event_type": "LIQUIDATION_CASCADE_PROXY",
                    "promotion_decision": "promoted",
                    "promotion_track": "standard",
                    "promotion_metrics_trace": "{}",
                    "evidence_bundle_json": _valid_evidence_bundle_json(
                        run_id="r1",
                        candidate_id="cand_1",
                        event_type="LIQUIDATION_CASCADE_PROXY",
                    ),
                }
            ]
        )
        promoted_df = pd.DataFrame(
            [
                {
                    "candidate_id": "cand_1",
                    "event_type": "LIQUIDATION_CASCADE_PROXY",
                    "status": "PROMOTED",
                }
            ]
        )
        return audit_df, promoted_df, {"promoted": 1}

    monkeypatch.setattr(svc, "promote_candidates", _promote_candidates)
    monkeypatch.setattr(svc, "build_promotion_statistical_audit", lambda **kwargs: kwargs["audit_df"])
    monkeypatch.setattr(
        svc, "stabilize_promoted_output_schema", lambda promoted_df, audit_df: promoted_df.copy()
    )

    result = _run_promotion(tmp_path)

    assert result.exit_code == 0
    hydrated = captured["candidates_df"].iloc[0]
    assert hydrated["family"] == "POSITIONING_EXTREMES"
    assert hydrated["net_expectancy_bps"] == pytest.approx(47.0)
    assert hydrated["cost_survival_ratio"] == pytest.approx(1.0)
    assert bool(hydrated["search_burden_estimated"]) is False
    assert hydrated["search_candidates_generated"] == 10
    assert hydrated["search_candidates_eligible"] == 5
    assert svc._diagnose_missing_fields(captured["candidates_df"]) == []


def test_run_promotion_service_fails_closed_when_promoted_row_lacks_evidence_bundle(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(svc, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(
        svc,
        "load_run_manifest",
        lambda run_id: {"run_mode": "confirmatory", "discovery_profile": "standard"},
    )
    monkeypatch.setattr(
        svc,
        "resolve_objective_profile_contract",
        lambda **kwargs: SimpleNamespace(
            min_net_expectancy_bps=5.0,
            max_fee_plus_slippage_bps=10.0,
            max_daily_turnover_multiple=5.0,
            require_retail_viability=False,
            require_low_capital_contract=False,
        ),
    )
    monkeypatch.setattr(svc, "ontology_spec_hash", lambda root: "hash")
    monkeypatch.setattr(svc, "_load_gates_spec", lambda root: {"promotion_confirmatory_gates": {}})
    monkeypatch.setattr(svc, "_load_negative_control_summary", lambda run_id: {})
    monkeypatch.setattr(svc, "_load_dynamic_min_events_by_event", lambda run_id: {})

    cand_path = tmp_path / "reports" / "edge_candidates" / "r1"
    cand_path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "family": "VOL_SHOCK",
                "n_events": 100,
                "stability_score": 0.8,
                "sign_consistency": 0.9,
                "cost_survival_ratio": 1.0,
                "net_expectancy_bps": 6.0,
                "q_value": 0.01,
                "confirmatory_locked": True,
                "frozen_spec_hash": "hash",
            }
        ]
    ).to_csv(cand_path / "edge_candidates_normalized.csv", index=False)

    audit_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "promotion_decision": "promoted",
                "promotion_track": "standard",
                "promotion_metrics_trace": "{}",
                "evidence_bundle_json": "",
            }
        ]
    )
    promoted_df = pd.DataFrame(
        [{"candidate_id": "cand_1", "event_type": "VOL_SHOCK", "status": "PROMOTED"}]
    )
    monkeypatch.setattr(
        svc,
        "promote_candidates",
        lambda **kwargs: (audit_df.copy(), promoted_df.copy(), {"promoted": 1}),
    )
    monkeypatch.setattr(svc, "build_promotion_statistical_audit", lambda **kwargs: audit_df.copy())
    monkeypatch.setattr(
        svc, "stabilize_promoted_output_schema", lambda promoted_df, audit_df: promoted_df.copy()
    )

    result = _run_promotion(tmp_path)

    assert result.exit_code == 1


def test_run_promotion_service_treats_zero_validated_candidates_as_success(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(svc, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(validation_writer, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(
        svc,
        "load_run_manifest",
        lambda run_id: {"run_mode": "confirmatory", "discovery_profile": "standard"},
    )
    monkeypatch.setattr(
        svc,
        "resolve_objective_profile_contract",
        lambda **kwargs: SimpleNamespace(
            min_net_expectancy_bps=5.0,
            max_fee_plus_slippage_bps=10.0,
            max_daily_turnover_multiple=5.0,
            require_retail_viability=False,
            require_low_capital_contract=False,
        ),
    )

    run_id = "r1"
    validation_dir = tmp_path / "reports" / "validation" / run_id
    bundle = ValidationBundle(
        run_id=run_id,
        created_at="2026-01-01T00:00:00Z",
        validated_candidates=[],
        rejected_candidates=[],
        inconclusive_candidates=[],
        summary_stats={"total": 1, "validated": 0},
        effect_stability_report={},
    )
    write_validation_bundle(bundle, base_dir=validation_dir)
    write_promotion_ready_candidates(bundle, base_dir=validation_dir)

    result = _run_promotion(tmp_path)

    assert result.exit_code == 0
    assert result.audit_df.empty
    assert result.promoted_df.empty
    assert result.diagnostics["promotion_input_mode"] == "canonical_empty"
    assert result.diagnostics["decision_summary"]["candidates_total"] == 0
    assert result.diagnostics["decision_summary"]["promoted_count"] == 0
    assert result.diagnostics["live_thesis_export"]["thesis_count"] == 0
    assert (tmp_path / "promotions" / "promotion_diagnostics.json").exists()
    assert (tmp_path / "promotions" / "promoted_candidates.parquet").exists()
    assert (tmp_path / "promotions" / "evidence_bundles.jsonl").exists()
    assert (tmp_path / "promotions" / "evidence_bundles.jsonl").read_text(encoding="utf-8") == ""


def test_annotate_promotion_audit_decisions_derives_failed_stage_summary():
    audit_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "promotion_decision": "rejected",
                "promotion_fail_gate_primary": "gate_promo_stability",
                "promotion_fail_reason_primary": "",
                "reject_reason": "stability_score|negative_control_fail",
                "promotion_metrics_trace": json.dumps(
                    {
                        "statistical": {"passed": True},
                        "stability": {"passed": False},
                        "negative_control": {"passed": False},
                    }
                ),
            },
            {
                "candidate_id": "cand_2",
                "event_type": "VOL_SHOCK",
                "promotion_decision": "promoted",
                "promotion_fail_gate_primary": "",
                "promotion_fail_reason_primary": "",
                "reject_reason": "",
                "promotion_metrics_trace": json.dumps(
                    {
                        "statistical": {"passed": True},
                        "stability": {"passed": True},
                    }
                ),
            },
        ]
    )

    out = svc._annotate_promotion_audit_decisions(audit_df)
    row = out.loc[out["candidate_id"] == "cand_1"].iloc[0]

    assert row["primary_reject_reason"] == "stability_score"
    assert row["failed_gate_count"] == 2
    assert row["failed_gate_list"] == "stability|negative_control"
    assert row["weakest_fail_stage"] == "stability"
    assert row["rejection_classification"] == "scope_mismatch"
    assert row["recommended_next_action"] == "narrow_scope"

    diagnostics = svc._build_promotion_decision_diagnostics(out)
    assert diagnostics["rejected_count"] == 1
    assert diagnostics["primary_fail_gate_counts"]["gate_promo_stability"] == 1
    assert diagnostics["primary_reject_reason_counts"]["stability_score"] == 1
    assert diagnostics["failed_stage_counts"]["negative_control"] == 1
    assert diagnostics["rejection_classification_counts"]["scope_mismatch"] == 1
    assert diagnostics["recommended_next_action_counts"]["narrow_scope"] == 1
    assert diagnostics["confirmatory_field_availability"]["plan_row_id"]["missing"] == 2


def test_classify_rejection_maps_holdout_and_contract_failures():
    holdout = svc._classify_rejection(
        {
            "promotion_fail_gate_primary": "gate_promo_oos_validation",
            "reject_reason": "oos_validation_fail",
        },
        ["oos_validation"],
    )
    contract = svc._classify_rejection(
        {
            "promotion_fail_gate_primary": "gate_promo_contract",
            "reject_reason": "missing_hypothesis_audit",
        },
        [],
    )

    assert holdout == "weak_holdout_support"
    assert svc._recommended_next_action_for_rejection(holdout) == "run_confirmatory"
    assert contract == "contract_failure"
    assert svc._recommended_next_action_for_rejection(contract) == "repair_pipeline"


def test_classify_rejection_does_not_mark_missing_controls_as_contract_failure():
    classification = svc._classify_rejection(
        {
            "promotion_fail_gate_primary": "gate_promo_retail_net_expectancy",
            "reject_reason": "negative_control_missing|oos_insufficient_samples (val=0, test=0)|hypothesis_audit",
            "weakest_fail_stage": "bundle_policy",
        },
        [
            "bundle_policy",
            "negative_control",
            "oos_validation",
            "hypothesis_audit",
            "retail",
            "stability",
        ],
    )

    assert classification == "weak_holdout_support"
    assert svc._recommended_next_action_for_rejection(classification) == "run_confirmatory"


def test_load_negative_control_summary_returns_empty_dict_on_invalid_json(monkeypatch, tmp_path):
    monkeypatch.setattr(svc, "get_data_root", lambda: tmp_path)
    path = tmp_path / "reports" / "negative_control" / "r1"
    path.mkdir(parents=True, exist_ok=True)
    (path / "negative_control_summary.json").write_text("{bad json", encoding="utf-8")

    assert svc._load_negative_control_summary("r1") == {}


def test_load_hypothesis_index_merges_per_event_registries(monkeypatch, tmp_path):
    monkeypatch.setattr(svc, "get_data_root", lambda: tmp_path)
    phase2_root = tmp_path / "reports" / "phase2" / "r1"
    event_a = phase2_root / "OI_FLUSH" / "5m"
    event_b = phase2_root / "VOL_SHOCK" / "5m"
    event_a.mkdir(parents=True, exist_ok=True)
    event_b.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [{"hypothesis_id": "hyp_a", "event_type": "OI_FLUSH", "symbol_scope": "BTCUSDT"}]
    ).to_csv(event_a / "hypothesis_registry.csv", index=False)
    pd.DataFrame(
        [{"hypothesis_id": "hyp_b", "event_type": "VOL_SHOCK", "symbol_scope": "BTCUSDT"}]
    ).to_csv(event_b / "hypothesis_registry.csv", index=False)

    hypothesis_index = svc._load_hypothesis_index(run_id="r1", data_root=tmp_path)

    assert "hyp_a" in hypothesis_index
    assert "hyp_b" in hypothesis_index
    assert hypothesis_index["hyp_a"]["plan_row_id"] == "hyp_a"
    assert hypothesis_index["hyp_a"]["executed"] is True
    assert hypothesis_index["hyp_a"]["statuses"] == ["candidate_discovery"]


def test_run_promotion_backfills_plan_row_id_from_hypothesis_id(monkeypatch, tmp_path):
    monkeypatch.setattr(svc, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(validation_writer, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(
        svc,
        "load_run_manifest",
        lambda run_id: {"run_mode": "confirmatory", "discovery_profile": "standard"},
    )
    monkeypatch.setattr(
        svc,
        "resolve_objective_profile_contract",
        lambda **kwargs: SimpleNamespace(
            min_net_expectancy_bps=5.0,
            max_fee_plus_slippage_bps=10.0,
            max_daily_turnover_multiple=5.0,
            require_retail_viability=False,
            require_low_capital_contract=False,
        ),
    )
    monkeypatch.setattr(svc, "ontology_spec_hash", lambda root: "hash")
    monkeypatch.setattr(svc, "_load_gates_spec", lambda root: {"promotion_confirmatory_gates": {}})
    monkeypatch.setattr(svc, "_load_negative_control_summary", lambda run_id: {})
    monkeypatch.setattr(svc, "_load_dynamic_min_events_by_event", lambda run_id: {})

    cand_path = tmp_path / "reports" / "edge_candidates" / "r1"
    cand_path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "family": "VOL_SHOCK",
                "n_events": 100,
                "stability_score": 0.8,
                "sign_consistency": 0.9,
                "cost_survival_ratio": 1.0,
                "net_expectancy_bps": 6.0,
                "q_value": 0.01,
                "hypothesis_id": "hyp_1",
                "confirmatory_locked": True,
                "frozen_spec_hash": "hash",
            }
        ]
    ).to_parquet(cand_path / "edge_candidates_normalized.parquet", index=False)
    _write_validated_candidate_artifacts(tmp_path, "r1", "cand_1")

    registry_path = tmp_path / "reports" / "phase2" / "r1" / "VOL_SHOCK" / "5m"
    registry_path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"hypothesis_id": "hyp_1", "event_type": "VOL_SHOCK"}]).to_csv(
        registry_path / "hypothesis_registry.csv",
        index=False,
    )

    captured: dict[str, object] = {}

    def _promote_candidates(**kwargs):
        captured["candidates_df"] = kwargs["candidates_df"].copy()
        captured["hypothesis_index"] = dict(kwargs["hypothesis_index"])
        audit_df = pd.DataFrame(
            [
                {
                        "candidate_id": "cand_1",
                        "event_type": "VOL_SHOCK",
                        "promotion_decision": "promoted",
                        "promotion_track": "standard",
                        "promotion_metrics_trace": "{}",
                        "evidence_bundle_json": _valid_evidence_bundle_json(
                            run_id="r1", candidate_id="cand_1"
                        ),
                    }
                ]
            )
        promoted_df = pd.DataFrame(
            [{"candidate_id": "cand_1", "event_type": "VOL_SHOCK", "status": "PROMOTED"}]
        )
        return audit_df, promoted_df, {"promoted": 1}

    monkeypatch.setattr(svc, "promote_candidates", _promote_candidates)
    monkeypatch.setattr(svc, "build_promotion_statistical_audit", lambda **kwargs: kwargs["audit_df"])
    monkeypatch.setattr(
        svc, "stabilize_promoted_output_schema", lambda promoted_df, audit_df: promoted_df.copy()
    )

    result = _run_promotion(tmp_path, allow_missing_negative_controls=True)

    assert result.exit_code == 0
    captured_df = captured["candidates_df"]
    assert isinstance(captured_df, pd.DataFrame)
    assert captured_df.loc[0, "plan_row_id"] == "hyp_1"
    captured_index = captured["hypothesis_index"]
    assert isinstance(captured_index, dict)
    assert captured_index["hyp_1"]["executed"] is True
    assert captured_index["hyp_1"]["plan_row_id"] == "hyp_1"


def test_read_csv_or_parquet_does_not_swallow_unexpected_runtime_errors(monkeypatch, tmp_path):
    path = tmp_path / "edge_candidates_normalized.parquet"
    path.write_text("placeholder", encoding="utf-8")

    def _boom(_path):
        raise RuntimeError("parquet engine blew up")

    monkeypatch.setattr(pd, "read_parquet", _boom)

    with pytest.raises(RuntimeError, match="parquet engine blew up"):
        svc._read_csv_or_parquet(path)


def test_load_hypothesis_index_records_degraded_state_for_unreadable_registry(monkeypatch, tmp_path):
    run_id = "r1"
    phase2_root = tmp_path / "reports" / "phase2" / run_id
    phase2_root.mkdir(parents=True, exist_ok=True)
    path = phase2_root / "hypothesis_registry.csv"
    path.write_text("broken", encoding="utf-8")

    def _boom(_path):
        raise ValueError("bad hypothesis registry")

    monkeypatch.setattr(svc, "_read_csv_or_parquet", _boom)
    diagnostics: dict[str, object] = {}

    out = svc._load_hypothesis_index(run_id=run_id, data_root=tmp_path, diagnostics=diagnostics)

    assert out == {}
    degraded_states = diagnostics["degraded_states"]
    assert isinstance(degraded_states, list)
    assert degraded_states[0]["code"] == "hypothesis_registry_unreadable"
    assert "bad hypothesis registry" in degraded_states[0]["message"]


def test_confirmatory_run_missing_lock_column_fails_cleanly(monkeypatch, tmp_path):
    monkeypatch.setattr(svc, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(
        svc,
        "load_run_manifest",
        lambda run_id: {"run_mode": "confirmatory", "discovery_profile": "standard"},
    )
    monkeypatch.setattr(
        svc,
        "resolve_objective_profile_contract",
        lambda **kwargs: SimpleNamespace(
            min_net_expectancy_bps=5.0,
            max_fee_plus_slippage_bps=10.0,
            max_daily_turnover_multiple=5.0,
            require_retail_viability=False,
            require_low_capital_contract=False,
        ),
    )

    cand_path = tmp_path / "reports" / "edge_candidates" / "r1"
    cand_path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "q_value": 0.01,
                "frozen_spec_hash": "hash",
            }
        ]
    ).to_csv(cand_path / "edge_candidates_normalized.csv", index=False)

    result = _run_promotion(tmp_path)
    assert result.exit_code == 1
    assert result.audit_df.empty
    assert result.promoted_df.empty


def test_execute_promotion_records_degraded_state_when_manifest_persist_fails(monkeypatch, tmp_path):
    RunArtifactManifest = __import__(
        "project.research.validation.manifest",
        fromlist=["RunArtifactManifest"],
    ).RunArtifactManifest

    monkeypatch.setattr(svc, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(validation_writer, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(
        svc,
        "load_run_manifest",
        lambda run_id: {"run_mode": "confirmatory", "discovery_profile": "standard"},
    )
    monkeypatch.setattr(
        svc,
        "resolve_objective_profile_contract",
        lambda **kwargs: SimpleNamespace(
            min_net_expectancy_bps=5.0,
            max_fee_plus_slippage_bps=10.0,
            max_daily_turnover_multiple=5.0,
            require_retail_viability=False,
            require_low_capital_contract=False,
        ),
    )
    monkeypatch.setattr(svc, "ontology_spec_hash", lambda root: "hash")
    monkeypatch.setattr(svc, "_load_gates_spec", lambda root: {"promotion_confirmatory_gates": {}})
    monkeypatch.setattr(svc, "_load_negative_control_summary", lambda run_id: {})
    monkeypatch.setattr(svc, "_load_dynamic_min_events_by_event", lambda run_id: {})

    cand_path = tmp_path / "reports" / "edge_candidates" / "r1"
    cand_path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "family": "VOL_SHOCK",
                "n_events": 100,
                "stability_score": 0.8,
                "sign_consistency": 0.9,
                "cost_survival_ratio": 1.0,
                "net_expectancy_bps": 6.0,
                "q_value": 0.01,
                "confirmatory_locked": True,
                "frozen_spec_hash": "hash",
            }
        ]
    ).to_csv(cand_path / "edge_candidates_normalized.csv", index=False)
    _write_validated_candidate_artifacts(tmp_path, "r1", "cand_1")

    audit_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "promotion_decision": "promoted",
                "promotion_track": "standard",
                "promotion_metrics_trace": "{}",
                "evidence_bundle_json": _valid_evidence_bundle_json(
                    run_id="r1", candidate_id="cand_1"
                ),
            }
        ]
    )
    promoted_df = pd.DataFrame(
        [{"candidate_id": "cand_1", "event_type": "VOL_SHOCK", "status": "PROMOTED"}]
    )
    monkeypatch.setattr(
        svc,
        "promote_candidates",
        lambda **kwargs: (audit_df.copy(), promoted_df.copy(), {"promoted": 1}),
    )
    monkeypatch.setattr(svc, "build_promotion_statistical_audit", lambda **kwargs: audit_df.copy())
    monkeypatch.setattr(
        svc, "stabilize_promoted_output_schema", lambda promoted_df, audit_df: promoted_df.copy()
    )

    def _fail_persist(self, base_dir):
        raise OSError("disk full")

    monkeypatch.setattr(RunArtifactManifest, "persist", _fail_persist)

    result = _run_promotion(tmp_path)

    assert result.exit_code == 0
    degraded_states = result.diagnostics["degraded_states"]
    assert degraded_states[-1]["code"] == "artifact_manifest_persist_failed"
    assert "disk full" in degraded_states[-1]["message"]


def test_resolve_promotion_policy_research_relaxes_deploy_only_controls():
    contract = SimpleNamespace(
        min_trade_count=150,
        min_net_expectancy_bps=4.0,
        max_fee_plus_slippage_bps=10.0,
        max_daily_turnover_multiple=4.0,
        require_retail_viability=True,
        require_low_capital_contract=True,
    )
    config = svc.PromotionConfig(
        run_id="r1",
        symbols="",
        out_dir=None,
        max_q_value=0.10,
        min_events=20,
        min_stability_score=0.05,
        min_sign_consistency=0.67,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.01,
        min_tob_coverage=0.60,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=False,
        require_multiplicity_diagnostics=False,
        min_dsr=0.5,
        max_overlap_ratio=0.80,
        max_profile_correlation=0.90,
        allow_discovery_promotion=False,
        program_id="default_program",
        retail_profile="capital_constrained",
        objective_name="",
        objective_spec=None,
        retail_profiles_spec=None,
        promotion_profile="research",
    )

    policy = svc._resolve_promotion_policy(
        config=config,
        contract=contract,
        source_run_mode="production",
        project_root=svc.PROJECT_ROOT.parent,
    )

    assert policy.promotion_profile == "research"
    assert policy.base_min_events == 20
    assert policy.dynamic_min_events == {}
    assert policy.min_net_expectancy_bps == 1.5
    assert policy.require_retail_viability is False
    assert policy.require_low_capital_viability is False
    assert policy.enforce_baseline_beats_complexity is False
    assert policy.enforce_placebo_controls is False
    assert policy.enforce_timeframe_consensus is False


def test_resolve_promotion_policy_deploy_preserves_contract_and_dynamic_floors(monkeypatch):
    contract = SimpleNamespace(
        min_trade_count=150,
        min_net_expectancy_bps=4.0,
        max_fee_plus_slippage_bps=10.0,
        max_daily_turnover_multiple=4.0,
        require_retail_viability=True,
        require_low_capital_contract=True,
    )
    config = svc.PromotionConfig(
        run_id="r1",
        symbols="",
        out_dir=None,
        max_q_value=0.10,
        min_events=20,
        min_stability_score=0.05,
        min_sign_consistency=0.67,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.01,
        min_tob_coverage=0.60,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=False,
        require_multiplicity_diagnostics=False,
        min_dsr=0.5,
        max_overlap_ratio=0.80,
        max_profile_correlation=0.90,
        allow_discovery_promotion=False,
        program_id="default_program",
        retail_profile="capital_constrained",
        objective_name="",
        objective_spec=None,
        retail_profiles_spec=None,
        promotion_profile="deploy",
    )
    monkeypatch.setattr(svc, "_load_dynamic_min_events_by_event", lambda _root: {"VOL_SHOCK": 300})

    policy = svc._resolve_promotion_policy(
        config=config,
        contract=contract,
        source_run_mode="production",
        project_root=svc.PROJECT_ROOT.parent,
    )

    assert policy.promotion_profile == "deploy"
    assert policy.base_min_events == 150
    assert policy.dynamic_min_events == {"VOL_SHOCK": 300}
    assert policy.min_net_expectancy_bps == 4.0
    assert policy.require_retail_viability is True
    assert policy.require_low_capital_viability is True
    assert policy.enforce_baseline_beats_complexity is True
