from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from project.research.services import promotion_service as svc
from project.research.services import evaluation_service
from project.research.validation import result_writer

PromotionConfig = svc.PromotionConfig


def _patch_canonical_validation_inputs(
    monkeypatch,
    config: svc.PromotionConfig,
    candidates_df: pd.DataFrame,
    candidate_ids: list[str],
) -> None:
    canonical_dir = config.resolved_out_dir().parent.parent / "validation" / config.run_id
    canonical_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"candidate_id": candidate_ids}).to_csv(
        canonical_dir / "promotion_ready_candidates.csv",
        index=False,
    )
    monkeypatch.setattr(
        result_writer,
        "load_validation_bundle",
        lambda *args, **kwargs: SimpleNamespace(
            validated_candidates=[
                SimpleNamespace(
                    candidate_id=candidate_id,
                    decision=SimpleNamespace(status="promoted", reason_codes=[]),
                    artifact_refs=[],
                )
                for candidate_id in candidate_ids
            ],
            rejected_candidates=[],
            inconclusive_candidates=[],
            run_id="test_run",
        ),
    )
    monkeypatch.setattr(
        evaluation_service.ValidationService,
        "load_candidate_tables",
        lambda self, run_id: {
            "edge_candidates": candidates_df.copy(),
            "promotion_audit": pd.DataFrame(),
            "phase2_candidates": pd.DataFrame(),
        },
    )


def test_promotion_rejection_classification_and_annotations() -> None:
    audit_df = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "promotion_fail_reason_primary": "spec hash mismatch",
                "promotion_fail_gate_primary": "contract_gate",
                "reject_reason": "",
                "promotion_metrics_trace": json.dumps({"contract": {"passed": False}}),
            },
            {
                "candidate_id": "c2",
                "promotion_fail_reason_primary": "low expectancy after_cost",
                "promotion_fail_gate_primary": "economics_gate",
                "reject_reason": "turnover",
                "promotion_metrics_trace": json.dumps(
                    {"economics": {"passed": False}, "stability": {"passed": False}}
                ),
            },
        ]
    )
    annotated = svc._annotate_promotion_audit_decisions(audit_df)
    assert list(annotated["rejection_classification"]) == ["contract_failure", "weak_economics"]
    assert list(annotated["recommended_next_action"]) == ["repair_pipeline", "stop_or_reframe"]
    assert annotated.loc[0, "failed_gate_count"] == 1
    assert annotated.loc[1, "failed_gate_count"] == 2

    diagnostics = svc._build_promotion_decision_diagnostics(
        annotated.assign(promotion_decision=["rejected", "rejected"])
    )
    assert diagnostics["candidates_total"] == 2
    assert diagnostics["rejected_count"] == 2
    assert diagnostics["primary_fail_gate_counts"]["contract_gate"] == 1
    assert diagnostics["failed_stage_counts"]["economics"] == 1


def test_resolve_promotion_policy_switches_by_profile(monkeypatch, tmp_path: Path) -> None:
    contract = SimpleNamespace(
        min_net_expectancy_bps=3.0,
        max_fee_plus_slippage_bps=7.0,
        max_daily_turnover_multiple=2.0,
        require_retail_viability=True,
        require_low_capital_contract=True,
        min_trade_count=20,
    )
    base_config = PromotionConfig(
        run_id="test_run",
        symbols="BTC",
        out_dir=tmp_path,
        max_q_value=0.10,
        min_events=100,
        min_stability_score=0.05,
        min_sign_consistency=0.67,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.01,
        min_tob_coverage=0.60,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=True,
        require_multiplicity_diagnostics=False,
        min_dsr=0.5,
        max_overlap_ratio=0.8,
        max_profile_correlation=0.9,
        allow_discovery_promotion=False,
        program_id="test_program",
        retail_profile="capital_constrained",
        objective_name="retail_profitability",
        objective_spec=None,
        retail_profiles_spec=None,
        promotion_profile="auto",
    )

    research = svc._resolve_promotion_policy(
        config=base_config,
        contract=contract,
        source_run_mode="research",
        project_root=tmp_path,
    )
    assert research.promotion_profile == "research"
    assert research.base_min_events == base_config.min_events
    assert research.dynamic_min_events == {}
    assert research.min_net_expectancy_bps == 1.5
    assert research.require_retail_viability is False

    monkeypatch.setattr(svc, "_load_dynamic_min_events_by_event", lambda root: {"BASIS_DISLOC": 25})
    deploy = svc._resolve_promotion_policy(
        config=base_config,
        contract=contract,
        source_run_mode="production",
        project_root=tmp_path,
    )
    assert deploy.promotion_profile == "deploy"
    assert deploy.base_min_events == max(base_config.min_events, contract.min_trade_count, 150)
    assert deploy.dynamic_min_events["BASIS_DISLOC"] == 25
    assert deploy.require_retail_viability is True


def test_execute_promotion_success_path(monkeypatch, tmp_path: Path) -> None:
    candidates_df = pd.DataFrame(
        {
            "candidate_id": ["cand-1"],
            "event_type": ["BASIS_DISLOC"],
            "confirmatory_locked": [True],
            "frozen_spec_hash": ["spec-hash"],
            "symbol": ["BTCUSDT"],
            "family": ["BASIS_FUNDING_DISLOCATION"],
            "net_expectancy_bps": [9.0],
            "stability_score": [0.9],
            "sign_consistency": [1.0],
            "cost_survival_ratio": [1.0],
            "q_value": [0.01],
            "n_events": [120],
        }
    )
    audit_df = pd.DataFrame(
        {
            "candidate_id": ["cand-1"],
            "event_type": ["BASIS_DISLOC"],
            "promotion_decision": ["promoted"],
            "promotion_track": ["deploy"],
            "rank_score": [1.0],
            "rejection_reasons": [""],
            "policy_version": ["v1"],
            "bundle_version": ["1"],
            "is_reduced_evidence": [False],
            "promotion_metrics_trace": [
                json.dumps(
                    {"economics": {"passed": True, "observed": {"x": 1}, "thresholds": {"y": 2}}}
                )
            ],
            "evidence_bundle_json": [
                json.dumps(
                    {
                        "candidate_id": "cand-1",
                        "event_family": "BASIS_DISLOC",
                        "event_type": "BASIS_DISLOC",
                        "run_id": "run-1",
                        "sample_definition": {
                            "n_events": 120,
                            "validation_samples": 60,
                            "test_samples": 60,
                            "symbol": "BTCUSDT",
                        },
                        "split_definition": {
                            "split_scheme_id": "confirmatory",
                            "purge_bars": 1,
                            "embargo_bars": 1,
                            "bar_duration_minutes": 5,
                        },
                        "effect_estimates": {
                            "estimate": 0.12,
                            "estimate_bps": 12.0,
                            "stderr": 0.03,
                            "stderr_bps": 3.0,
                        },
                        "uncertainty_estimates": {
                            "ci_low": 0.02,
                            "ci_high": 0.22,
                            "ci_low_bps": 2.0,
                            "ci_high_bps": 22.0,
                            "p_value_raw": 0.01,
                            "q_value": 0.01,
                            "q_value_by": 0.01,
                            "q_value_cluster": 0.01,
                            "n_obs": 120,
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
                            "net_expectancy_bps": 9.0,
                            "effective_cost_bps": 3.0,
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
                        },
                        "metadata": {"plan_row_id": "plan-1", "hypothesis_id": "hyp-1"},
                        "promotion_decision": {
                            "promotion_status": "promoted",
                            "promotion_track": "deploy",
                            "eligible": True,
                            "rank_score": 1.0,
                        },
                    }
                )
            ],
        }
    )
    promoted_df = pd.DataFrame({"candidate_id": ["cand-1"], "event_type": ["BASIS_DISLOC"]})
    writes = {}

    monkeypatch.setattr(
        svc,
        "load_run_manifest",
        lambda run_id: {
            "run_mode": "production",
            "discovery_profile": "standard",
            "confirmatory_rerun_run_id": "rerun-1",
            "candidate_origin_run_id": "origin-1",
            "program_id": "prog-1",
            "symbols": "BTCUSDT",
        },
    )
    monkeypatch.setattr(
        svc,
        "resolve_objective_profile_contract",
        lambda **kwargs: SimpleNamespace(
            min_net_expectancy_bps=3.0,
            max_fee_plus_slippage_bps=7.0,
            max_daily_turnover_multiple=2.0,
            require_retail_viability=True,
            require_low_capital_contract=True,
            min_trade_count=10,
        ),
    )
    monkeypatch.setattr(svc, "ontology_spec_hash", lambda root: "spec-hash")
    monkeypatch.setattr(svc, "_load_gates_spec", lambda root: {})
    monkeypatch.setattr(svc, "_load_hypothesis_index", lambda **kwargs: {})
    monkeypatch.setattr(svc, "_load_negative_control_summary", lambda run_id: {})
    monkeypatch.setattr(
        svc, "_hydrate_edge_candidates_from_phase2", lambda **kwargs: candidates_df.copy()
    )
    monkeypatch.setattr(
        svc,
        "promote_candidates",
        lambda **kwargs: (audit_df.copy(), promoted_df.copy(), {"seed": 1}),
    )
    monkeypatch.setattr(svc, "build_promotion_statistical_audit", lambda **kwargs: audit_df.copy())
    monkeypatch.setattr(
        svc, "stabilize_promoted_output_schema", lambda **kwargs: promoted_df.copy()
    )
    monkeypatch.setattr(
        svc,
        "serialize_evidence_bundles",
        lambda bundles, path: writes.setdefault("bundles", list(bundles)),
    )
    monkeypatch.setattr(svc, "bundle_to_flat_record", lambda bundle: dict(bundle))
    monkeypatch.setattr(svc, "write_promotion_reports", lambda **kwargs: writes.update(kwargs))
    monkeypatch.setattr(svc, "start_manifest", lambda *args, **kwargs: {"status": "started"})
    monkeypatch.setattr(
        svc,
        "finalize_manifest",
        lambda manifest, status, **kwargs: manifest.update({"status": status, **kwargs}),
    )

    config = PromotionConfig(
        run_id="test_run",
        symbols="BTC",
        out_dir=tmp_path,
        max_q_value=0.10,
        min_events=100,
        min_stability_score=0.05,
        min_sign_consistency=0.67,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.01,
        min_tob_coverage=0.60,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=True,
        require_multiplicity_diagnostics=False,
        min_dsr=0.5,
        max_overlap_ratio=0.8,
        max_profile_correlation=0.9,
        allow_discovery_promotion=False,
        program_id="test_program",
        retail_profile="capital_constrained",
        objective_name="retail_profitability",
        objective_spec=None,
        retail_profiles_spec=None,
        promotion_profile="auto",
    )

    _patch_canonical_validation_inputs(monkeypatch, config, candidates_df, ["cand-1"])

    result = svc.execute_promotion(config)
    assert result.exit_code == 0
    assert result.diagnostics["decision_summary"]["promoted_count"] == 1
    assert list(result.audit_df["rejection_classification"]) == ["unclassified"]
    assert writes["evidence_bundle_summary"].shape[0] == 1


def test_execute_promotion_allows_research_run_mode(monkeypatch, tmp_path: Path) -> None:
    candidates_df = pd.DataFrame(
        {
            "candidate_id": ["cand-1"],
            "event_type": ["BASIS_DISLOC"],
            "confirmatory_locked": [False],
            "frozen_spec_hash": [pd.NA],
            "symbol": ["BTCUSDT"],
            "family": ["BASIS_FUNDING_DISLOCATION"],
            "net_expectancy_bps": [9.0],
            "stability_score": [0.9],
            "sign_consistency": [1.0],
            "cost_survival_ratio": [1.0],
            "q_value": [0.01],
            "n_events": [120],
        }
    )
    audit_df = pd.DataFrame(
        {
            "candidate_id": ["cand-1"],
            "event_type": ["BASIS_DISLOC"],
            "promotion_decision": ["rejected"],
            "promotion_track": ["fallback_only"],
            "rank_score": [0.0],
            "rejection_reasons": ["min_events"],
            "policy_version": ["v1"],
            "bundle_version": ["1"],
            "is_reduced_evidence": [False],
            "promotion_metrics_trace": [json.dumps({"economics": {"passed": False}})],
            "evidence_bundle_json": [
                json.dumps({"candidate_id": "cand-1", "event_type": "BASIS_DISLOC"})
            ],
        }
    )
    promoted_df = pd.DataFrame(columns=["candidate_id", "event_type"])
    writes = {}

    monkeypatch.setattr(
        svc,
        "load_run_manifest",
        lambda run_id: {
            "run_mode": "research",
            "discovery_profile": "standard",
            "program_id": "prog-1",
            "symbols": "BTCUSDT",
        },
    )
    monkeypatch.setattr(
        svc,
        "resolve_objective_profile_contract",
        lambda **kwargs: SimpleNamespace(
            min_net_expectancy_bps=3.0,
            max_fee_plus_slippage_bps=7.0,
            max_daily_turnover_multiple=2.0,
            require_retail_viability=True,
            require_low_capital_contract=True,
            min_trade_count=10,
        ),
    )
    monkeypatch.setattr(svc, "ontology_spec_hash", lambda root: "spec-hash")
    monkeypatch.setattr(svc, "_load_gates_spec", lambda root: {})
    monkeypatch.setattr(svc, "_load_hypothesis_index", lambda **kwargs: {})
    monkeypatch.setattr(svc, "_load_negative_control_summary", lambda run_id: {})
    monkeypatch.setattr(
        svc, "_hydrate_edge_candidates_from_phase2", lambda **kwargs: candidates_df.copy()
    )
    monkeypatch.setattr(
        svc,
        "promote_candidates",
        lambda **kwargs: (audit_df.copy(), promoted_df.copy(), {"seed": 1}),
    )
    monkeypatch.setattr(svc, "build_promotion_statistical_audit", lambda **kwargs: audit_df.copy())
    monkeypatch.setattr(
        svc, "stabilize_promoted_output_schema", lambda **kwargs: promoted_df.copy()
    )
    monkeypatch.setattr(
        svc,
        "serialize_evidence_bundles",
        lambda bundles, path: writes.setdefault("bundles", list(bundles)),
    )
    monkeypatch.setattr(svc, "bundle_to_flat_record", lambda bundle: dict(bundle))
    monkeypatch.setattr(svc, "write_promotion_reports", lambda **kwargs: writes.update(kwargs))
    monkeypatch.setattr(svc, "start_manifest", lambda *args, **kwargs: {"status": "started"})
    monkeypatch.setattr(
        svc,
        "finalize_manifest",
        lambda manifest, status, **kwargs: manifest.update({"status": status, **kwargs}),
    )

    config = PromotionConfig(
        run_id="test_run",
        symbols="BTC",
        out_dir=tmp_path,
        max_q_value=0.10,
        min_events=100,
        min_stability_score=0.05,
        min_sign_consistency=0.67,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.01,
        min_tob_coverage=0.60,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=True,
        require_multiplicity_diagnostics=False,
        min_dsr=0.5,
        max_overlap_ratio=0.8,
        max_profile_correlation=0.9,
        allow_discovery_promotion=False,
        program_id="test_program",
        retail_profile="capital_constrained",
        objective_name="retail_profitability",
        objective_spec=None,
        retail_profiles_spec=None,
        promotion_profile="auto",
    )

    _patch_canonical_validation_inputs(monkeypatch, config, candidates_df, ["cand-1"])

    result = svc.execute_promotion(config)

    assert result.exit_code == 0
    assert result.diagnostics["promotion_profile"] == "research"
    assert set(writes["audit_df"]["source_run_mode"]) == {"research"}


def test_execute_promotion_normalizes_empty_bundle_outputs(monkeypatch, tmp_path: Path) -> None:
    candidates_df = pd.DataFrame(
        {
            "candidate_id": ["cand-1"],
            "event_type": ["BASIS_DISLOC"],
            "symbol": ["BTCUSDT"],
            "family": ["BASIS_FUNDING_DISLOCATION"],
            "net_expectancy_bps": [9.0],
            "stability_score": [0.9],
            "sign_consistency": [1.0],
            "cost_survival_ratio": [1.0],
            "q_value": [0.01],
            "n_events": [120],
        }
    )
    audit_df = pd.DataFrame()
    promoted_df = pd.DataFrame(columns=["candidate_id", "event_type"])
    writes = {}

    monkeypatch.setattr(
        svc,
        "load_run_manifest",
        lambda run_id: {
            "run_mode": "research",
            "discovery_profile": "standard",
            "program_id": "prog-1",
            "symbols": "BTCUSDT",
        },
    )
    monkeypatch.setattr(
        svc,
        "resolve_objective_profile_contract",
        lambda **kwargs: SimpleNamespace(
            min_net_expectancy_bps=3.0,
            max_fee_plus_slippage_bps=7.0,
            max_daily_turnover_multiple=2.0,
            require_retail_viability=True,
            require_low_capital_contract=True,
            min_trade_count=10,
        ),
    )
    monkeypatch.setattr(svc, "ontology_spec_hash", lambda root: "spec-hash")
    monkeypatch.setattr(svc, "_load_gates_spec", lambda root: {})
    monkeypatch.setattr(svc, "_load_hypothesis_index", lambda **kwargs: {})
    monkeypatch.setattr(svc, "_load_negative_control_summary", lambda run_id: {})
    monkeypatch.setattr(
        svc, "_hydrate_edge_candidates_from_phase2", lambda **kwargs: candidates_df.copy()
    )
    monkeypatch.setattr(
        svc,
        "promote_candidates",
        lambda **kwargs: (audit_df.copy(), promoted_df.copy(), {"seed": 1}),
    )
    monkeypatch.setattr(svc, "build_promotion_statistical_audit", lambda **kwargs: audit_df.copy())
    monkeypatch.setattr(
        svc, "stabilize_promoted_output_schema", lambda **kwargs: promoted_df.copy()
    )
    monkeypatch.setattr(
        svc,
        "serialize_evidence_bundles",
        lambda bundles, path: writes.setdefault("bundles", list(bundles)),
    )
    monkeypatch.setattr(svc, "write_promotion_reports", lambda **kwargs: writes.update(kwargs))
    monkeypatch.setattr(svc, "start_manifest", lambda *args, **kwargs: {"status": "started"})
    monkeypatch.setattr(
        svc,
        "finalize_manifest",
        lambda manifest, status, **kwargs: manifest.update({"status": status, **kwargs}),
    )

    config = PromotionConfig(
        run_id="test_run",
        symbols="BTC",
        out_dir=tmp_path,
        max_q_value=0.10,
        min_events=100,
        min_stability_score=0.05,
        min_sign_consistency=0.67,
        min_cost_survival_ratio=0.75,
        max_negative_control_pass_rate=0.01,
        min_tob_coverage=0.60,
        require_hypothesis_audit=True,
        allow_missing_negative_controls=True,
        require_multiplicity_diagnostics=False,
        min_dsr=0.5,
        max_overlap_ratio=0.8,
        max_profile_correlation=0.9,
        allow_discovery_promotion=False,
        program_id="test_program",
        retail_profile="capital_constrained",
        objective_name="retail_profitability",
        objective_spec=None,
        retail_profiles_spec=None,
        promotion_profile="auto",
    )

    _patch_canonical_validation_inputs(monkeypatch, config, candidates_df, ["cand-1"])

    result = svc.execute_promotion(config)

    assert result.exit_code == 0
    assert writes["bundles"] == []
    assert writes["evidence_bundle_summary"].empty
    assert writes["promotion_decisions"].empty
    assert set(writes["evidence_bundle_summary"].columns) >= {
        "candidate_id",
        "event_type",
        "promotion_decision",
        "promotion_track",
        "policy_version",
        "bundle_version",
        "is_reduced_evidence",
    }
    assert set(writes["promotion_decisions"].columns) >= {
        "candidate_id",
        "event_type",
        "promotion_decision",
        "promotion_track",
        "policy_version",
        "bundle_version",
        "is_reduced_evidence",
    }


def test_trace_helpers_and_scope_classification() -> None:
    assert svc._trace_payload('{"a": {"passed": false}}') == {"a": {"passed": False}}
    assert svc._failed_stages_from_trace({"x": {"passed": False}, "y": {"passed": True}}) == ["x"]
    row = {"reject_reason": "placebo | overlap", "weakest_fail_stage": ""}
    assert svc._classify_rejection(row, []) == "scope_mismatch"
    assert svc._recommended_next_action_for_rejection("scope_mismatch") == "narrow_scope"
