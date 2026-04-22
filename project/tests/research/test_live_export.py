from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from project.core.exceptions import DataIntegrityError
from project.core.exceptions import IncompleteLineageError
from project.core.exceptions import SchemaMismatchError
from project.live.deployment import check_thesis
from project.live.thesis_store import ThesisStore
from project.research.live_export import export_promoted_theses_for_run
from project.research.validation.contracts import (
    ValidationArtifactRef,
    ValidationBundle,
    ValidatedCandidateRecord,
    ValidationDecision,
    ValidationMetrics,
)
from project.research.validation.result_writer import (
    write_promotion_ready_candidates,
    write_validation_bundle,
)


def _bundle() -> dict:
    return {
        "candidate_id": "cand_1",
        "event_family": "VOL_SHOCK",
        "event_type": "VOL_SHOCK",
        "run_id": "run_1",
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
        "effect_estimates": {"estimate": 0.12, "estimate_bps": 12.0},
        "uncertainty_estimates": {"q_value": 0.01},
        "stability_tests": {"stability_score": 0.9},
        "falsification_results": {"passes_control": True},
        "cost_robustness": {
            "cost_survival_ratio": 1.0,
            "net_expectancy_bps": 9.0,
            "tob_coverage": 0.95,
            "retail_net_expectancy_pass": True,
        },
        "multiplicity_adjustment": {"q_value_program": 0.01},
        "metadata": {
            "hypothesis_id": "hyp_1",
            "plan_row_id": "plan_1",
            "has_realized_oos_path": True,
        },
        "promotion_decision": {
            "promotion_status": "promoted",
            "promotion_track": "deploy",
            "rank_score": 1.0,
        },
        "policy_version": "v1",
        "bundle_version": "b1",
    }


def _write_validation_lineage(
    root: Path,
    *,
    run_id: str,
    candidate_id: str,
    status: str = "validated",
) -> None:
    bundle = ValidationBundle(
        run_id=run_id,
        created_at="2026-01-01T00:00:00Z",
        validated_candidates=(
            [
                ValidatedCandidateRecord(
                    candidate_id=candidate_id,
                    decision=ValidationDecision(
                        status=status,
                        candidate_id=candidate_id,
                        run_id=run_id,
                        reason_codes=["passed_validation"] if status == "validated" else [],
                    ),
                    metrics=ValidationMetrics(sample_count=120, stability_score=0.9),
                )
            ]
            if status == "validated"
            else []
        ),
        rejected_candidates=[],
        inconclusive_candidates=[],
        summary_stats={"total": 1, "validated": 1 if status == "validated" else 0},
        effect_stability_report={},
    )
    base_dir = root / "reports" / "validation" / run_id
    write_validation_bundle(bundle, base_dir=base_dir)
    write_promotion_ready_candidates(bundle, base_dir=base_dir)


def _write_promotion_artifacts(
    root: Path,
    *,
    run_id: str,
    bundles: list[dict],
    promoted_rows: list[dict],
) -> None:
    promotion_dir = root / "reports" / "promotions" / run_id
    promotion_dir.mkdir(parents=True, exist_ok=True)
    (promotion_dir / "evidence_bundles.jsonl").write_text(
        "".join(json.dumps(bundle, sort_keys=True) + "\n" for bundle in bundles),
        encoding="utf-8",
    )
    pd.DataFrame(promoted_rows).to_parquet(
        promotion_dir / "promoted_candidates.parquet",
        index=False,
    )


def test_export_promoted_theses_pending_then_active_with_blueprint(tmp_path: Path) -> None:
    _write_validation_lineage(tmp_path, run_id="run_1", candidate_id="cand_1")
    promoted_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "status": "PROMOTED",
                "canonical_regime": "VOLATILITY",
                "routing_profile_id": "routing_v1",
            }
        ]
    )

    first = export_promoted_theses_for_run(
        "run_1",
        data_root=tmp_path,
        bundles=[_bundle()],
        promoted_df=promoted_df,
    )
    assert first.contract_json_path is not None
    assert first.contract_md_path is not None
    assert first.contract_json_path.exists()
    assert first.contract_md_path.exists()
    payload = json.loads(first.output_path.read_text(encoding="utf-8"))
    contract_payload = json.loads(first.contract_json_path.read_text(encoding="utf-8"))
    assert first.thesis_count == 1
    assert first.pending_count == 1
    assert payload["theses"][0]["status"] == "pending_blueprint"
    assert payload["theses"][0]["invalidation"] == {}
    assert contract_payload["contracts"][0]["thesis_id"] == "thesis::run_1::cand_1"
    assert contract_payload["contracts"][0]["authored_contract_linked"] is False
    assert contract_payload["contracts"][0]["primary_event_id"] == "VOL_SHOCK"
    assert contract_payload["contracts"][0]["compat_event_family"] == "VOL_SHOCK"
    index_payload = json.loads(first.index_path.read_text(encoding="utf-8"))
    assert index_payload["latest_run_id"] == "run_1"
    assert index_payload["default_resolution_disabled"] is True

    second = export_promoted_theses_for_run(
        "run_1",
        data_root=tmp_path,
        bundles=[_bundle()],
        promoted_df=promoted_df,
        blueprints=[
            {
                "id": "bp_1",
                "candidate_id": "cand_1",
                "direction": "long",
                "symbol_scope": {
                    "mode": "single_symbol",
                    "symbols": ["BTCUSDT"],
                    "candidate_symbol": "BTCUSDT",
                },
                "exit": {
                    "time_stop_bars": 8,
                    "stop_type": "range_pct",
                    "stop_value": 0.02,
                    "target_type": "range_pct",
                    "target_value": 0.03,
                    "invalidation": {
                        "metric": "adverse_proxy",
                        "operator": ">",
                        "value": 0.02,
                    },
                },
                "lineage": {"proposal_id": "proposal_1"},
            }
        ],
    )

    updated = json.loads(second.output_path.read_text(encoding="utf-8"))
    thesis = updated["theses"][0]
    assert second.active_count == 1
    assert thesis["status"] == "active"
    assert thesis["lineage"]["blueprint_id"] == "bp_1"
    assert thesis["invalidation"]["metric"] == "adverse_proxy"


def test_export_promoted_theses_fails_on_corrupted_existing_index(tmp_path: Path) -> None:
    _write_validation_lineage(tmp_path, run_id="run_1", candidate_id="cand_1")
    promoted_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "status": "PROMOTED",
            }
        ]
    )
    index_path = tmp_path / "live" / "theses" / "index.json"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text("{not-json", encoding="utf-8")

    with pytest.raises(DataIntegrityError):
        export_promoted_theses_for_run(
            "run_1",
            data_root=tmp_path,
            bundles=[_bundle()],
            promoted_df=promoted_df,
        )


def test_export_promoted_theses_uses_authored_thesis_definition_from_lineage(
    tmp_path: Path,
) -> None:
    _write_validation_lineage(tmp_path, run_id="run_1", candidate_id="cand_1")
    promoted_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK_LIQUIDITY_CONFIRM",
                "status": "PROMOTED",
                "canonical_regime": "VOLATILITY_TRANSITION",
            }
        ]
    )
    bundle = _bundle()
    bundle["event_type"] = "VOL_SHOCK_LIQUIDITY_CONFIRM"
    bundle["event_family"] = "VOL_SHOCK"
    bundle["metadata"] = {
        **bundle["metadata"],
        "hypothesis_id": "THESIS_VOL_SHOCK_LIQUIDITY_CONFIRM",
    }

    result = export_promoted_theses_for_run(
        "run_1",
        data_root=tmp_path,
        bundles=[bundle],
        promoted_df=promoted_df,
    )

    thesis = json.loads(result.output_path.read_text(encoding="utf-8"))["theses"][0]
    contract_payload = json.loads(result.contract_json_path.read_text(encoding="utf-8"))
    assert thesis["primary_event_id"] == "VOL_SHOCK"
    assert thesis["event_family"] == "VOL_SHOCK"
    assert thesis["requirements"]["trigger_events"] == ["VOL_SHOCK"]
    assert thesis["requirements"]["confirmation_events"] == ["LIQUIDITY_VACUUM"]
    assert thesis["requirements"]["sequence_mode"] == "event_plus_confirm"
    assert thesis["source"]["event_contract_ids"] == ["VOL_SHOCK", "LIQUIDITY_VACUUM"]
    assert (
        contract_payload["contracts"][0]["authored_contract_id"]
        == "THESIS_VOL_SHOCK_LIQUIDITY_CONFIRM"
    )
    assert contract_payload["contracts"][0]["authored_contract_linked"] is True


def test_export_promoted_theses_derives_multi_clause_requirements_from_metadata(
    tmp_path: Path,
) -> None:
    _write_validation_lineage(tmp_path, run_id="run_1", candidate_id="cand_structural")
    promoted_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_structural",
                "event_type": "STRUCTURAL_CONFIRM_PROXY",
                "status": "PROMOTED",
            }
        ]
    )
    bundle = _bundle()
    bundle["candidate_id"] = "cand_structural"
    bundle["event_type"] = "STRUCTURAL_CONFIRM_PROXY"
    bundle["event_family"] = "VOL_SHOCK"
    bundle["metadata"] = {
        **bundle["metadata"],
        "source_type": "event_plus_confirm",
        "event_contract_ids": ["VOL_SHOCK", "LIQUIDITY_VACUUM"],
        "episode_ids": ["EP_LIQUIDITY_SHOCK"],
    }

    result = export_promoted_theses_for_run(
        "run_1",
        data_root=tmp_path,
        bundles=[bundle],
        promoted_df=promoted_df,
    )

    thesis = json.loads(result.output_path.read_text(encoding="utf-8"))["theses"][0]
    contract_payload = json.loads(result.contract_json_path.read_text(encoding="utf-8"))
    assert thesis["primary_event_id"] == "VOL_SHOCK"
    assert thesis["requirements"]["trigger_events"] == ["VOL_SHOCK"]
    assert thesis["requirements"]["confirmation_events"] == ["LIQUIDITY_VACUUM"]
    assert thesis["requirements"]["required_episodes"] == ["EP_LIQUIDITY_SHOCK"]
    assert thesis["requirements"]["sequence_mode"] == "event_plus_confirm"
    assert thesis["source"]["event_contract_ids"] == ["VOL_SHOCK", "LIQUIDITY_VACUUM"]
    assert contract_payload["contracts"][0]["authored_contract_linked"] is False
    assert contract_payload["contracts"][0]["required_episodes"] == ["EP_LIQUIDITY_SHOCK"]


def test_export_promoted_theses_can_register_runtime_batch_and_override_deployment_state(
    tmp_path: Path,
) -> None:
    _write_validation_lineage(tmp_path, run_id="run_1", candidate_id="cand_1")
    promoted_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "status": "PROMOTED",
            }
        ]
    )

    with pytest.raises(RuntimeError, match="DeploymentGate blocked"):
        export_promoted_theses_for_run(
            "run_1",
            data_root=tmp_path,
            bundles=[_bundle()],
            promoted_df=promoted_df,
            deployment_state_overrides={"cand_1": "live_enabled"},
            register_runtime_name="paper_btc_runtime",
        )


def test_export_promoted_theses_preserves_live_eligible_for_deployment_gate(
    tmp_path: Path,
) -> None:
    _write_validation_lineage(tmp_path, run_id="run_1", candidate_id="cand_1")
    promoted_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "status": "PROMOTED",
                "deployment_state_default": "live_eligible",
            }
        ]
    )

    with pytest.raises(RuntimeError, match="DeploymentGate blocked"):
        export_promoted_theses_for_run(
            "run_1",
            data_root=tmp_path,
            bundles=[_bundle()],
            promoted_df=promoted_df,
        )


def test_export_promoted_theses_loads_validation_lineage_from_canonical_validation_root(
    tmp_path: Path,
) -> None:
    validation_dir = tmp_path / "reports" / "validation" / "run_1"
    bundle = ValidationBundle(
        run_id="run_1",
        created_at="2026-01-01T00:00:00Z",
        validated_candidates=[
            ValidatedCandidateRecord(
                candidate_id="cand_1",
                decision=ValidationDecision(
                    status="validated",
                    candidate_id="cand_1",
                    run_id="run_1",
                    reason_codes=["passed_validation"],
                ),
                metrics=ValidationMetrics(sample_count=120, stability_score=0.9),
                artifact_refs=[
                    ValidationArtifactRef(
                        artifact_type="validation_report",
                        path="reports/validation/run_1/validation_report.json",
                    )
                ],
            )
        ],
        summary_stats={"total": 1, "validated": 1},
    )
    write_validation_bundle(bundle, base_dir=validation_dir)
    write_promotion_ready_candidates(bundle, base_dir=validation_dir)

    promoted_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "status": "PROMOTED",
            }
        ]
    )

    result = export_promoted_theses_for_run(
        "run_1",
        data_root=tmp_path,
        bundles=[_bundle()],
        promoted_df=promoted_df,
    )

    thesis = json.loads(result.output_path.read_text(encoding="utf-8"))["theses"][0]
    assert thesis["lineage"]["validation_run_id"] == "run_1"
    assert thesis["lineage"]["validation_status"] == "validated"
    assert thesis["lineage"]["validation_reason_codes"] == ["passed_validation"]
    assert thesis["lineage"]["validation_artifact_paths"] == {
        "validation_report": "reports/validation/run_1/validation_report.json"
    }


def test_canonical_export_certifies_promotion_evidence_lineage_and_runtime_gate(
    tmp_path: Path,
) -> None:
    _write_validation_lineage(tmp_path, run_id="run_green", candidate_id="cand_1")
    green_bundle = _bundle()
    _write_promotion_artifacts(
        tmp_path,
        run_id="run_green",
        bundles=[green_bundle],
        promoted_rows=[{"candidate_id": "cand_1", "event_type": "VOL_SHOCK", "status": "PROMOTED"}],
    )

    result = export_promoted_theses_for_run("run_green", data_root=tmp_path)
    loaded = ThesisStore.from_run_id("run_green", data_root=tmp_path).all()

    assert result.thesis_count == 1
    assert loaded[0].lineage.validation_run_id == "run_green"
    assert loaded[0].lineage.candidate_id == "cand_1"
    assert loaded[0].deployment_state == "paper_only"
    assert check_thesis(loaded[0]) == []

    red_bundle = _bundle()
    red_bundle["candidate_id"] = "cand_evidence"
    _write_validation_lineage(tmp_path, run_id="run_red", candidate_id="cand_evidence")
    _write_promotion_artifacts(
        tmp_path,
        run_id="run_red",
        bundles=[red_bundle],
        promoted_rows=[
            {
                "candidate_id": "cand_promoted",
                "event_type": "VOL_SHOCK",
                "status": "PROMOTED",
            }
        ],
    )

    with pytest.raises(IncompleteLineageError, match="Promotion/evidence lineage mismatch"):
        export_promoted_theses_for_run("run_red", data_root=tmp_path)
    assert not (tmp_path / "live" / "theses" / "run_red" / "promoted_theses.json").exists()


def test_export_promoted_theses_fails_closed_on_malformed_validation_bundle(tmp_path: Path) -> None:
    validation_dir = tmp_path / "reports" / "validation" / "run_1"
    validation_dir.mkdir(parents=True, exist_ok=True)
    (validation_dir / "validation_bundle.json").write_text(
        json.dumps(
            {
                "run_id": "run_1",
                "created_at": "2026-01-01T00:00:00Z",
                "validated_candidates": [
                    {
                        "candidate_id": "cand_1",
                        "decision": {
                            "status": "maybe",
                            "candidate_id": "cand_1",
                            "run_id": "run_1",
                        },
                        "metrics": {"sample_count": 120},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    promoted_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "status": "PROMOTED",
            }
        ]
    )

    with pytest.raises((DataIntegrityError, SchemaMismatchError, ValueError)):
        export_promoted_theses_for_run(
            "run_1",
            data_root=tmp_path,
            bundles=[_bundle()],
            promoted_df=promoted_df,
        )


def test_export_promoted_theses_rejects_unknown_override_target(tmp_path: Path) -> None:
    _write_validation_lineage(tmp_path, run_id="run_1", candidate_id="cand_1")
    promoted_df = pd.DataFrame(
        [
            {
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "status": "PROMOTED",
            }
        ]
    )

    with pytest.raises(ValueError, match="did not match any exported thesis"):
        export_promoted_theses_for_run(
            "run_1",
            data_root=tmp_path,
            bundles=[_bundle()],
            promoted_df=promoted_df,
            deployment_state_overrides={"missing_selector": "live_enabled"},
        )


def test_export_promoted_theses_allows_zero_thesis_export_for_canonical_empty_validation(tmp_path: Path) -> None:
    _write_validation_lineage(tmp_path, run_id="run_empty", candidate_id="cand_0", status="rejected")

    result = export_promoted_theses_for_run("run_empty", data_root=tmp_path)
    payload = json.loads(result.output_path.read_text(encoding="utf-8"))

    assert result.thesis_count == 0
    assert result.active_count == 0
    assert result.pending_count == 0
    assert payload["theses"] == []
