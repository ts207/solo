from __future__ import annotations

import json
from pathlib import Path
from subprocess import CompletedProcess

import pandas as pd
import yaml

from project.research.services.regime_shakeout_service import (
    build_shakeout_audit,
    build_shakeout_proposal_payload,
    load_regime_shakeout_matrix,
    materialize_regime_shakeout_slices,
    run_regime_shakeout_matrix,
    summarize_shakeout_run_group,
    summarize_shakeout_run,
)
from project.research.services.run_comparison_service import research_diagnostics_paths


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _seed_run(data_root: Path, run_id: str, *, phase2_rows: list[dict], promoted_rows: list[dict]) -> None:
    phase2_path = data_root / "reports" / "phase2" / run_id / "phase2_candidates.parquet"
    phase2_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(phase2_rows).to_parquet(phase2_path)

    promotion_root = data_root / "reports" / "promotions" / run_id
    promotion_root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(promoted_rows).to_parquet(promotion_root / "promoted_candidates.parquet")
    pd.DataFrame(promoted_rows).to_parquet(promotion_root / "promotion_decisions.parquet")
    _write_json(
        promotion_root / "promotion_diagnostics.json",
        {
            "decision_summary": {
                "candidates_total": len(phase2_rows),
                "promoted_count": len(promoted_rows),
                "rejected_count": max(len(phase2_rows) - len(promoted_rows), 0),
            }
        },
    )
    diag_paths = research_diagnostics_paths(data_root=data_root, run_id=run_id)
    _write_json(
        diag_paths["phase2"],
        {
            "false_discovery_diagnostics": {
                "global": {
                    "candidates_total": len(phase2_rows),
                    "survivors_total": len(promoted_rows),
                }
            }
        },
    )
    _write_json(
        diag_paths["regime_effectiveness"],
        {
            "status": "ok",
            "regimes_total": 1,
            "episodes_total": len(phase2_rows),
            "scorecard_rows": 1,
            "recommended_bucket_counts": {"trade_generating": 1},
            "top_regimes_by_incidence": [
                {"canonical_regime": "LIQUIDITY_STRESS", "episode_count": len(phase2_rows)}
            ],
        },
    )
    edge_root = data_root / "reports" / "edge_candidates" / run_id
    edge_root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(phase2_rows).to_parquet(edge_root / "edge_candidates_normalized.parquet")


def test_materialize_regime_shakeout_slices_expands_pairs():
    matrix = load_regime_shakeout_matrix(
        Path("spec/benchmarks/regime_shakeout_matrix.yaml").resolve()
    )

    slices = materialize_regime_shakeout_slices(matrix)

    assert len(slices) > 24
    regime_slices = [row for row in slices if row.slice_type == "regime_first"]
    raw_slices = [row for row in slices if row.slice_type == "raw_control"]
    assert len(regime_slices) == 12
    assert len(raw_slices) > 12
    assert all(row.raw_control_events for row in raw_slices)
    assert all(len(row.raw_control_events) == 1 for row in raw_slices)
    assert all(row.baseline_event_type for row in raw_slices)


def test_build_shakeout_proposal_payload_switches_trigger_surface():
    matrix = load_regime_shakeout_matrix(
        Path("spec/benchmarks/regime_shakeout_matrix.yaml").resolve()
    )
    slices = materialize_regime_shakeout_slices(matrix)
    regime_slice = next(row for row in slices if row.slice_type == "regime_first")
    raw_slice = next(row for row in slices if row.slice_type == "raw_control")

    regime_payload = build_shakeout_proposal_payload(matrix=matrix, slice_def=regime_slice)
    raw_payload = build_shakeout_proposal_payload(matrix=matrix, slice_def=raw_slice)

    assert regime_payload["trigger_space"]["canonical_regimes"] == [regime_slice.canonical_regime]
    assert regime_payload["trigger_space"]["events"] == {}
    assert raw_payload["trigger_space"]["events"]["include"] == [raw_slice.baseline_event_type]
    assert raw_payload["trigger_space"]["canonical_regimes"] == []
    assert regime_payload["discovery_profile"] == "exploratory"
    assert regime_payload["phase2_gate_profile"] == "discovery"
    assert raw_payload["search_spec"] == "spec/search_space.yaml"


def test_summarize_shakeout_run_reports_contract_health(tmp_path):
    _seed_run(
        tmp_path,
        "run_a",
        phase2_rows=[
            {
                "candidate_id": "c1",
                "event_type": "LIQUIDITY_GAP_PRINT",
                "canonical_regime": "LIQUIDITY_STRESS",
                "subtype": "gap_print",
                "phase": "shock",
                "evidence_mode": "direct",
                "recommended_bucket": "trade_generating",
                "regime_bucket": "trade_generating",
                "routing_profile_id": "routing_v1",
                "after_cost_expectancy": 10.0,
                "q_value": 0.02,
            },
            {
                "candidate_id": "c2",
                "event_type": "LIQUIDITY_VACUUM",
                "canonical_regime": "LIQUIDITY_STRESS",
                "subtype": "vacuum",
                "phase": "shock",
                "evidence_mode": "proxy",
                "recommended_bucket": "trade_generating",
                "regime_bucket": "trade_generating",
                "routing_profile_id": "routing_v1",
                "after_cost_expectancy": 8.0,
                "q_value": 0.03,
            },
        ],
        promoted_rows=[
            {
                "candidate_id": "c1",
                "event_type": "LIQUIDITY_GAP_PRINT",
                "status": "promoted",
                "canonical_regime": "LIQUIDITY_STRESS",
                "subtype": "gap_print",
                "phase": "shock",
                "evidence_mode": "direct",
                "recommended_bucket": "trade_generating",
                "regime_bucket": "trade_generating",
                "routing_profile_id": "routing_v1",
                "after_cost_expectancy": 10.0,
            }
        ],
    )

    summary = summarize_shakeout_run(data_root=tmp_path, run_id="run_a")

    assert summary["candidate_surface"] == "edge_candidates"
    assert summary["candidate_count"] == 2
    assert summary["promoted_count"] == 1
    assert summary["raw_event_to_canonical_collapse_ratio"] == 2.0
    assert summary["bucket_agreement_rate"] == 1.0
    assert summary["contract_health"]["passed"] is True


def test_build_shakeout_audit_pairs_regime_and_raw_runs(tmp_path):
    matrix = {
        "matrix_id": "test_matrix",
        "defaults": {},
        "symbols": ["BTCUSDT"],
        "windows": [{"label": "stress", "start": "2025-01-01", "end": "2025-01-31"}],
        "regimes": [{"canonical_regime": "LIQUIDITY_STRESS", "raw_control_events": ["LIQUIDITY_GAP_PRINT"]}],
    }
    slices = materialize_regime_shakeout_slices(matrix)
    regime_run = next(row for row in slices if row.slice_type == "regime_first")
    raw_run = next(row for row in slices if row.slice_type == "raw_control")
    common_phase2 = [
        {
            "candidate_id": "c1",
            "event_type": "LIQUIDITY_GAP_PRINT",
            "canonical_regime": "LIQUIDITY_STRESS",
            "subtype": "gap_print",
            "phase": "shock",
            "evidence_mode": "direct",
            "recommended_bucket": "trade_generating",
            "regime_bucket": "trade_generating",
            "routing_profile_id": "routing_v1",
            "after_cost_expectancy": 10.0,
            "q_value": 0.02,
        }
    ]
    _seed_run(tmp_path, regime_run.run_id, phase2_rows=common_phase2, promoted_rows=common_phase2)
    _seed_run(tmp_path, raw_run.run_id, phase2_rows=common_phase2, promoted_rows=[])

    audit = build_shakeout_audit(matrix=matrix, slices=slices, data_root=tmp_path)

    assert audit["run_count"] == 2
    assert len(audit["pairs"]) == 1
    pair = audit["pairs"][0]
    assert pair["regime_run_id"] == regime_run.run_id
    assert pair["raw_control_run_ids"] == [raw_run.run_id]
    assert pair["delta"]["promoted_count"] == 1


def test_summarize_shakeout_run_tolerates_missing_promotion_artifacts(tmp_path):
    phase2_path = tmp_path / "reports" / "phase2" / "run_missing_promotions"
    phase2_path.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "event_type": "LIQUIDITY_GAP_PRINT",
                "canonical_regime": "LIQUIDITY_STRESS",
                "subtype": "gap_print",
                "phase": "shock",
                "evidence_mode": "direct",
                "recommended_bucket": "trade_generating",
                "regime_bucket": "trade_generating",
                "routing_profile_id": "routing_v1",
                "after_cost_expectancy": 10.0,
            }
        ]
    ).to_parquet(phase2_path / "phase2_candidates.parquet")

    summary = summarize_shakeout_run(data_root=tmp_path, run_id="run_missing_promotions")

    assert summary["candidate_surface"] == "phase2_search_engine"
    assert summary["candidate_count"] == 1
    assert summary["promoted_count"] == 0
    assert summary["promotion_decision_rows"] == 0


def test_summarize_shakeout_run_prefers_edge_candidate_surface(tmp_path):
    _seed_run(
        tmp_path,
        "run_edge_surface",
        phase2_rows=[
            {
                "candidate_id": "c1",
                "event_type": "STATE_CHOP_STATE",
                "after_cost_expectancy": 1.0,
                "q_value": 0.02,
            }
        ],
        promoted_rows=[],
    )
    edge_root = tmp_path / "reports" / "edge_candidates" / "run_edge_surface"
    pd.DataFrame(
        [
            {
                "candidate_id": "c2",
                "event_type": "LIQUIDITY_GAP_PRINT",
                "canonical_regime": "LIQUIDITY_STRESS",
                "subtype": "gap_print",
                "phase": "shock",
                "evidence_mode": "direct",
                "recommended_bucket": "trade_generating",
                "regime_bucket": "trade_generating",
                "routing_profile_id": "routing_v1",
                "after_cost_expectancy": 5.0,
                "q_value": 0.01,
            }
        ]
    ).to_parquet(edge_root / "edge_candidates_normalized.parquet")

    summary = summarize_shakeout_run(data_root=tmp_path, run_id="run_edge_surface")

    assert summary["candidate_surface"] == "edge_candidates"
    assert summary["candidate_count"] == 1
    assert summary["unique_canonical_regimes_represented"] == 1


def test_summarize_shakeout_run_group_aggregates_raw_controls(tmp_path):
    common = {
        "canonical_regime": "BASIS_FUNDING_DISLOCATION",
        "recommended_bucket": "trade_generating",
        "regime_bucket": "trade_generating",
        "routing_profile_id": "routing_v1",
    }
    _seed_run(
        tmp_path,
        "raw_a",
        phase2_rows=[
            {
                "candidate_id": "a1",
                "event_type": "BASIS_DISLOC",
                "subtype": "basis_dislocation",
                "phase": "shock",
                "evidence_mode": "statistical",
                **common,
            }
        ],
        promoted_rows=[],
    )
    _seed_run(
        tmp_path,
        "raw_b",
        phase2_rows=[
            {
                "candidate_id": "b1",
                "event_type": "FND_DISLOC",
                "subtype": "funding_dislocation",
                "phase": "shock",
                "evidence_mode": "direct",
                **common,
            }
        ],
        promoted_rows=[],
    )

    summary = summarize_shakeout_run_group(data_root=tmp_path, run_ids=["raw_a", "raw_b"])

    assert summary["candidate_surface"] == "edge_candidates"
    assert summary["candidate_count"] == 2
    assert summary["unique_raw_events_represented"] == 2
    assert summary["unique_canonical_regimes_represented"] == 1
    assert summary["contract_health"]["passed"] is True


def test_summarize_shakeout_run_empty_candidate_surface_is_not_contract_failure(tmp_path):
    edge_root = tmp_path / "reports" / "edge_candidates" / "run_empty"
    edge_root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame().to_parquet(edge_root / "edge_candidates_normalized.parquet")

    summary = summarize_shakeout_run(data_root=tmp_path, run_id="run_empty")

    assert summary["candidate_surface"] == "edge_candidates"
    assert summary["candidate_count"] == 0
    assert summary["unknown_regime_rate"] == 0.0
    assert summary["contract_health"]["passed"] is True


def test_run_regime_shakeout_matrix_reuses_successful_rows(tmp_path, monkeypatch):
    matrix_path = tmp_path / "matrix.yaml"
    matrix_path.write_text(
        yaml.safe_dump(
            {
                "matrix_id": "mini_matrix",
                "symbols": ["BTCUSDT"],
                "windows": [{"label": "late", "start": "2021-12-01", "end": "2021-12-31"}],
                "defaults": {},
                "regimes": [
                    {
                        "canonical_regime": "CROSS_ASSET_DESYNCHRONIZATION",
                        "raw_control_events": ["CROSS_ASSET_DESYNC_EVENT"],
                    }
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    prior_manifest = {
        "results": [
            {
                "run_id": "shakeout_cross_asset_desynchronization_btcusdt_late_regime",
                "status": "success",
                "returncode": 0,
            }
        ]
    }
    (out_dir / "regime_shakeout_manifest.json").write_text(
        json.dumps(prior_manifest), encoding="utf-8"
    )

    calls: list[list[str]] = []

    def _fake_execute_command(*, command, data_root, check):
        calls.append(list(command))
        return CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(
        "project.research.services.regime_shakeout_service._execute_command",
        _fake_execute_command,
    )

    result = run_regime_shakeout_matrix(
        matrix_path=matrix_path,
        out_dir=out_dir,
        registry_root=(Path("project/configs/registries").resolve()),
        data_root=tmp_path / "data",
        execute=True,
        plan_only=True,
        dry_run=False,
        check=False,
    )

    manifest = json.loads((out_dir / "regime_shakeout_manifest.json").read_text(encoding="utf-8"))
    reused = next(row for row in manifest["results"] if row["run_id"].endswith("_regime"))
    rerun = next(row for row in manifest["results"] if "_raw_" in row["run_id"])

    assert result["planned_runs"] == 2
    assert reused["reused_result"] is True
    assert rerun["status"] == "success"
    assert len(calls) == 1
