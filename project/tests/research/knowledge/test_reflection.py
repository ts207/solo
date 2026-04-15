from __future__ import annotations

import json

import pandas as pd

from project.research.knowledge.reflection import REFLECTION_VERSION, build_run_reflection


def _write_run_manifest(base_dir, run_id: str, payload: dict) -> None:
    run_dir = base_dir / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_id": run_id,
        "program_id": "btc_campaign",
        "objective_name": "retail_profitability",
        "symbols": "BTCUSDT",
        "start": "2026-01-01",
        "end": "2026-01-31",
        "run_mode": "production",
        "status": "success",
    }
    manifest.update(payload)
    (run_dir / "run_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def test_build_run_reflection_separates_market_and_system_findings(tmp_path, monkeypatch):
    data_root = tmp_path
    run_id = "r1"
    run_dir = data_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    reports_root = data_root / "reports"
    (reports_root / "phase2" / run_id).mkdir(parents=True, exist_ok=True)
    (reports_root / "promotions" / run_id).mkdir(parents=True, exist_ok=True)

    _write_run_manifest(
        data_root,
        run_id,
        {
            "planned_stages": ["phase2_search_engine", "promote_candidates"],
            "status": "success",
        },
    )
    (reports_root / "phase2" / run_id / "discovery_quality_summary.json").write_text(
        json.dumps({"total_candidates": 3}),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "event_type": "BASIS_DISLOC",
                "promotion_decision": "rejected",
                "promotion_fail_gate_primary": "gate_promo_statistical",
                "gate_promo_statistical": False,
                "gate_bridge_tradable": True,
                "net_expectancy_bps": 2.0,
                "n_events": 24,
            }
        ]
    ).to_parquet(
        reports_root / "promotions" / run_id / "promotion_statistical_audit.parquet", index=False
    )
    (run_dir / "validate_feature_integrity_5m.json").write_text(
        json.dumps({"status": "warning", "symbols_with_issues": 1}),
        encoding="utf-8",
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))
    reflection = build_run_reflection(run_id=run_id)
    market = json.loads(reflection["market_findings"])
    system = json.loads(reflection["system_findings"])
    anomalies = json.loads(reflection["anomalies"])

    assert reflection["program_id"] == "btc_campaign"
    assert reflection["mechanical_outcome"] == "warning_only"
    assert reflection["statistical_outcome"] == "weak_signal"
    assert reflection["candidate_count"] == 1
    assert market["candidate_count"] == 1
    assert market["primary_fail_gate"] == "gate_promo_statistical"
    assert system["warning_stage_count"] >= 1
    assert any(item["type"] == "feature_integrity_warning" for item in anomalies)
    assert reflection["recommended_next_action"] == "explore_adjacent_region"
    assert reflection["reflection_version"] == REFLECTION_VERSION


def test_build_run_reflection_detects_stale_manifest_after_replayed_tail(tmp_path, monkeypatch):
    data_root = tmp_path
    run_id = "r2"
    run_dir = data_root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    reports_root = data_root / "reports"
    (reports_root / "promotions" / run_id).mkdir(parents=True, exist_ok=True)

    _write_run_manifest(
        data_root,
        run_id,
        {
            "status": "failed",
            "failed_stage": "promote_candidates",
            "planned_stages": ["promote_candidates"],
        },
    )
    (run_dir / "promote_candidates.json").write_text(
        json.dumps({"stage": "promote_candidates", "status": "success"}),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "event_type": "BASIS_DISLOC",
                "promotion_decision": "promoted",
                "promotion_fail_gate_primary": "",
                "gate_promo_statistical": True,
                "gate_bridge_tradable": True,
                "net_expectancy_bps": 4.0,
                "n_events": 140,
            }
        ]
    ).to_parquet(
        reports_root / "promotions" / run_id / "promotion_statistical_audit.parquet", index=False
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))
    reflection = build_run_reflection(run_id=run_id)
    anomalies = json.loads(reflection["anomalies"])

    assert reflection["mechanical_outcome"] == "mechanical_failure"
    assert reflection["statistical_outcome"] == "deploy_promising"
    assert reflection["promoted_count"] == 1
    assert any(item["type"] == "stale_run_manifest" for item in anomalies)
    assert any(item["type"] == "stage_status_mismatch" for item in anomalies)
    assert reflection["recommended_next_action"] == "repair_pipeline"


def test_build_run_reflection_marks_sample_limited_runs_as_inconclusive(tmp_path, monkeypatch):
    data_root = tmp_path
    run_id = "r3"
    reports_root = data_root / "reports"
    (reports_root / "edge_candidates" / run_id).mkdir(parents=True, exist_ok=True)

    _write_run_manifest(
        data_root,
        run_id,
        {
            "planned_stages": ["export_edge_candidates"],
            "status": "success",
        },
    )
    pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "event_type": "FND_DISLOC",
                "primary_fail_gate": "gate_promo_statistical",
                "gate_bridge_tradable": True,
                "after_cost_expectancy": 1.0,
                "sample_size": 12,
                "validation_samples": 7,
                "test_samples": 5,
            }
        ]
    ).to_parquet(
        reports_root / "edge_candidates" / run_id / "edge_candidates_normalized.parquet",
        index=False,
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))
    reflection = build_run_reflection(run_id=run_id)
    next_experiment = json.loads(reflection["recommended_next_experiment"])

    assert reflection["mechanical_outcome"] == "success"
    assert reflection["statistical_outcome"] == "inconclusive_due_to_sample"
    assert reflection["recommended_next_action"] == "rerun_same_scope"
    assert next_experiment["event_type"] == "FND_DISLOC"


def test_build_run_reflection_uses_final_phase2_candidates_not_discovery_total(tmp_path, monkeypatch):
    data_root = tmp_path
    run_id = "r4"
    reports_root = data_root / "reports"
    (reports_root / "phase2" / run_id).mkdir(parents=True, exist_ok=True)

    _write_run_manifest(
        data_root,
        run_id,
        {
            "planned_stages": ["phase2_search_engine"],
            "status": "success",
        },
    )
    (reports_root / "phase2" / run_id / "discovery_quality_summary.json").write_text(
        json.dumps({"total_candidates": 32, "phase2_candidates": 0}),
        encoding="utf-8",
    )
    pd.DataFrame([]).to_parquet(
        reports_root / "phase2" / run_id / "phase2_candidates.parquet",
        index=False,
    )

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))
    reflection = build_run_reflection(run_id=run_id)

    assert reflection["candidate_count"] == 0
    assert reflection["statistical_outcome"] == "no_signal"
    assert reflection["recommended_next_action"] == "hold"
