from __future__ import annotations

import json
import logging
from types import SimpleNamespace

import pandas as pd

import project.research.generate_recommendations_checklist as checklist


class _Args:
    min_edge_candidates = 1
    min_promoted_candidates = 1
    min_bridge_tradable_candidates = 1
    min_bridge_tradable_promoted_candidates = 1
    min_expectancy_evidence = 1
    min_robust_survivors = 1
    max_capital_slot_pressure_over_limit_count = 0
    max_capital_leverage_over_budget_count = 0
    require_expectancy_exists = 1
    require_stability_pass = 1
    require_capacity_pass = 1


def test_checklist_uses_survivor_definition_note():
    payload = checklist._build_payload(
        run_id="r1",
        args=_Args(),
        edge_metrics={
            "rows": 1,
            "promoted": 1,
            "bridge_tradable": 1,
            "bridge_tradable_promoted": 1,
        },
        expectancy_payload={"expectancy_exists": True, "expectancy_evidence": [{"x": 1}]},
        robustness_payload={
            "survivor_definition": "promotion_grade_v1",
            "survivors": [{"condition": "compression", "horizon": 4}],
            "stability_diagnostics": {"pass": True},
            "capacity_diagnostics": {"pass": True},
        },
        paths={},
    )

    gate = next(g for g in payload["gates"] if g["name"] == "robust_survivor_count")
    assert gate["passed"] is True
    assert gate["note"] == "definition=promotion_grade_v1"
    assert payload["metrics"]["robust_survivor_count"] == 1


def test_checklist_discovery_profile_relaxes_ops_gates():
    args = SimpleNamespace(
        gate_profile="discovery",
        min_edge_candidates=1,
        min_promoted_candidates=1,
        min_bridge_tradable_candidates=1,
        min_bridge_tradable_promoted_candidates=1,
        min_expectancy_evidence=1,
        min_robust_survivors=1,
        max_capital_slot_pressure_over_limit_count=0,
        max_capital_leverage_over_budget_count=0,
        require_expectancy_exists=1,
        require_stability_pass=1,
        require_capacity_pass=1,
    )
    out = checklist._apply_checklist_gate_profile(args)
    assert out.require_stability_pass == 0
    assert out.require_capacity_pass == 0


def test_checklist_capacity_profile_gates_capital_footprint_counts():
    payload = checklist._build_payload(
        run_id="r2",
        args=_Args(),
        edge_metrics={
            "rows": 2,
            "promoted": 2,
            "bridge_tradable": 2,
            "bridge_tradable_promoted": 2,
        },
        expectancy_payload={"expectancy_exists": True, "expectancy_evidence": [{"x": 1}]},
        robustness_payload={
            "survivor_definition": "promotion_grade_v1",
            "survivors": [{"condition": "compression", "horizon": 4}],
            "stability_diagnostics": {"pass": True},
            "capacity_diagnostics": {"pass": True},
        },
        capital_footprint_payload={
            "slot_pressure_over_limit_count": 1,
            "leverage_over_budget_count": 0,
        },
        paths={},
    )
    gate = next(
        g for g in payload["gates"] if g["name"] == "capital_slot_pressure_over_limit_count"
    )
    assert gate["passed"] is False
    assert payload["decision"] == "KEEP_RESEARCH"


def test_release_signoff_blocks_on_kpi_and_override_failures():
    signoff = checklist._build_release_signoff(
        run_id="r3",
        checklist_payload={"decision": "PROMOTE"},
        run_manifest_payload={
            "run_mode": "production",
            "objective_hard_gates": {
                "min_trade_count": 100,
                "min_oos_sign_consistency": 0.67,
                "max_drawdown_pct": 0.20,
            },
            "retail_profile_config": {
                "min_net_expectancy_bps": 3.0,
                "max_daily_turnover_multiple": 4.0,
            },
            "non_production_overrides": ["stage_x:flag=1"],
            "ci_fail_on_non_production_overrides": True,
        },
        kpi_payload={
            "metrics": {
                "trade_count": {"value": 50},
                "oos_sign_consistency": {"value": 0.50},
                "max_drawdown_pct": {"value": -0.35},
                "net_expectancy_bps": {"value": 1.0},
                "turnover_proxy_mean": {"value": 6.0},
            }
        },
    )

    assert signoff["decision"] == "BLOCK_RELEASE"
    assert signoff["override_audit"]["non_production_override_count"] == 1
    failed = [g["name"] for g in signoff["gates"] if not g["passed"]]
    assert "kpi_trade_count" in failed
    assert "override_audit_clean" in failed


def test_release_signoff_approves_when_gates_pass_and_no_overrides():
    signoff = checklist._build_release_signoff(
        run_id="r4",
        checklist_payload={"decision": "PROMOTE"},
        run_manifest_payload={
            "run_mode": "certification",
            "objective_hard_gates": {
                "min_trade_count": 10,
                "min_oos_sign_consistency": 0.50,
                "max_drawdown_pct": 0.30,
            },
            "retail_profile_config": {
                "min_net_expectancy_bps": 1.0,
                "max_daily_turnover_multiple": 8.0,
            },
            "non_production_overrides": [],
        },
        kpi_payload={
            "metrics": {
                "trade_count": {"value": 250},
                "oos_sign_consistency": {"value": 0.71},
                "max_drawdown_pct": {"value": -0.15},
                "net_expectancy_bps": {"value": 7.0},
                "turnover_proxy_mean": {"value": 2.0},
            }
        },
    )

    assert signoff["decision"] == "APPROVE_RELEASE"
    assert signoff["failure_reasons"] == []


def test_edge_metrics_uses_edge_candidates_parquet_first(tmp_path):
    edge_dir = tmp_path / "reports" / "edge_candidates" / "r5"
    edge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"status": "PROMOTED", "gate_bridge_tradable": True},
            {"status": "REJECTED", "gate_bridge_tradable": True},
            {"status": "PROMOTED_RESEARCH", "gate_bridge_tradable": False},
        ]
    ).to_parquet(edge_dir / "edge_candidates_normalized.parquet", index=False)

    metrics = checklist._edge_candidate_metrics(
        edge_parquet_path=edge_dir / "edge_candidates_normalized.parquet",
        edge_csv_path=edge_dir / "edge_candidates_normalized.csv",
        edge_json_path=edge_dir / "edge_candidates_normalized.json",
        promoted_candidates_parquet_path=tmp_path
        / "reports"
        / "promotions"
        / "r5"
        / "promoted_candidates.parquet",
        promoted_candidates_csv_path=tmp_path
        / "reports"
        / "promotions"
        / "r5"
        / "promoted_candidates.csv",
        promotion_audit_parquet_path=tmp_path
        / "reports"
        / "promotions"
        / "r5"
        / "promotion_statistical_audit.parquet",
        promotion_audit_csv_path=tmp_path
        / "reports"
        / "promotions"
        / "r5"
        / "promotion_statistical_audit.csv",
        promotion_summary_path=tmp_path
        / "reports"
        / "promotions"
        / "r5"
        / "promotion_summary.json",
    )

    assert metrics["source"] == "edge_candidates_parquet"
    assert metrics["rows"] == 3
    assert metrics["promoted"] == 1
    assert metrics["bridge_tradable"] == 2
    assert metrics["bridge_tradable_promoted"] == 1


def test_edge_metrics_prefers_promoted_candidates_artifact_for_promotion_counts(tmp_path):
    edge_dir = tmp_path / "reports" / "edge_candidates" / "r5b"
    promo_dir = tmp_path / "reports" / "promotions" / "r5b"
    edge_dir.mkdir(parents=True, exist_ok=True)
    promo_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"status": "PROMOTED_RESEARCH", "gate_bridge_tradable": True},
            {"status": "PROMOTED_RESEARCH", "gate_bridge_tradable": True},
        ]
    ).to_parquet(edge_dir / "edge_candidates_normalized.parquet", index=False)
    pd.DataFrame(columns=["gate_bridge_tradable"]).to_parquet(
        promo_dir / "promoted_candidates.parquet", index=False
    )

    metrics = checklist._edge_candidate_metrics(
        edge_parquet_path=edge_dir / "edge_candidates_normalized.parquet",
        edge_csv_path=edge_dir / "edge_candidates_normalized.csv",
        edge_json_path=edge_dir / "edge_candidates_normalized.json",
        promoted_candidates_parquet_path=promo_dir / "promoted_candidates.parquet",
        promoted_candidates_csv_path=promo_dir / "promoted_candidates.csv",
        promotion_audit_parquet_path=promo_dir / "promotion_statistical_audit.parquet",
        promotion_audit_csv_path=promo_dir / "promotion_statistical_audit.csv",
        promotion_summary_path=promo_dir / "promotion_summary.json",
    )

    assert metrics["rows"] == 2
    assert metrics["promoted"] == 0
    assert metrics["bridge_tradable"] == 2
    assert metrics["bridge_tradable_promoted"] == 0


def test_edge_metrics_falls_back_to_promotion_statistical_audit_when_edge_export_missing(tmp_path):
    promo_dir = tmp_path / "reports" / "promotions" / "r6"
    promo_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"promotion_decision": "promoted", "gate_bridge_tradable": True},
            {"promotion_decision": "rejected", "gate_bridge_tradable": True},
            {"promotion_decision": "promoted", "gate_bridge_tradable": False},
        ]
    ).to_parquet(promo_dir / "promotion_statistical_audit.parquet", index=False)

    metrics = checklist._edge_candidate_metrics(
        edge_parquet_path=tmp_path
        / "reports"
        / "edge_candidates"
        / "r6"
        / "edge_candidates_normalized.parquet",
        edge_csv_path=tmp_path
        / "reports"
        / "edge_candidates"
        / "r6"
        / "edge_candidates_normalized.csv",
        edge_json_path=tmp_path
        / "reports"
        / "edge_candidates"
        / "r6"
        / "edge_candidates_normalized.json",
        promoted_candidates_parquet_path=promo_dir / "promoted_candidates.parquet",
        promoted_candidates_csv_path=promo_dir / "promoted_candidates.csv",
        promotion_audit_parquet_path=promo_dir / "promotion_statistical_audit.parquet",
        promotion_audit_csv_path=promo_dir / "promotion_statistical_audit.csv",
        promotion_summary_path=promo_dir / "promotion_summary.json",
    )

    assert metrics["source"] == "promotion_audit_parquet"
    assert metrics["rows"] == 3
    assert metrics["promoted"] == 2
    assert metrics["bridge_tradable"] == 2
    assert metrics["bridge_tradable_promoted"] == 1


def test_kpi_payload_hydrates_from_promotion_statistical_audit_when_missing(tmp_path):
    promo_dir = tmp_path / "reports" / "promotions" / "r7"
    promo_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "n_events": 100,
                "bridge_validation_stressed_after_cost_bps": 10.0,
                "sign_consistency": 0.50,
                "turnover_proxy_mean": 0.8,
                "naive_max_drawdown": -0.10,
            },
            {
                "n_events": 50,
                "bridge_validation_stressed_after_cost_bps": 20.0,
                "sign_consistency": 1.00,
                "turnover_proxy_mean": 1.2,
                "naive_max_drawdown": -0.20,
            },
        ]
    ).to_parquet(promo_dir / "promotion_statistical_audit.parquet", index=False)

    hydrated = checklist._hydrate_kpi_payload_with_promotion_fallback(
        kpi_payload={"metrics": {}},
        promotion_audit_parquet_path=promo_dir / "promotion_statistical_audit.parquet",
        promotion_audit_csv_path=promo_dir / "promotion_statistical_audit.csv",
    )

    assert hydrated["hydrated_with_promotion_fallback"] is True
    assert checklist._metric_value(hydrated, "trade_count") == 150.0
    assert checklist._metric_value(hydrated, "net_expectancy_bps") == 15.0
    assert checklist._metric_value(hydrated, "oos_sign_consistency") == 0.75
    assert checklist._metric_value(hydrated, "turnover_proxy_mean") == 1.0
    assert checklist._metric_value(hydrated, "max_drawdown_pct") == -0.2


def test_metric_value_treats_missing_kpi_as_default_without_warning(caplog) -> None:
    with caplog.at_level(logging.WARNING):
        out = checklist._metric_value({"metrics": {"trade_count": {"value": None}}}, "trade_count")

    assert out == 0.0
    assert caplog.records == []


def test_main_writes_warning_manifest_and_exits_zero_on_keep_research(tmp_path, monkeypatch):
    data_root = tmp_path / "data"
    run_id = "checklist_keep_research"
    reports_root = data_root / "reports"
    runs_root = data_root / "runs"
    expectancy_dir = reports_root / "expectancy" / run_id
    edge_dir = reports_root / "edge_candidates" / run_id
    promo_dir = reports_root / "promotions" / run_id
    expectancy_dir.mkdir(parents=True, exist_ok=True)
    edge_dir.mkdir(parents=True, exist_ok=True)
    promo_dir.mkdir(parents=True, exist_ok=True)
    (expectancy_dir / "conditional_expectancy.json").write_text(
        '{"expectancy_exists": true}',
        encoding="utf-8",
    )
    (expectancy_dir / "conditional_expectancy_robustness.json").write_text(
        '{"survivor_definition": "promotion_grade_v1", "survivors": []}',
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {"status": "PROMOTED_RESEARCH", "gate_bridge_tradable": True},
        ]
    ).to_parquet(edge_dir / "edge_candidates_normalized.parquet", index=False)
    pd.DataFrame(columns=["gate_bridge_tradable"]).to_parquet(
        promo_dir / "promoted_candidates.parquet", index=False
    )
    (runs_root / run_id).mkdir(parents=True, exist_ok=True)
    (runs_root / run_id / "run_manifest.json").write_text("{}", encoding="utf-8")
    (runs_root / run_id / "kpi_scorecard.json").write_text('{"metrics": {}}', encoding="utf-8")

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))
    monkeypatch.setattr(
        checklist,
        "_parse_args",
        lambda: SimpleNamespace(
            run_id=run_id,
            gate_profile="promotion",
            reports_root=str(reports_root),
            runs_root=str(runs_root),
            out_dir="",
            retail_profile="capital_constrained",
            min_edge_candidates=1,
            min_promoted_candidates=1,
            min_bridge_tradable_candidates=1,
            min_bridge_tradable_promoted_candidates=1,
            min_expectancy_evidence=1,
            min_robust_survivors=1,
            max_capital_slot_pressure_over_limit_count=0,
            max_capital_leverage_over_budget_count=0,
            require_expectancy_exists=1,
            require_stability_pass=1,
            require_capacity_pass=1,
        ),
    )

    assert checklist.main() == 0

    stage_manifest = json.loads(
        (runs_root / run_id / "generate_recommendations_checklist.json").read_text(encoding="utf-8")
    )
    assert stage_manifest["status"] == "warning"
    assert stage_manifest["stats"]["decision"] == "KEEP_RESEARCH"

    checklist_payload = json.loads(
        (runs_root / run_id / "research_checklist" / "checklist.json").read_text(encoding="utf-8")
    )
    assert checklist_payload["decision"] == "KEEP_RESEARCH"
