from __future__ import annotations

import json

import pandas as pd

from project.research.services import confirmatory_candidate_service as svc


def test_compare_confirmatory_candidates_matches_by_structural_key(tmp_path):
    data_root = tmp_path / "data"
    origin_dir = data_root / "reports" / "edge_candidates" / "origin_run"
    target_dir = data_root / "reports" / "phase2" / "target_run" / "search_engine"
    origin_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "candidate_id": "origin_1",
                "symbol": "BTCUSDT",
                "event_type": "STATE_CHOP_STATE",
                "direction": "long",
                "rule_template": "continuation",
                "horizon": "60m",
                "gate_bridge_tradable": "pass",
                "cost_config_digest": "digest-a",
                "fee_bps_per_side": 4.0,
                "slippage_bps_per_fill": 2.0,
                "cost_bps": 6.0,
                "round_trip_cost_bps": 12.0,
                "after_cost_includes_funding_carry": False,
                "cost_model_source": "stub",
                "q_value": 0.01,
            },
            {
                "candidate_id": "origin_2",
                "symbol": "BTCUSDT",
                "event_type": "TRANSITION_TRENDING_STATE_CHOP_STATE",
                "direction": "long",
                "rule_template": "mean_reversion",
                "horizon": "5m",
                "gate_bridge_tradable": "pass",
                "cost_config_digest": "digest-a",
                "fee_bps_per_side": 4.0,
                "slippage_bps_per_fill": 2.0,
                "cost_bps": 6.0,
                "round_trip_cost_bps": 12.0,
                "after_cost_includes_funding_carry": False,
                "cost_model_source": "stub",
                "q_value": 0.02,
            },
        ]
    ).to_parquet(origin_dir / "edge_candidates_normalized.parquet", index=False)

    pd.DataFrame(
        [
            {
                "candidate_id": "target_a",
                "symbol": "BTCUSDT",
                "event_type": "STATE_CHOP_STATE",
                "direction": "long",
                "rule_template": "continuation",
                "horizon": "60m",
                "gate_oos_validation": True,
                "gate_after_cost_positive": True,
                "gate_after_cost_stressed_positive": True,
                "gate_bridge_tradable": True,
                "gate_multiplicity": True,
                "gate_c_regime_stable": True,
                "gate_multiplicity_strict": True,
                "bridge_eval_status": "tradable",
                "cost_config_digest": "digest-a",
                "fee_bps_per_side": 4.0,
                "slippage_bps_per_fill": 2.0,
                "cost_bps": 6.0,
                "round_trip_cost_bps": 12.0,
                "after_cost_includes_funding_carry": False,
                "cost_model_source": "stub",
                "q_value": 0.03,
            },
            {
                "candidate_id": "target_b",
                "symbol": "BTCUSDT",
                "event_type": "TRANSITION_TRENDING_STATE_CHOP_STATE",
                "direction": "long",
                "rule_template": "mean_reversion",
                "horizon": "5m",
                "gate_oos_validation": False,
                "gate_after_cost_positive": False,
                "gate_after_cost_stressed_positive": False,
                "gate_bridge_tradable": False,
                "gate_multiplicity": True,
                "gate_c_regime_stable": True,
                "gate_multiplicity_strict": False,
                "bridge_eval_status": "rejected",
                "cost_config_digest": "digest-a",
                "fee_bps_per_side": 4.0,
                "slippage_bps_per_fill": 2.0,
                "cost_bps": 6.0,
                "round_trip_cost_bps": 12.0,
                "after_cost_includes_funding_carry": False,
                "cost_model_source": "stub",
                "q_value": 0.50,
            },
        ]
    ).to_parquet(target_dir / "phase2_candidates.parquet", index=False)

    payload = svc.compare_confirmatory_candidates(
        data_root=data_root,
        origin_run_id="origin_run",
        target_run_id="target_run",
    )

    assert payload["origin_summary"]["candidate_count"] == 2
    assert payload["matched_summary"]["matched_structural_rows"] == 2
    assert payload["matched_summary"]["matched_bridge_pass_count"] == 1
    assert payload["matched_summary"]["matched_gate_pass_count"] == 1
    assert payload["matched_summary"]["matched_strict_pass_count"] == 1

    report_path = svc.write_confirmatory_candidate_report(
        data_root=data_root,
        origin_run_id="origin_run",
        target_run_id="target_run",
    )
    report = json.loads(report_path.read_text())
    assert report["matched_summary"]["matched_structural_keys"] == 2


def test_plan_confirmatory_window_reports_missing_forward_month(tmp_path):
    data_root = tmp_path / "data"
    (data_root / "runs" / "origin_run").mkdir(parents=True, exist_ok=True)
    (data_root / "runs" / "older_run").mkdir(parents=True, exist_ok=True)
    (data_root / "reports" / "phase2" / "older_run" / "search_engine").mkdir(
        parents=True, exist_ok=True
    )
    (
        data_root
        / "lake"
        / "raw"
        / "binance"
        / "perp"
        / "BTCUSDT"
        / "funding"
        / "year=2025"
        / "month=01"
    ).mkdir(parents=True, exist_ok=True)

    (data_root / "runs" / "origin_run" / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "origin_run",
                "start": "2025-01-01",
                "end": "2025-01-31",
                "normalized_symbols": ["BTCUSDT"],
            }
        ),
        encoding="utf-8",
    )
    (data_root / "runs" / "older_run" / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "older_run",
                "start": "2024-12-01",
                "end": "2024-12-31",
                "normalized_symbols": ["BTCUSDT"],
            }
        ),
        encoding="utf-8",
    )
    pd.DataFrame([{"candidate_id": "x"}]).to_parquet(
        data_root
        / "reports"
        / "phase2"
        / "older_run"
        / "search_engine"
        / "phase2_candidates.parquet",
        index=False,
    )

    payload = svc.plan_confirmatory_window(
        data_root=data_root,
        origin_run_id="origin_run",
    )

    assert payload["readiness"] == "blocked_by_missing_forward_data"
    assert payload["next_required_funding_month"] == "2025-02"
    assert payload["local_common_funding_months"] == ["2025-01"]
    assert payload["nearest_forward_local_target"] is None
    assert payload["target_runs_considered"][0]["funding_covered"] is False

    out_path = svc.write_confirmatory_window_plan(
        data_root=data_root,
        origin_run_id="origin_run",
    )
    report = json.loads(out_path.read_text(encoding="utf-8"))
    assert report["next_required_funding_month"] == "2025-02"


def test_adjacent_survivorship_classifies_fail_reasons(tmp_path):
    data_root = tmp_path / "data"
    origin_dir = data_root / "reports" / "edge_candidates" / "origin_run"
    target_dir = data_root / "reports" / "phase2" / "target_run" / "search_engine"
    origin_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "candidate_id": "origin_1",
                "symbol": "BTCUSDT",
                "event_type": "STATE_CHOP_STATE",
                "direction": "long",
                "rule_template": "continuation",
                "horizon": "60m",
                "gate_bridge_tradable": "pass",
                "q_value": 0.01,
                "after_cost_expectancy_per_trade": 0.002,
            },
            {
                "candidate_id": "origin_2",
                "symbol": "BTCUSDT",
                "event_type": "TRANSITION_TRENDING_STATE_CHOP_STATE",
                "direction": "long",
                "rule_template": "mean_reversion",
                "horizon": "5m",
                "gate_bridge_tradable": "pass",
                "q_value": 0.02,
                "after_cost_expectancy_per_trade": 0.001,
            },
        ]
    ).to_parquet(origin_dir / "edge_candidates_normalized.parquet", index=False)

    pd.DataFrame(
        [
            {
                "candidate_id": "target_1",
                "symbol": "BTCUSDT",
                "event_type": "STATE_CHOP_STATE",
                "direction": "long",
                "rule_template": "continuation",
                "horizon": "60m",
                "gate_oos_validation": False,
                "gate_after_cost_positive": False,
                "gate_after_cost_stressed_positive": False,
                "gate_bridge_tradable": False,
                "gate_multiplicity": True,
                "gate_multiplicity_strict": True,
                "gate_c_regime_stable": False,
                "q_value": 0.2,
                "after_cost_expectancy_per_trade": -0.001,
                "stressed_after_cost_expectancy_per_trade": -0.002,
            }
        ]
    ).to_parquet(target_dir / "phase2_candidates.parquet", index=False)

    payload = svc.build_adjacent_survivorship_payload(
        data_root=data_root,
        origin_run_id="origin_run",
        target_run_id="target_run",
    )

    assert payload["origin_survivor_count"] == 2
    assert payload["adjacent_survivor_count"] == 0
    assert payload["failure_reason_counts"]["oos_validation_fail"] == 1
    assert payload["failure_reason_counts"]["after_cost_negative"] == 1
    assert payload["failure_reason_counts"]["regime_unstable"] == 1
    assert payload["failure_reason_counts"]["missing_in_target"] == 1

    out_path = svc.write_adjacent_survivorship_report(
        data_root=data_root,
        origin_run_id="origin_run",
        target_run_id="target_run",
    )
    report = json.loads(out_path.read_text(encoding="utf-8"))
    assert report["primary_event_ids"] == [
        "STATE_CHOP_STATE",
        "TRANSITION_TRENDING_STATE_CHOP_STATE",
    ]
    assert report["by_primary_event_id"]["STATE_CHOP_STATE"]["origin_count"] == 1
    assert report["compat_grouping_aliases"]["by_event_family"] == "by_primary_event_id"
    assert report["by_event_family"]["STATE_CHOP_STATE"]["origin_count"] == 1


def test_build_confirmatory_workflow_payload_blocks_when_forward_data_missing(tmp_path):
    data_root = tmp_path / "data"
    (data_root / "runs" / "origin_run").mkdir(parents=True, exist_ok=True)
    (
        data_root
        / "lake"
        / "raw"
        / "binance"
        / "perp"
        / "BTCUSDT"
        / "funding"
        / "year=2025"
        / "month=01"
    ).mkdir(parents=True, exist_ok=True)
    (data_root / "runs" / "origin_run" / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "origin_run",
                "start": "2025-01-01",
                "end": "2025-01-31",
                "normalized_symbols": ["BTCUSDT"],
            }
        ),
        encoding="utf-8",
    )

    payload = svc.build_confirmatory_workflow_payload(
        data_root=data_root,
        origin_run_id="origin_run",
    )

    assert payload["workflow_status"] == "blocked"
    assert payload["next_action"] == "ingest_forward_data"
    assert payload["blocking_reason"] == "2025-02"


def test_build_confirmatory_workflow_payload_recommends_promotion_review_for_strict_pass(tmp_path):
    data_root = tmp_path / "data"
    origin_dir = data_root / "reports" / "edge_candidates" / "origin_run"
    target_dir = data_root / "reports" / "phase2" / "target_run" / "search_engine"
    origin_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    (data_root / "runs" / "origin_run").mkdir(parents=True, exist_ok=True)
    (data_root / "runs" / "origin_run" / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "origin_run",
                "start": "2025-01-01",
                "end": "2025-01-31",
                "normalized_symbols": ["BTCUSDT"],
            }
        ),
        encoding="utf-8",
    )
    (
        data_root
        / "lake"
        / "raw"
        / "binance"
        / "perp"
        / "BTCUSDT"
        / "funding"
        / "year=2025"
        / "month=01"
    ).mkdir(parents=True, exist_ok=True)
    (
        data_root
        / "lake"
        / "raw"
        / "binance"
        / "perp"
        / "BTCUSDT"
        / "funding"
        / "year=2025"
        / "month=02"
    ).mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "candidate_id": "origin_1",
                "symbol": "BTCUSDT",
                "event_type": "STATE_CHOP_STATE",
                "direction": "long",
                "rule_template": "continuation",
                "horizon": "60m",
                "gate_bridge_tradable": "pass",
                "cost_config_digest": "digest-a",
                "fee_bps_per_side": 4.0,
                "slippage_bps_per_fill": 2.0,
                "cost_bps": 6.0,
                "round_trip_cost_bps": 12.0,
                "after_cost_includes_funding_carry": False,
                "cost_model_source": "stub",
                "q_value": 0.01,
            }
        ]
    ).to_parquet(origin_dir / "edge_candidates_normalized.parquet", index=False)
    pd.DataFrame(
        [
            {
                "candidate_id": "target_a",
                "symbol": "BTCUSDT",
                "event_type": "STATE_CHOP_STATE",
                "direction": "long",
                "rule_template": "continuation",
                "horizon": "60m",
                "gate_oos_validation": True,
                "gate_after_cost_positive": True,
                "gate_after_cost_stressed_positive": True,
                "gate_bridge_tradable": True,
                "gate_multiplicity": True,
                "gate_c_regime_stable": True,
                "gate_multiplicity_strict": True,
                "bridge_eval_status": "tradable",
                "cost_config_digest": "digest-a",
                "fee_bps_per_side": 4.0,
                "slippage_bps_per_fill": 2.0,
                "cost_bps": 6.0,
                "round_trip_cost_bps": 12.0,
                "after_cost_includes_funding_carry": False,
                "cost_model_source": "stub",
                "q_value": 0.03,
            }
        ]
    ).to_parquet(target_dir / "phase2_candidates.parquet", index=False)

    payload = svc.build_confirmatory_workflow_payload(
        data_root=data_root,
        origin_run_id="origin_run",
        target_run_id="target_run",
    )

    assert payload["workflow_status"] == "confirmatory_strict_pass"
    assert payload["next_action"] == "promotion_review"


def test_build_confirmatory_workflow_payload_blocks_missing_cost_identity(tmp_path):
    data_root = tmp_path / "data"
    origin_dir = data_root / "reports" / "edge_candidates" / "origin_run"
    target_dir = data_root / "reports" / "phase2" / "target_run" / "search_engine"
    origin_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    (data_root / "runs" / "origin_run").mkdir(parents=True, exist_ok=True)
    (data_root / "runs" / "origin_run" / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "origin_run",
                "start": "2025-01-01",
                "end": "2025-01-31",
                "normalized_symbols": ["BTCUSDT"],
            }
        ),
        encoding="utf-8",
    )
    (
        data_root
        / "lake"
        / "raw"
        / "binance"
        / "perp"
        / "BTCUSDT"
        / "funding"
        / "year=2025"
        / "month=01"
    ).mkdir(parents=True, exist_ok=True)
    (
        data_root
        / "lake"
        / "raw"
        / "binance"
        / "perp"
        / "BTCUSDT"
        / "funding"
        / "year=2025"
        / "month=02"
    ).mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "candidate_id": "origin_1",
                "symbol": "BTCUSDT",
                "event_type": "STATE_CHOP_STATE",
                "direction": "long",
                "rule_template": "continuation",
                "horizon": "60m",
                "gate_bridge_tradable": "pass",
                "q_value": 0.01,
            }
        ]
    ).to_parquet(origin_dir / "edge_candidates_normalized.parquet", index=False)
    pd.DataFrame(
        [
            {
                "candidate_id": "target_a",
                "symbol": "BTCUSDT",
                "event_type": "STATE_CHOP_STATE",
                "direction": "long",
                "rule_template": "continuation",
                "horizon": "60m",
                "gate_oos_validation": True,
                "gate_after_cost_positive": True,
                "gate_after_cost_stressed_positive": True,
                "gate_bridge_tradable": True,
                "gate_multiplicity": True,
                "gate_c_regime_stable": True,
                "gate_multiplicity_strict": True,
                "bridge_eval_status": "tradable",
                "q_value": 0.03,
            }
        ]
    ).to_parquet(target_dir / "phase2_candidates.parquet", index=False)

    payload = svc.build_confirmatory_workflow_payload(
        data_root=data_root,
        origin_run_id="origin_run",
        target_run_id="target_run",
    )

    assert payload["workflow_status"] == "blocked"
    assert payload["next_action"] == "repair_confirmatory_cost_identity"
    assert payload["comparison"]["strict_matching_blocked"] is True



def test_compare_confirmatory_candidates_requires_entry_lag_match(tmp_path):
    data_root = tmp_path / "data"
    origin_dir = data_root / "reports" / "edge_candidates" / "origin_run"
    target_dir = data_root / "reports" / "phase2" / "target_run" / "search_engine"
    origin_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "candidate_id": "origin_1",
                "symbol": "BTCUSDT",
                "event_type": "STATE_CHOP_STATE",
                "direction": "long",
                "rule_template": "continuation",
                "horizon": "60m",
                "entry_lag_bars": 1,
                "gate_bridge_tradable": "pass",
                "q_value": 0.01,
            }
        ]
    ).to_parquet(origin_dir / "edge_candidates_normalized.parquet", index=False)

    pd.DataFrame(
        [
            {
                "candidate_id": "target_a",
                "symbol": "BTCUSDT",
                "event_type": "STATE_CHOP_STATE",
                "direction": "long",
                "rule_template": "continuation",
                "horizon": "60m",
                "entry_lag_bars": 2,
                "gate_oos_validation": True,
                "gate_after_cost_positive": True,
                "gate_after_cost_stressed_positive": True,
                "gate_bridge_tradable": True,
                "gate_multiplicity": True,
                "gate_c_regime_stable": True,
                "gate_multiplicity_strict": True,
                "bridge_eval_status": "tradable",
                "q_value": 0.03,
            }
        ]
    ).to_parquet(target_dir / "phase2_candidates.parquet", index=False)

    payload = svc.compare_confirmatory_candidates(
        data_root=data_root,
        origin_run_id="origin_run",
        target_run_id="target_run",
    )

    assert "entry_lag_bars" in payload["structural_key_columns"]
    assert payload["matched_summary"]["matched_structural_rows"] == 0


def test_plan_confirmatory_window_discovers_flat_phase2_targets(tmp_path):
    data_root = tmp_path / "data"
    (data_root / "runs" / "origin_run").mkdir(parents=True, exist_ok=True)
    (data_root / "runs" / "target_run").mkdir(parents=True, exist_ok=True)
    (data_root / "runs" / "origin_run" / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "origin_run",
                "start": "2025-01-01",
                "end": "2025-01-31",
                "normalized_symbols": ["BTCUSDT"],
            }
        ),
        encoding="utf-8",
    )
    (data_root / "runs" / "target_run" / "run_manifest.json").write_text(
        json.dumps(
            {
                "run_id": "target_run",
                "start": "2025-02-01",
                "end": "2025-02-28",
                "normalized_symbols": ["BTCUSDT"],
            }
        ),
        encoding="utf-8",
    )
    (
        data_root
        / "lake"
        / "raw"
        / "binance"
        / "perp"
        / "BTCUSDT"
        / "funding"
        / "year=2025"
        / "month=01"
    ).mkdir(parents=True, exist_ok=True)
    (
        data_root
        / "lake"
        / "raw"
        / "binance"
        / "perp"
        / "BTCUSDT"
        / "funding"
        / "year=2025"
        / "month=02"
    ).mkdir(parents=True, exist_ok=True)
    (data_root / "reports" / "phase2" / "target_run").mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"candidate_id": "flat"}]).to_parquet(
        data_root / "reports" / "phase2" / "target_run" / "phase2_candidates.parquet",
        index=False,
    )

    payload = svc.plan_confirmatory_window(
        data_root=data_root,
        origin_run_id="origin_run",
    )

    assert [row["run_id"] for row in payload["target_runs_considered"]] == ["target_run"]
    assert payload["nearest_forward_local_target"]["run_id"] == "target_run"


def test_compare_confirmatory_candidates_supports_flat_phase2_layout(tmp_path):
    data_root = tmp_path / "data"
    origin_dir = data_root / "reports" / "edge_candidates" / "origin_run"
    target_dir = data_root / "reports" / "phase2" / "target_run"
    origin_dir.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "candidate_id": "origin_1",
                "symbol": "BTCUSDT",
                "event_type": "STATE_CHOP_STATE",
                "direction": "long",
                "rule_template": "continuation",
                "horizon": "60m",
                "gate_bridge_tradable": "pass",
                "q_value": 0.01,
            }
        ]
    ).to_parquet(origin_dir / "edge_candidates_normalized.parquet", index=False)
    pd.DataFrame(
        [
            {
                "candidate_id": "target_a",
                "symbol": "BTCUSDT",
                "event_type": "STATE_CHOP_STATE",
                "direction": "long",
                "rule_template": "continuation",
                "horizon": "60m",
                "gate_oos_validation": True,
                "gate_after_cost_positive": True,
                "gate_after_cost_stressed_positive": True,
                "gate_bridge_tradable": True,
                "gate_multiplicity": True,
                "gate_c_regime_stable": True,
                "gate_multiplicity_strict": True,
                "q_value": 0.03,
            }
        ]
    ).to_parquet(target_dir / "phase2_candidates.parquet", index=False)

    payload = svc.compare_confirmatory_candidates(
        data_root=data_root,
        origin_run_id="origin_run",
        target_run_id="target_run",
    )

    assert payload["target_summary"]["candidate_count"] == 1
    assert payload["matched_summary"]["matched_structural_rows"] == 1
    assert payload["target_path"].endswith("/reports/phase2/target_run/phase2_candidates.parquet")
