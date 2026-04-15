from __future__ import annotations

import json
import sys

import numpy as np
import pandas as pd

from project.research import build_strategy_candidates


def test_edge_candidate_market_entry_action_is_executable(monkeypatch):
    row = {
        "event": "LIQUIDITY_VACUUM",
        "candidate_id": "cand_001",
        "status": "PROMOTED",
        "edge_score": 0.25,
        "expectancy_per_trade": 0.2,
        "expectancy_after_multiplicity": 0.18,
        "stability_proxy": 0.7,
        "robustness_score": 0.7,
        "event_frequency": 0.1,
        "capacity_proxy": 0.2,
        "profit_density_score": 0.05,
        "n_events": 120,
        "gate_oos_consistency_strict": True,
        "gate_bridge_tradable": True,
    }
    detail = {
        "condition": "all",
        "action": "enter_short_market",
        "gate_oos_consistency_strict": True,
        "gate_bridge_tradable": True,
    }

    candidate = build_strategy_candidates._build_edge_strategy_candidate(
        row=row,
        detail=detail,
        symbols=["BTCUSDT"],
    )

    assert candidate["executable_action"] is True


def test_fractional_allocation_synthesizer_downsizes_non_tradable_profile():
    policy = build_strategy_candidates._synthesize_fractional_allocation_policy(
        {
            "turnover_proxy_mean": 10.0,
            "effective_cost_bps": 20.0,
            "net_expectancy_bps": 8.0,
        },
        retail_profile_cfg={
            "max_daily_turnover_multiple": 4.0,
            "max_fee_plus_slippage_bps": 10.0,
            "min_net_expectancy_bps": 2.0,
        },
    )
    assert policy["mode"] == "fractional_top_quantile"
    assert 0.05 <= float(policy["signal_take_rate"]) < 1.0
    assert "projected_turnover_multiple" in policy


def _run_builder(monkeypatch, tmp_path, argv):
    monkeypatch.setattr(build_strategy_candidates, "get_data_root", lambda: tmp_path)
    monkeypatch.setattr(build_strategy_candidates, "checklist_decision", lambda **kwargs: "PROMOTE")
    monkeypatch.setattr(
        build_strategy_candidates, "load_promoted_blueprints", lambda **kwargs: ([], {})
    )
    monkeypatch.setattr(
        build_strategy_candidates,
        "load_retail_profile",
        lambda **kwargs: {
            "max_daily_turnover_multiple": 4.0,
            "max_fee_plus_slippage_bps": 10.0,
            "min_net_expectancy_bps": 2.0,
        },
    )
    monkeypatch.setattr(build_strategy_candidates, "start_manifest", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        build_strategy_candidates, "finalize_manifest", lambda *args, **kwargs: None
    )
    monkeypatch.setattr(sys, "argv", argv)
    assert build_strategy_candidates.main() == 0
    return json.loads(
        (tmp_path / "reports" / "strategy_builder" / "r1" / "strategy_candidates.json").read_text(
            encoding="utf-8"
        )
    )


def test_builder_loads_parquet_edge_artifacts_and_applies_top_k(monkeypatch, tmp_path):
    edge_dir = tmp_path / "reports" / "edge_candidates" / "r1"
    edge_dir.mkdir(parents=True, exist_ok=True)
    detail_path = tmp_path / "phase2_candidates.parquet"
    pd.DataFrame(
        [
            {"candidate_id": "cand_top", "condition": "all", "action": "enter_long_market"},
            {"candidate_id": "cand_second", "condition": "all", "action": "enter_short_market"},
        ]
    ).to_parquet(detail_path, index=False)
    pd.DataFrame(
        [
            {
                "event": "VOL_SHOCK",
                "candidate_id": "cand_top",
                "status": "PROMOTED",
                "edge_score": 0.8,
                "selection_score_executed": 0.8,
                "expectancy_per_trade": 0.01,
                "expectancy_after_multiplicity": 0.01,
                "stability_proxy": 0.7,
                "robustness_score": 0.7,
                "event_frequency": 0.2,
                "capacity_proxy": 0.2,
                "profit_density_score": 0.1,
                "n_events": 120,
                "source_path": str(detail_path),
            },
            {
                "event": "VOL_SHOCK",
                "candidate_id": "cand_second",
                "status": "PROMOTED",
                "edge_score": 0.5,
                "selection_score_executed": 0.5,
                "expectancy_per_trade": 0.009,
                "expectancy_after_multiplicity": 0.009,
                "stability_proxy": 0.6,
                "robustness_score": 0.6,
                "event_frequency": 0.2,
                "capacity_proxy": 0.2,
                "profit_density_score": 0.09,
                "n_events": 110,
                "source_path": str(detail_path),
            },
        ]
    ).to_parquet(edge_dir / "edge_candidates_normalized.parquet", index=False)

    rows = _run_builder(
        monkeypatch,
        tmp_path,
        [
            "build_strategy_candidates",
            "--run_id",
            "r1",
            "--symbols",
            "BTCUSDT",
            "--top_k_per_event",
            "1",
            "--max_candidates_per_event",
            "5",
            "--include_alpha_bundle",
            "0",
        ],
    )

    assert [row["candidate_id"] for row in rows] == ["cand_top"]
    assert rows[0]["action"] == "enter_long_market"


def test_builder_can_fallback_missing_detail_and_gate_fractional_allocation(monkeypatch, tmp_path):
    edge_dir = tmp_path / "reports" / "edge_candidates" / "r1"
    edge_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "event": "LIQUIDITY_VACUUM",
                "candidate_id": "cand_frac",
                "status": "PROMOTED",
                "edge_score": 0.6,
                "selection_score_executed": 0.6,
                "expectancy_per_trade": 0.0008,
                "expectancy_after_multiplicity": 0.0008,
                "stability_proxy": 0.7,
                "robustness_score": 0.7,
                "event_frequency": 0.2,
                "capacity_proxy": 0.2,
                "profit_density_score": 0.1,
                "n_events": 120,
                "direction": 1.0,
                "turnover_proxy_mean": 10.0,
                "avg_dynamic_cost_bps": 20.0,
                "source_path": str(tmp_path / "missing_candidates.parquet"),
            }
        ]
    ).to_parquet(edge_dir / "edge_candidates_normalized.parquet", index=False)

    enabled_rows = _run_builder(
        monkeypatch,
        tmp_path,
        [
            "build_strategy_candidates",
            "--run_id",
            "r1",
            "--symbols",
            "BTCUSDT",
            "--allow_missing_candidate_detail",
            "1",
            "--enable_fractional_allocation",
            "1",
            "--include_alpha_bundle",
            "0",
        ],
    )
    disabled_rows = _run_builder(
        monkeypatch,
        tmp_path,
        [
            "build_strategy_candidates",
            "--run_id",
            "r1",
            "--symbols",
            "BTCUSDT",
            "--allow_missing_candidate_detail",
            "1",
            "--enable_fractional_allocation",
            "0",
            "--include_alpha_bundle",
            "0",
        ],
    )

    assert enabled_rows[0]["action"] == "enter_long_market"
    assert enabled_rows[0]["fractional_allocation_applied"] is True
    assert float(enabled_rows[0]["risk_controls"]["size_scale"]) < 1.0
    assert disabled_rows[0]["fractional_allocation_applied"] is False
    assert float(disabled_rows[0]["risk_controls"]["size_scale"]) == 1.0


def test_builder_includes_alpha_bundle_candidates_when_requested(monkeypatch, tmp_path):
    alpha_dir = tmp_path / "feature_store" / "alpha_bundle" / "r1"
    alpha_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"symbol": "BTCUSDT", "score": 0.4},
            {"symbol": "BTCUSDT", "score": 0.2},
        ]
    ).to_parquet(alpha_dir / "alpha_bundle_scores.parquet", index=False)

    rows = _run_builder(
        monkeypatch,
        tmp_path,
        [
            "build_strategy_candidates",
            "--run_id",
            "r1",
            "--symbols",
            "BTCUSDT",
            "--include_alpha_bundle",
            "1",
        ],
    )

    assert rows[0]["source_type"] == "alpha_bundle"
    assert rows[0]["base_strategy"] == "dsl_interpreter_v1"
    assert rows[0]["strategy_instances"][0]["base_strategy"] == "dsl_interpreter_v1"


def test_builder_honors_fractional_policy_when_flag_is_null(monkeypatch, tmp_path):
    edge_dir = tmp_path / "reports" / "edge_candidates" / "r1"
    edge_dir.mkdir(parents=True, exist_ok=True)
    detail_path = tmp_path / "phase2_candidates.parquet"
    pd.DataFrame(
        [{"candidate_id": "cand_policy", "condition": "all", "action": "enter_long_market"}]
    ).to_parquet(detail_path, index=False)
    pd.DataFrame(
        [
            {
                "event": "VOL_SHOCK",
                "candidate_id": "cand_policy",
                "status": "PROMOTED",
                "edge_score": 0.7,
                "selection_score_executed": 0.7,
                "expectancy_per_trade": 0.01,
                "expectancy_after_multiplicity": 0.01,
                "stability_proxy": 0.7,
                "robustness_score": 0.7,
                "event_frequency": 0.2,
                "capacity_proxy": 0.2,
                "profit_density_score": 0.1,
                "n_events": 120,
                "source_path": str(detail_path),
                "allocation_policy_json": json.dumps(
                    {
                        "mode": "fractional_top_quantile",
                        "signal_take_rate": 0.25,
                        "max_participation_rate": 0.1,
                        "allocation_viable": True,
                    }
                ),
                "fractional_allocation_applied": np.nan,
            }
        ]
    ).to_parquet(edge_dir / "edge_candidates_normalized.parquet", index=False)

    rows = _run_builder(
        monkeypatch,
        tmp_path,
        [
            "build_strategy_candidates",
            "--run_id",
            "r1",
            "--symbols",
            "BTCUSDT",
            "--include_alpha_bundle",
            "0",
            "--enable_fractional_allocation",
            "1",
        ],
    )

    assert rows[0]["fractional_allocation_applied"] is True
    assert float(rows[0]["risk_controls"]["size_scale"]) == 0.25
    assert float(rows[0]["risk_controls"]["max_participation_rate"]) == 0.1


def test_builder_consumes_compiled_blueprints_file(monkeypatch, tmp_path):
    blueprint_dir = tmp_path / "reports" / "strategy_blueprints" / "r1"
    blueprint_dir.mkdir(parents=True, exist_ok=True)
    (blueprint_dir / "blueprints.jsonl").write_text(
        json.dumps(
            {
                "id": "bp_r1_vol_shock_cand_bp_single_symbol",
                "run_id": "r1",
                "event_type": "VOL_SHOCK",
                "candidate_id": "cand_bp",
                "symbol_scope": {
                    "mode": "single_symbol",
                    "symbols": ["BTCUSDT"],
                    "candidate_symbol": "BTCUSDT",
                },
                "entry": {
                    "triggers": ["vol_shock_relaxation_event"],
                    "conditions": ["all"],
                    "confirmations": [],
                    "delay_bars": 0,
                    "cooldown_bars": 0,
                    "condition_logic": "all",
                    "condition_nodes": [],
                    "arm_bars": 0,
                    "reentry_lockout_bars": 0,
                },
                "overlays": [],
                "lineage": {
                    "events_count_used_for_gate": 180,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    promo_dir = tmp_path / "reports" / "promotions" / "r1"
    promo_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "candidate_id": "cand_bp",
                "event_type": "VOL_SHOCK",
                "bridge_certified": True,
                "status": "PROMOTED",
                "selection_score": 0.55,
                "quality_score": 0.55,
                "expectancy_after_multiplicity": 0.01,
                "expectancy_per_trade": 0.01,
                "edge_score": 0.55,
                "stability_proxy": 0.7,
                "robustness_score": 0.7,
                "oos_sign_consistency": 0.82,
                "n_events": 180,
            }
        ]
    ).to_parquet(promo_dir / "promoted_candidates.parquet", index=False)

    rows = _run_builder(
        monkeypatch,
        tmp_path,
        [
            "build_strategy_candidates",
            "--run_id",
            "r1",
            "--symbols",
            "BTCUSDT",
            "--blueprints_file",
            str(blueprint_dir / "blueprints.jsonl"),
            "--include_alpha_bundle",
            "0",
        ],
    )

    assert rows[0]["candidate_id"] == "cand_bp"
    assert rows[0]["source_type"] == "promoted_blueprint"
    assert float(rows[0]["selection_score"]) == 0.55
    assert float(rows[0]["oos_sign_consistency"]) == 0.82


def test_builder_skips_compiled_blueprints_without_promoted_metrics(monkeypatch, tmp_path):
    blueprint_dir = tmp_path / "reports" / "strategy_blueprints" / "r1"
    blueprint_dir.mkdir(parents=True, exist_ok=True)
    (blueprint_dir / "blueprints.jsonl").write_text(
        json.dumps(
            {
                "id": "bp_r1_vol_shock_cand_missing_single_symbol",
                "run_id": "r1",
                "event_type": "VOL_SHOCK",
                "candidate_id": "cand_missing",
                "symbol_scope": {
                    "mode": "single_symbol",
                    "symbols": ["BTCUSDT"],
                    "candidate_symbol": "BTCUSDT",
                },
                "entry": {
                    "triggers": ["vol_shock_relaxation_event"],
                    "conditions": ["all"],
                    "confirmations": [],
                    "delay_bars": 0,
                    "cooldown_bars": 0,
                    "condition_logic": "all",
                    "condition_nodes": [],
                    "arm_bars": 0,
                    "reentry_lockout_bars": 0,
                },
                "overlays": [],
                "lineage": {
                    "events_count_used_for_gate": 180,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    rows = _run_builder(
        monkeypatch,
        tmp_path,
        [
            "build_strategy_candidates",
            "--run_id",
            "r1",
            "--symbols",
            "BTCUSDT",
            "--blueprints_file",
            str(blueprint_dir / "blueprints.jsonl"),
            "--include_alpha_bundle",
            "0",
        ],
    )

    assert rows == []
