from __future__ import annotations

import json
import sys
from types import SimpleNamespace

import pandas as pd

from project.research import select_profitable_strategies as stage


def test_select_profitable_strategies_filters_candidates(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    run_id = "select_profitable_test"
    promotions_dir = data_root / "reports" / "promotions" / run_id
    promotions_dir.mkdir(parents=True, exist_ok=True)

    promoted = pd.DataFrame(
        [
            {
                "status": "PROMOTED",
                "event_type": "VOL_SHOCK",
                "candidate_id": "good_1",
                "gate_promo_statistical": "pass",
                "gate_promo_stability": "pass",
                "gate_promo_cost_survival": "pass",
                "gate_promo_negative_control": "pass",
                "gate_promo_oos_validation": "pass",
                "gate_promo_microstructure": "pass",
                "gate_promo_retail_viability": "pass",
                "gate_promo_low_capital_viability": "pass",
                "bridge_validation_stressed_after_cost_bps": 12.0,
                "oos_sign_consistency": 0.9,
                "n_events": 500,
                "selection_score": 0.8,
            },
            {
                "status": "PROMOTED",
                "event_type": "VOL_SHOCK",
                "candidate_id": "bad_1",
                "gate_promo_statistical": "pass",
                "gate_promo_stability": "pass",
                "gate_promo_cost_survival": "pass",
                "gate_promo_negative_control": "pass",
                "gate_promo_oos_validation": "pass",
                "gate_promo_microstructure": "pass",
                "gate_promo_retail_viability": "pass",
                "gate_promo_low_capital_viability": "pass",
                "bridge_validation_stressed_after_cost_bps": -4.0,
                "oos_sign_consistency": 0.5,
                "n_events": 500,
                "selection_score": 0.7,
            },
        ]
    )
    promoted.to_parquet(promotions_dir / "promoted_candidates.parquet", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))
    monkeypatch.setattr(stage, "get_data_root", lambda: data_root)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "select_profitable_strategies.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--min_expectancy_bps",
            "0.0",
            "--min_events",
            "100",
        ],
    )

    rc = stage.main()
    assert rc == 0

    out_path = (
        data_root / "reports" / "strategy_selection" / run_id / "profitable_strategies.parquet"
    )
    assert out_path.exists()
    selected = pd.read_parquet(out_path)
    assert selected["candidate_id"].tolist() == ["good_1"]


def test_select_profitable_strategies_prefers_strategy_candidates_artifact(monkeypatch, tmp_path):
    data_root = tmp_path / "data"
    run_id = "select_strategy_candidates_test"
    strategy_dir = data_root / "reports" / "strategy_builder" / run_id
    strategy_dir.mkdir(parents=True, exist_ok=True)
    promotions_dir = data_root / "reports" / "promotions" / run_id
    promotions_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "strategy_candidate_id": "strat_good",
                "candidate_id": "good_strategy",
                "status": "PROMOTED",
                "event_type": "VOL_SHOCK",
                "selection_score": 0.8,
                "expectancy_after_multiplicity": 0.0012,
                "oos_sign_consistency": 0.9,
                "n_events": 300,
                "executable_condition": True,
                "executable_action": True,
                "allocation_policy": json.dumps({"allocation_viable": True}),
            },
            {
                "strategy_candidate_id": "strat_bad",
                "candidate_id": "bad_strategy",
                "status": "PROMOTED",
                "event_type": "VOL_SHOCK",
                "selection_score": 0.3,
                "expectancy_after_multiplicity": -0.0002,
                "oos_sign_consistency": 0.2,
                "n_events": 300,
                "executable_condition": True,
                "executable_action": True,
                "allocation_policy": json.dumps({"allocation_viable": True}),
            },
        ]
    ).to_parquet(strategy_dir / "strategy_candidates.parquet", index=False)

    pd.DataFrame(
        [
            {
                "status": "PROMOTED",
                "event_type": "VOL_SHOCK",
                "candidate_id": "legacy_promoted",
                "gate_promo_statistical": "pass",
                "gate_promo_stability": "pass",
                "gate_promo_cost_survival": "pass",
                "gate_promo_negative_control": "pass",
                "gate_promo_oos_validation": "pass",
                "gate_promo_microstructure": "pass",
                "gate_promo_retail_viability": "pass",
                "gate_promo_low_capital_viability": "pass",
                "bridge_validation_stressed_after_cost_bps": 20.0,
                "oos_sign_consistency": 0.9,
                "n_events": 500,
                "selection_score": 0.9,
            }
        ]
    ).to_parquet(promotions_dir / "promoted_candidates.parquet", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))
    monkeypatch.setattr(stage, "get_data_root", lambda: data_root)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "select_profitable_strategies.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
            "--min_expectancy_bps",
            "5.0",
            "--min_events",
            "100",
        ],
    )

    rc = stage.main()
    assert rc == 0

    out_path = (
        data_root / "reports" / "strategy_selection" / run_id / "profitable_strategies.parquet"
    )
    selected = pd.read_parquet(out_path)
    assert selected["candidate_id"].tolist() == ["good_strategy"]


def test_select_profitable_strategies_enforces_oos_consistency_for_strategy_candidates(
    monkeypatch, tmp_path
):
    data_root = tmp_path / "data"
    run_id = "select_strategy_candidates_oos_gate"
    strategy_dir = data_root / "reports" / "strategy_builder" / run_id
    strategy_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "strategy_candidate_id": "strat_good",
                "candidate_id": "good_strategy",
                "status": "PROMOTED",
                "event_type": "VOL_SHOCK",
                "selection_score": 0.8,
                "expectancy_after_multiplicity": 0.0012,
                "oos_sign_consistency": 0.9,
                "n_events": 300,
                "executable_condition": True,
                "executable_action": True,
                "allocation_policy": json.dumps({"allocation_viable": True}),
            },
            {
                "strategy_candidate_id": "strat_missing_oos",
                "candidate_id": "missing_oos_strategy",
                "status": "PROMOTED",
                "event_type": "VOL_SHOCK",
                "selection_score": 0.75,
                "expectancy_after_multiplicity": 0.0011,
                "n_events": 300,
                "executable_condition": True,
                "executable_action": True,
                "allocation_policy": json.dumps({"allocation_viable": True}),
            },
            {
                "strategy_candidate_id": "strat_bad_oos",
                "candidate_id": "bad_oos_strategy",
                "status": "PROMOTED",
                "event_type": "VOL_SHOCK",
                "selection_score": 0.7,
                "expectancy_after_multiplicity": 0.0010,
                "oos_sign_consistency": 0.4,
                "n_events": 300,
                "executable_condition": True,
                "executable_action": True,
                "allocation_policy": json.dumps({"allocation_viable": True}),
            },
        ]
    ).to_parquet(strategy_dir / "strategy_candidates.parquet", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))
    monkeypatch.setattr(stage, "get_data_root", lambda: data_root)
    monkeypatch.setattr(
        stage,
        "resolve_objective_profile_contract",
        lambda **kwargs: SimpleNamespace(
            min_net_expectancy_bps=5.0,
            min_trade_count=100,
            min_oos_sign_consistency=0.75,
            require_retail_viability=False,
            require_low_capital_contract=False,
            objective_name="test_objective",
            retail_profile_name="test_profile",
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "select_profitable_strategies.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
        ],
    )

    rc = stage.main()
    assert rc == 0

    out_path = (
        data_root / "reports" / "strategy_selection" / run_id / "profitable_strategies.parquet"
    )
    selected = pd.read_parquet(out_path)
    assert selected["candidate_id"].tolist() == ["good_strategy"]


def test_select_profitable_strategies_writes_empty_outputs_for_empty_strategy_candidates(
    monkeypatch, tmp_path
):
    data_root = tmp_path / "data"
    run_id = "select_strategy_candidates_empty"
    strategy_dir = data_root / "reports" / "strategy_builder" / run_id
    strategy_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        columns=[
            "strategy_candidate_id",
            "candidate_id",
            "status",
            "event_type",
            "selection_score",
            "expectancy_after_multiplicity",
            "oos_sign_consistency",
            "n_events",
            "executable_condition",
            "executable_action",
            "allocation_policy",
        ]
    ).to_parquet(strategy_dir / "strategy_candidates.parquet", index=False)

    monkeypatch.setenv("BACKTEST_DATA_ROOT", str(data_root))
    monkeypatch.setattr(stage, "get_data_root", lambda: data_root)
    monkeypatch.setattr(
        stage,
        "resolve_objective_profile_contract",
        lambda **kwargs: SimpleNamespace(
            min_net_expectancy_bps=5.0,
            min_trade_count=100,
            min_oos_sign_consistency=0.75,
            require_retail_viability=False,
            require_low_capital_contract=False,
            objective_name="test_objective",
            retail_profile_name="test_profile",
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "select_profitable_strategies.py",
            "--run_id",
            run_id,
            "--symbols",
            "BTCUSDT",
        ],
    )

    rc = stage.main()
    assert rc == 0

    out_dir = data_root / "reports" / "strategy_selection" / run_id
    out_path = out_dir / "profitable_strategies.parquet"
    summary_path = out_dir / "profitability_summary.json"

    selected = pd.read_parquet(out_path)
    assert selected.empty

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["selected_count"] == 0
    assert summary["no_candidates_found"] is True
