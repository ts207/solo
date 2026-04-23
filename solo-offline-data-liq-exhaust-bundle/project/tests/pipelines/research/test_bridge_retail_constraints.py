from __future__ import annotations

from pathlib import Path

import pandas as pd

from project.engine.execution_model import load_calibration_config
from project.research.bridge_evaluate_phase2 import (
    _resolve_bridge_policy,
    _build_bridge_symbol_calibrations,
    _evaluate_bridge_row,
    _load_symbol_calibrated_cost_bps,
    _write_bridge_symbol_calibrations,
)
from project.research.bridge_evaluation import evaluate_bridge_performance
from project.research.helpers.viability import evaluate_retail_constraints


def test_evaluate_retail_constraints_falls_back_to_after_cost_expectancy():
    out = evaluate_retail_constraints(
        {
            "after_cost_expectancy_per_trade": 0.0006,
            "avg_dynamic_cost_bps": 6.0,
            "turnover_proxy_mean": 2.0,
            "tob_coverage": 0.9,
        },
        min_tob_coverage=0.8,
        min_net_expectancy_bps=4.0,
        max_fee_plus_slippage_bps=10.0,
        max_daily_turnover_multiple=4.0,
    )
    assert abs(float(out["net_expectancy_bps"]) - 6.0) < 1e-9
    assert out["gate_tob_coverage"] is True
    assert out["gate_net_expectancy"] is True
    assert out["gate_cost_budget"] is True
    assert out["gate_turnover"] is True
    assert out["gate_retail_viability"] is True


def test_bridge_row_applies_retail_gate_when_required():
    row = pd.Series(
        {
            "candidate_id": "cand_1",
            "candidate_type": "standalone",
            "overlay_base_candidate_id": "",
            "bridge_validation_after_cost_bps": 1.0,
            "bridge_validation_stressed_after_cost_bps": 0.5,
            "bridge_train_after_cost_bps": 1.2,
            "validation_samples": 80,
            "turnover_proxy_mean": 0.8,
            "avg_dynamic_cost_bps": 10.0,
            "effective_lag_bars": 1,
            "tob_coverage": 0.9,
        }
    )
    result, overlay_delta = _evaluate_bridge_row(
        row,
        event_type="VOL_SHOCK",
        base_lookup={},
        edge_cost_k=1.0,
        min_validation_trades=10,
        stressed_cost_multiplier=1.5,
        min_net_expectancy_bps=4.0,
        max_fee_plus_slippage_bps=10.0,
        max_daily_turnover_multiple=4.0,
        require_retail_viability=True,
        micro_max_spread_stress=2.0,
        micro_max_depth_depletion=0.7,
        micro_max_sweep_pressure=2.5,
        micro_max_abs_imbalance=0.9,
        micro_min_feature_coverage=0.25,
    )
    assert overlay_delta is None
    assert result["gate_bridge_retail_net_expectancy"] is False
    assert result["gate_bridge_retail_cost_budget"] is True
    assert result["gate_bridge_retail_turnover"] is True
    assert result["gate_bridge_retail_viability"] is False
    assert result["gate_bridge_tradable"] is False
    assert "gate_bridge_retail_net_expectancy" in str(result["bridge_fail_reasons"])


def test_bridge_row_keeps_retail_gate_soft_when_not_required():
    row = pd.Series(
        {
            "candidate_id": "cand_2",
            "candidate_type": "standalone",
            "overlay_base_candidate_id": "",
            "bridge_validation_after_cost_bps": 1.0,
            "bridge_validation_stressed_after_cost_bps": 0.5,
            "bridge_train_after_cost_bps": 1.2,
            "validation_samples": 80,
            "turnover_proxy_mean": 0.8,
            "avg_dynamic_cost_bps": 10.0,
            "effective_lag_bars": 1,
            "tob_coverage": 0.9,
        }
    )
    result, overlay_delta = _evaluate_bridge_row(
        row,
        event_type="VOL_SHOCK",
        base_lookup={},
        edge_cost_k=1.0,
        min_validation_trades=10,
        stressed_cost_multiplier=1.5,
        min_net_expectancy_bps=4.0,
        max_fee_plus_slippage_bps=10.0,
        max_daily_turnover_multiple=4.0,
        require_retail_viability=False,
        micro_max_spread_stress=2.0,
        micro_max_depth_depletion=0.7,
        micro_max_sweep_pressure=2.5,
        micro_max_abs_imbalance=0.9,
        micro_min_feature_coverage=0.25,
    )
    assert overlay_delta is None
    assert result["gate_bridge_retail_net_expectancy"] is False
    assert result["gate_bridge_tradable"] is True


def test_evaluate_bridge_performance_uses_computed_bridge_metrics_for_retail_constraints():
    survivors = pd.DataFrame(
        [
            {
                "candidate_id": "cand_bridge_metrics",
                "symbol": "BTCUSDT",
                "candidate_type": "standalone",
                "expectancy": 0.0013,
                "sample_size": 120,
                "avg_dynamic_cost_bps": 0.6,
                "turnover_proxy_mean": 1.0,
                "tob_coverage": 0.9,
                "effective_lag_bars": 1,
            }
        ]
    )
    rows, overlays = evaluate_bridge_performance(
        survivors,
        event_type="VOL_SHOCK",
        base_lookup={},
        edge_cost_k=1.0,
        min_validation_trades=20,
        stressed_cost_multiplier=1.5,
        min_net_expectancy_bps=4.0,
        max_fee_plus_slippage_bps=10.0,
        max_daily_turnover_multiple=4.0,
        require_retail_viability=True,
        micro_thresholds={
            "max_spread_stress": 2.0,
            "max_depth_depletion": 0.7,
            "max_sweep_pressure": 2.5,
            "max_abs_imbalance": 0.9,
            "min_feature_coverage": 0.25,
        },
    )
    assert overlays == []
    assert len(rows) == 1
    out = rows[0]
    assert out["gate_bridge_retail_net_expectancy"] is True
    assert out["gate_bridge_retail_cost_budget"] is True
    assert out["gate_bridge_retail_turnover"] is True
    assert out["gate_bridge_retail_viability"] is True
    assert out["gate_bridge_tradable"] is True
    assert abs(float(out["tob_coverage"]) - 0.9) < 1e-9
    assert abs(float(out["turnover_proxy_mean"]) - 1.0) < 1e-9


def test_evaluate_bridge_performance_exports_default_turnover_proxy_when_missing():
    survivors = pd.DataFrame(
        [
            {
                "candidate_id": "cand_bridge_turnover_default",
                "symbol": "BTCUSDT",
                "candidate_type": "standalone",
                "expectancy": 0.0013,
                "sample_size": 120,
                "avg_dynamic_cost_bps": 0.6,
                "effective_lag_bars": 1,
            }
        ]
    )
    rows, _ = evaluate_bridge_performance(
        survivors,
        event_type="VOL_SHOCK",
        base_lookup={},
        edge_cost_k=1.0,
        min_validation_trades=20,
        stressed_cost_multiplier=1.5,
        min_net_expectancy_bps=4.0,
        max_fee_plus_slippage_bps=10.0,
        max_daily_turnover_multiple=4.0,
        require_retail_viability=True,
        micro_thresholds={
            "max_spread_stress": 2.0,
            "max_depth_depletion": 0.7,
            "max_sweep_pressure": 2.5,
            "max_abs_imbalance": 0.9,
            "min_feature_coverage": 0.25,
        },
    )
    out = rows[0]
    assert abs(float(out["turnover_proxy_mean"]) - 0.5) < 1e-9
    assert out["gate_bridge_retail_turnover"] is True


def test_bridge_row_flags_microstructure_risk_gate():
    row = pd.Series(
        {
            "candidate_id": "cand_micro",
            "candidate_type": "standalone",
            "bridge_validation_after_cost_bps": 3.0,
            "bridge_validation_stressed_after_cost_bps": 2.0,
            "bridge_train_after_cost_bps": 3.1,
            "validation_samples": 80,
            "turnover_proxy_mean": 0.8,
            "avg_dynamic_cost_bps": 5.0,
            "effective_lag_bars": 1,
            "micro_spread_stress": 5.0,
            "micro_depth_depletion": 0.2,
            "micro_sweep_pressure": 1.0,
            "micro_abs_imbalance": 0.2,
            "micro_feature_coverage": 1.0,
        }
    )
    result, _ = _evaluate_bridge_row(
        row,
        event_type="VOL_SHOCK",
        base_lookup={},
        edge_cost_k=1.0,
        min_validation_trades=10,
        stressed_cost_multiplier=1.5,
        min_net_expectancy_bps=0.0,
        max_fee_plus_slippage_bps=20.0,
        max_daily_turnover_multiple=8.0,
        require_retail_viability=False,
        micro_max_spread_stress=2.0,
        micro_max_depth_depletion=0.7,
        micro_max_sweep_pressure=2.5,
        micro_max_abs_imbalance=0.9,
        micro_min_feature_coverage=0.25,
    )
    assert result["gate_bridge_microstructure"] is False
    assert result["gate_bridge_tradable_without_microstructure"] is True
    assert result["gate_bridge_tradable"] is False
    assert "gate_bridge_micro_spread_stress" in str(result["bridge_fail_reasons"])


def test_bridge_row_enforces_low_capital_viability_when_enabled():
    row = pd.Series(
        {
            "candidate_id": "cand_low_cap",
            "candidate_type": "standalone",
            "bridge_validation_after_cost_bps": 1.0,
            "bridge_validation_stressed_after_cost_bps": 0.1,
            "bridge_train_after_cost_bps": 1.1,
            "validation_samples": 50,
            "turnover_proxy_mean": 3.0,
            "avg_dynamic_cost_bps": 4.0,
            "effective_lag_bars": 1,
            "tob_coverage": 0.9,
            "micro_spread_stress": 3.0,
            "micro_feature_coverage": 0.9,
        }
    )
    result, _ = _evaluate_bridge_row(
        row,
        event_type="VOL_SHOCK",
        base_lookup={},
        edge_cost_k=0.5,
        min_validation_trades=10,
        stressed_cost_multiplier=1.5,
        min_net_expectancy_bps=0.0,
        max_fee_plus_slippage_bps=20.0,
        max_daily_turnover_multiple=8.0,
        require_retail_viability=False,
        micro_max_spread_stress=5.0,
        micro_max_depth_depletion=0.7,
        micro_max_sweep_pressure=2.5,
        micro_max_abs_imbalance=0.9,
        micro_min_feature_coverage=0.25,
        low_capital_contract={
            "account_equity_usd": 10000,
            "max_position_notional_usd": 2000,
            "min_position_notional_usd": 50,
            "max_leverage": 3,
            "max_trades_per_day": 20,
            "max_turnover_per_day": 2.0,
            "fee_tier": "taker",
            "slippage_model_baseline_bps": 6.0,
            "stress_cost_multiplier_2x": 2.0,
            "stress_cost_multiplier_3x": 3.0,
            "spread_model": "top_book_bps",
            "entry_delay_bars_default": 1,
            "entry_delay_bars_stress": 2,
            "max_drawdown_pct": 0.2,
            "max_daily_loss_pct": 0.05,
            "stop_trading_rule": "daily_loss_breach",
            "bar_timestamp_semantics": "open_time",
            "signal_snap_side": "left",
            "active_range_semantics": "[start,end)",
            "require_top_book_coverage": 0.8,
            "spread_ceiling_bps": 2.0,
        },
        enforce_low_capital_viability=True,
    )
    assert result["gate_bridge_low_capital_viability"] is False
    assert result["gate_bridge_tradable"] is False
    assert "gate_bridge_low_capital_viability" in str(result["bridge_fail_reasons"])
    assert abs(float(result["low_capital_estimated_position_notional_usd"]) - 1500.0) < 1e-9
    assert abs(float(result["low_capital_required_min_notional_usd"]) - 50.0) < 1e-9
    assert abs(float(result["low_capital_min_order_ratio"]) - 30.0) < 1e-9
    assert result["low_capital_estimated_position_notional_source"] == "turnover_implied"


def test_bridge_row_low_capital_viability_handles_missing_tob_coverage():
    row = pd.Series(
        {
            "candidate_id": "cand_missing_tob",
            "candidate_type": "standalone",
            "bridge_validation_after_cost_bps": 6.0,
            "bridge_validation_stressed_after_cost_bps": 5.0,
            "bridge_train_after_cost_bps": 6.2,
            "validation_samples": 80,
            "turnover_proxy_mean": 1.0,
            "avg_dynamic_cost_bps": 1.0,
            "effective_lag_bars": 1,
            "micro_feature_coverage": 0.9,
            "micro_spread_stress": 1.0,
        }
    )
    result, _ = _evaluate_bridge_row(
        row,
        event_type="VOL_SHOCK",
        base_lookup={},
        edge_cost_k=0.5,
        min_validation_trades=10,
        stressed_cost_multiplier=1.5,
        min_net_expectancy_bps=0.0,
        max_fee_plus_slippage_bps=20.0,
        max_daily_turnover_multiple=8.0,
        require_retail_viability=False,
        micro_max_spread_stress=5.0,
        micro_max_depth_depletion=0.7,
        micro_max_sweep_pressure=2.5,
        micro_max_abs_imbalance=0.9,
        micro_min_feature_coverage=0.25,
        low_capital_contract={
            "account_equity_usd": 10000,
            "max_position_notional_usd": 2000,
            "min_position_notional_usd": 50,
            "max_leverage": 3,
            "max_trades_per_day": 20,
            "max_turnover_per_day": 2.0,
            "fee_tier": "taker",
            "slippage_model_baseline_bps": 6.0,
            "stress_cost_multiplier_2x": 2.0,
            "stress_cost_multiplier_3x": 3.0,
            "spread_model": "top_book_bps",
            "entry_delay_bars_default": 1,
            "entry_delay_bars_stress": 2,
            "max_drawdown_pct": 0.2,
            "max_daily_loss_pct": 0.05,
            "stop_trading_rule": "daily_loss_breach",
            "bar_timestamp_semantics": "open_time",
            "signal_snap_side": "left",
            "active_range_semantics": "[start,end)",
            "max_holding_bars": 48,
            "require_top_book_coverage": 0.8,
            "spread_ceiling_bps": 2.0,
        },
        enforce_low_capital_viability=True,
    )
    assert result["gate_bridge_low_capital_viability"] is True


def test_bridge_row_low_capital_liquidity_sanity_does_not_fail_closed_without_l2():
    row = pd.Series(
        {
            "candidate_id": "cand_no_l2",
            "candidate_type": "standalone",
            "bridge_validation_after_cost_bps": 6.0,
            "bridge_validation_stressed_after_cost_bps": 5.0,
            "bridge_train_after_cost_bps": 6.2,
            "validation_samples": 80,
            "turnover_proxy_mean": 0.5,
            "avg_dynamic_cost_bps": 1.0,
            "effective_lag_bars": 1,
            "micro_spread_stress": 1.0,
        }
    )
    result, _ = _evaluate_bridge_row(
        row,
        event_type="VOL_SHOCK",
        base_lookup={},
        edge_cost_k=0.5,
        min_validation_trades=10,
        stressed_cost_multiplier=1.5,
        min_net_expectancy_bps=4.0,
        max_fee_plus_slippage_bps=10.0,
        max_daily_turnover_multiple=4.0,
        require_retail_viability=True,
        micro_max_spread_stress=5.0,
        micro_max_depth_depletion=0.7,
        micro_max_sweep_pressure=2.5,
        micro_max_abs_imbalance=0.9,
        micro_min_feature_coverage=0.25,
        low_capital_contract={
            "account_equity_usd": 10000,
            "max_position_notional_usd": 2000,
            "min_position_notional_usd": 50,
            "max_leverage": 3,
            "max_trades_per_day": 20,
            "max_turnover_per_day": 2.0,
            "fee_tier": "taker",
            "slippage_model_baseline_bps": 6.0,
            "stress_cost_multiplier_2x": 2.0,
            "stress_cost_multiplier_3x": 3.0,
            "spread_model": "top_book_bps",
            "entry_delay_bars_default": 1,
            "entry_delay_bars_stress": 2,
            "max_drawdown_pct": 0.2,
            "max_daily_loss_pct": 0.05,
            "stop_trading_rule": "daily_loss_breach",
            "bar_timestamp_semantics": "open_time",
            "signal_snap_side": "left",
            "active_range_semantics": "[start,end)",
            "max_holding_bars": 48,
            "require_top_book_coverage": 0.8,
            "spread_ceiling_bps": 8.0,
        },
        enforce_low_capital_viability=True,
    )
    assert result["gate_bridge_retail_viability"] is True
    assert result["gate_bridge_low_capital_viability"] is True


def test_bridge_row_low_capital_liquidity_sanity_allows_missing_spread_without_l2():
    row = pd.Series(
        {
            "candidate_id": "cand_no_l2_no_spread",
            "candidate_type": "standalone",
            "bridge_validation_after_cost_bps": 6.0,
            "bridge_validation_stressed_after_cost_bps": 5.0,
            "bridge_train_after_cost_bps": 6.2,
            "validation_samples": 80,
            "turnover_proxy_mean": 0.5,
            "avg_dynamic_cost_bps": 1.0,
            "effective_lag_bars": 1,
        }
    )
    result, _ = _evaluate_bridge_row(
        row,
        event_type="VOL_SHOCK",
        base_lookup={},
        edge_cost_k=0.5,
        min_validation_trades=10,
        stressed_cost_multiplier=1.5,
        min_net_expectancy_bps=4.0,
        max_fee_plus_slippage_bps=10.0,
        max_daily_turnover_multiple=4.0,
        require_retail_viability=True,
        micro_max_spread_stress=5.0,
        micro_max_depth_depletion=0.7,
        micro_max_sweep_pressure=2.5,
        micro_max_abs_imbalance=0.9,
        micro_min_feature_coverage=0.25,
        low_capital_contract={
            "account_equity_usd": 10000,
            "max_position_notional_usd": 2000,
            "min_position_notional_usd": 50,
            "max_leverage": 3,
            "max_trades_per_day": 20,
            "max_turnover_per_day": 2.0,
            "fee_tier": "taker",
            "slippage_model_baseline_bps": 6.0,
            "stress_cost_multiplier_2x": 2.0,
            "stress_cost_multiplier_3x": 3.0,
            "spread_model": "top_book_bps",
            "entry_delay_bars_default": 1,
            "entry_delay_bars_stress": 2,
            "max_drawdown_pct": 0.2,
            "max_daily_loss_pct": 0.05,
            "stop_trading_rule": "daily_loss_breach",
            "bar_timestamp_semantics": "open_time",
            "signal_snap_side": "left",
            "active_range_semantics": "[start,end)",
            "max_holding_bars": 48,
            "require_top_book_coverage": 0.8,
            "spread_ceiling_bps": 8.0,
        },
        enforce_low_capital_viability=True,
    )
    assert result["gate_bridge_retail_viability"] is True
    assert result["gate_bridge_low_capital_viability"] is True


def test_bridge_symbol_cost_calibration_round_trip(tmp_path: Path):
    metrics_df = pd.DataFrame(
        [
            {"symbol": "BTCUSDT", "bridge_effective_cost_bps_per_trade": 9.0},
            {"symbol": "BTCUSDT", "bridge_effective_cost_bps_per_trade": 11.0},
            {"symbol": "ETHUSDT", "bridge_effective_cost_bps_per_trade": 7.0},
        ]
    )
    calibrations = _build_bridge_symbol_calibrations(
        metrics_df=metrics_df,
        base_fee_bps=4.0,
        min_tob_coverage=0.8,
    )
    assert sorted(calibrations.keys()) == ["BTCUSDT", "ETHUSDT"]
    assert calibrations["BTCUSDT"]["base_fee_bps"] == 4.0
    assert calibrations["BTCUSDT"]["base_slippage_bps"] == 6.0

    written = _write_bridge_symbol_calibrations(
        calibrations=calibrations,
        calibration_dir=tmp_path,
    )
    assert len(written) == 2
    loaded = _load_symbol_calibrated_cost_bps(symbol="BTCUSDT", calibration_dir=tmp_path)
    assert loaded is not None
    assert abs(float(loaded) - 10.0) < 1e-9
    merged_cfg = load_calibration_config(
        "BTCUSDT",
        calibration_dir=tmp_path,
        base_config={"cost_model": "dynamic", "base_fee_bps": 1.0, "base_slippage_bps": 1.0},
    )
    assert abs(float(merged_cfg["base_fee_bps"]) - 4.0) < 1e-9
    assert abs(float(merged_cfg["base_slippage_bps"]) - 6.0) < 1e-9


def test_resolve_bridge_policy_uses_objective_contract(tmp_path: Path):
    data_root = tmp_path / "data"
    (data_root / "runs" / "bridge_policy_case").mkdir(parents=True, exist_ok=True)

    class Args:
        run_id = "bridge_policy_case"
        objective_name = "retail_profitability"
        objective_spec = None
        retail_profile = ""
        retail_profiles_spec = None

    policy = _resolve_bridge_policy(Args(), data_root)

    assert float(policy["min_net_expectancy_bps"]) == 4.0
    assert float(policy["max_fee_plus_slippage_bps"]) == 10.0
    assert float(policy["max_daily_turnover_multiple"]) == 4.0
    assert policy["require_retail_viability"] is True
    assert policy["enforce_low_capital_viability"] is True
    assert float(policy["low_capital_contract"]["max_position_notional_usd"]) == 15000.0
