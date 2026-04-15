from __future__ import annotations

import pandas as pd
import pytest

from project.compilers.executable_strategy_spec import ExecutableStrategySpec
from project.strategy.runtime.dsl_interpreter_v1 import DslInterpreterV1, _build_blueprint


def _base_blueprint() -> dict:
    return {
        "id": "bp_test",
        "run_id": "r1",
        "event_type": "VOL_SHOCK",
        "candidate_id": "cand_1",
        "symbol_scope": {
            "mode": "single_symbol",
            "symbols": ["BTCUSDT"],
            "candidate_symbol": "BTCUSDT",
        },
        "direction": "long",
        "entry": {
            "triggers": ["spread_guard_pass"],
            "conditions": ["all"],
            "confirmations": [],
            "delay_bars": 0,
            "cooldown_bars": 0,
            "condition_logic": "all",
            "condition_nodes": [],
            "arm_bars": 0,
            "reentry_lockout_bars": 0,
        },
        "exit": {
            "time_stop_bars": 10,
            "invalidation": {"metric": "close", "operator": ">", "value": 10_000.0},
            "stop_type": "percent",
            "stop_value": 0.01,
            "target_type": "percent",
            "target_value": 0.02,
            "trailing_stop_type": "none",
            "trailing_stop_value": 0.0,
            "break_even_r": 0.0,
        },
        "sizing": {
            "mode": "fixed_risk",
            "risk_per_trade": 0.01,
            "target_vol": None,
            "max_gross_leverage": 1.0,
            "max_position_scale": 1.0,
            "portfolio_risk_budget": 1.0,
            "symbol_risk_budget": 1.0,
        },
        "overlays": [],
        "evaluation": {
            "min_trades": 1,
            "cost_model": {"fees_bps": 2.0, "slippage_bps": 2.0, "funding_included": True},
            "robustness_flags": {
                "oos_required": True,
                "multiplicity_required": True,
                "regime_stability_required": True,
            },
        },
        "lineage": {
            "source_path": "dummy",
            "compiler_version": "v1",
            "generated_at_utc": "2026-01-01T00:00:00Z",
        },
    }


def _base_features(bars: pd.DataFrame) -> pd.DataFrame:
    # Preserve timestamp for the merge join in DslInterpreterV1
    return (
        bars[["timestamp", "close", "quote_volume"]]
        .assign(
            funding_rate_scaled=0.0001,
            spread_bps=1.0,  # 1bp << 12bps threshold for spread_guard_pass
        )
        .copy()
    )


def test_blueprint_feature_reference_contract_rejects_disallowed_condition_feature():
    bp = _base_blueprint()
    bp["entry"]["condition_nodes"] = [
        {
            "feature": "forward_return_h",
            "operator": ">",
            "value": 0.0,
            "lookback_bars": 0,
            "window_bars": 0,
        }
    ]
    with pytest.raises(ValueError, match="Disallowed feature reference"):
        _build_blueprint(bp)


def test_dsl_enforces_minimum_one_bar_decision_lag():
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                ],
                utc=True,
            ),
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
            "volume": [10.0, 10.0, 10.0],
            "quote_volume": [1_000_000.0, 1_000_000.0, 1_000_000.0],
        }
    )
    features = _base_features(bars)

    strategy = DslInterpreterV1()
    positions = strategy.generate_positions(
        bars=bars,
        features=features,
        params={
            "strategy_symbol": "BTCUSDT",
            "dsl_blueprint": _base_blueprint(),
        },
    )

    assert int(positions.iloc[0]) == 0
    assert int(positions.iloc[1]) in {0, 1}
    assert int(positions.iloc[1]) == 1


def test_dsl_accepts_registry_backed_signal_without_hardcoded_allowlist_entry():
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                ],
                utc=True,
            ),
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
            "volume": [10.0, 10.0, 10.0],
            "quote_volume": [1_000_000.0, 1_000_000.0, 1_000_000.0],
        }
    )
    features = _base_features(bars)
    # "oi_flush_event" comes from registry-backed canonical spec signal names.
    features["oi_flush_event"] = [True, True, False]

    blueprint = _base_blueprint()
    blueprint["entry"]["triggers"] = ["oi_flush_event"]
    blueprint["entry"]["confirmations"] = []

    strategy = DslInterpreterV1()
    positions = strategy.generate_positions(
        bars=bars,
        features=features,
        params={
            "strategy_symbol": "BTCUSDT",
            "dsl_blueprint": blueprint,
        },
    )

    assert int(positions.iloc[0]) == 0
    assert int(positions.iloc[1]) == 1


def test_dsl_rejects_unknown_non_registry_signal():
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-01-01T00:00:00Z", "2026-01-01T00:05:00Z"],
                utc=True,
            ),
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.0, 101.0],
            "volume": [10.0, 10.0],
            "quote_volume": [1_000_000.0, 1_000_000.0],
        }
    )
    features = _base_features(bars)
    features["not_a_real_signal"] = [True, True]

    blueprint = _base_blueprint()
    blueprint["entry"]["triggers"] = ["not_a_real_signal"]
    blueprint["entry"]["confirmations"] = []

    strategy = DslInterpreterV1()
    with pytest.raises(ValueError, match="unknown trigger signals"):
        strategy.generate_positions(
            bars=bars,
            features=features,
            params={
                "strategy_symbol": "BTCUSDT",
                "dsl_blueprint": blueprint,
            },
        )


def test_dsl_runtime_allows_non_funding_strategies_without_funding_column():
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                ],
                utc=True,
            ),
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
            "volume": [10.0, 10.0, 10.0],
            "quote_volume": [1_000_000.0, 1_000_000.0, 1_000_000.0],
        }
    )
    features = bars[["timestamp", "close", "quote_volume"]].assign(spread_bps=1.0).copy()

    strategy = DslInterpreterV1()
    positions = strategy.generate_positions(
        bars=bars,
        features=features,
        params={
            "strategy_symbol": "BTCUSDT",
            "dsl_blueprint": _base_blueprint(),
        },
    )

    assert len(positions) == len(bars)
    assert int(positions.iloc[0]) == 0


def test_dsl_runtime_rejects_funding_guard_without_canonical_funding_column():
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                ],
                utc=True,
            ),
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
            "volume": [10.0, 10.0, 10.0],
            "quote_volume": [1_000_000.0, 1_000_000.0, 1_000_000.0],
        }
    )
    features = bars[["timestamp", "close", "quote_volume"]].assign(spread_bps=1.0).copy()
    blueprint = _base_blueprint()
    blueprint["entry"]["confirmations"] = ["funding_normalization_pass"]
    blueprint["overlays"] = [{"name": "funding_guard", "params": {"max_abs_funding_bps": 12.0}}]

    strategy = DslInterpreterV1()
    with pytest.raises(ValueError, match="requires canonical funding_rate_scaled"):
        strategy.generate_positions(
            bars=bars,
            features=features,
            params={
                "strategy_symbol": "BTCUSDT",
                "dsl_blueprint": blueprint,
            },
        )


def test_dsl_enforces_fail_on_zero_trigger_coverage():
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                ],
                utc=True,
            ),
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
            "volume": [10.0, 10.0, 10.0],
            "quote_volume": [1_000_000.0, 1_000_000.0, 1_000_000.0],
        }
    )
    features = _base_features(bars)
    features["liquidity_vacuum_event"] = [False, False, False]

    blueprint = _base_blueprint()
    blueprint["entry"]["triggers"] = ["liquidity_vacuum_event"]
    blueprint["entry"]["confirmations"] = []

    strategy = DslInterpreterV1()
    with pytest.raises(ValueError, match="all-zero trigger coverage"):
        strategy.generate_positions(
            bars=bars,
            features=features,
            params={
                "strategy_symbol": "BTCUSDT",
                "dsl_blueprint": blueprint,
                "fail_on_zero_trigger_coverage": 1,
            },
        )


def test_dsl_emits_trigger_coverage_in_metadata():
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                ],
                utc=True,
            ),
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
            "volume": [10.0, 10.0, 10.0],
            "quote_volume": [1_000_000.0, 1_000_000.0, 1_000_000.0],
        }
    )
    features = _base_features(bars)
    features["liquidity_vacuum_event"] = [True, False, False]

    blueprint = _base_blueprint()
    blueprint["entry"]["triggers"] = ["liquidity_vacuum_event"]
    blueprint["entry"]["confirmations"] = []

    strategy = DslInterpreterV1()
    positions = strategy.generate_positions(
        bars=bars,
        features=features,
        params={
            "strategy_symbol": "BTCUSDT",
            "dsl_blueprint": blueprint,
        },
    )
    metadata = positions.attrs["strategy_metadata"]
    coverage = metadata["trigger_coverage"]

    assert coverage["all_zero"] is False
    assert coverage["missing"] == []
    assert coverage["triggers"]["liquidity_vacuum_event"]["true_count"] == 1


def test_dsl_accepts_executable_strategy_spec_contract():
    bars = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:05:00Z",
                    "2026-01-01T00:10:00Z",
                ],
                utc=True,
            ),
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.0, 101.0, 102.0],
            "volume": [10.0, 10.0, 10.0],
            "quote_volume": [1_000_000.0, 1_000_000.0, 1_000_000.0],
        }
    )
    features = _base_features(bars)
    features["oi_flush_event"] = [True, True, False]

    executable = ExecutableStrategySpec.model_validate(
        {
            "metadata": {
                "run_id": "r1",
                "blueprint_id": "bp_test",
                "candidate_id": "cand_1",
                "event_type": "VOL_SHOCK",
                "direction": "long",
                "retail_profile": "standard",
            },
            "research_origin": {
                "source_path": "dummy",
                "compiler_version": "v1",
                "generated_at_utc": "2026-01-01T00:00:00Z",
            },
            "entry": {
                "triggers": ["oi_flush_event"],
                "conditions": [],
                "confirmations": [],
                "delay_bars": 0,
                "cooldown_bars": 0,
                "condition_logic": "all",
                "order_type_assumption": "market",
            },
            "exit": {
                "time_stop_bars": 10,
                "invalidation": {"metric": "close", "operator": ">", "value": 10000.0},
                "stop_type": "percent",
                "stop_value": 0.01,
                "target_type": "percent",
                "target_value": 0.02,
                "trailing_stop_type": "none",
                "trailing_stop_value": 0.0,
                "break_even_r": 0.0,
            },
            "risk": {
                "low_capital_contract": {},
                "cost_model": {
                    "fees_bps_per_side": 2.0,
                    "slippage_bps_per_fill": 2.0,
                    "default_fee_tier": "standard",
                },
            },
            "sizing": {
                "mode": "fixed_risk",
                "risk_per_trade": 0.01,
                "max_gross_leverage": 1.0,
            },
            "execution": {
                "symbol_scope": {
                    "mode": "single_symbol",
                    "symbols": ["BTCUSDT"],
                    "candidate_symbol": "BTCUSDT",
                },
                "execution": {
                    "mode": "market",
                    "urgency": "aggressive",
                    "max_slippage_bps": 100.0,
                    "fill_profile": "base",
                    "retry_logic": {},
                },
                "policy_executor_config": {
                    "entry_delay_bars": 0,
                    "max_concurrent_positions": 1,
                    "per_position_notional_cap_usd": 1000.0,
                    "fee_tier": "standard",
                },
                "throttles": {
                    "one_trade_per_episode": False,
                    "cooldown_bars": 0,
                    "max_concurrent_positions": 1,
                },
            },
            "portfolio_constraints": {
                "effective_per_position_notional_cap_usd": 1000.0,
                "effective_max_concurrent_positions": 1,
            },
        }
    )

    strategy = DslInterpreterV1()
    positions = strategy.generate_positions(
        bars=bars,
        features=features,
        params={
            "strategy_symbol": "BTCUSDT",
            "executable_strategy_spec": executable.model_dump(),
        },
    )

    assert int(positions.iloc[0]) == 0
    assert int(positions.iloc[1]) == 1
    assert positions.attrs["strategy_metadata"]["contract_source"] == "executable_strategy_spec"
