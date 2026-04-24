
import pandas as pd
import pytest

from project.compilers.executable_strategy_spec import ExecutableStrategySpec
from project.core.feature_schema import feature_dataset_dir_name
from project.engine.runner import run_engine
from project.portfolio.allocation_spec import AllocationSpec


@pytest.fixture
def mock_data_root(tmp_path):
    # Write a tiny fake feature/bar slice
    lake = tmp_path / "lake"
    feat_dir = lake / "features" / "perp" / "BTCUSDT" / "5m" / feature_dataset_dir_name()
    feat_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=10, freq="5min", tz="UTC"),
            "open": [100.0] * 10,
            "close": [100.0] * 10,
            "high": [101.0] * 10,
            "low": [99.0] * 10,
            "volume": [10.0] * 10,
            "funding_rate_scaled": [0.0001] * 10,
            "direction_score": [0.5] * 10,
        }
    ).to_parquet(feat_dir / "slice.parquet")

    bar_dir = lake / "cleaned" / "perp" / "BTCUSDT" / "bars_5m"
    bar_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=10, freq="5min", tz="UTC"),
            "open": [100.0] * 10,
            "close": [100.0] * 10,
            "high": [101.0] * 10,
            "low": [99.0] * 10,
            "volume": [10.0] * 10,
            "quote_volume": [1000.0] * 10,
        }
    ).to_parquet(bar_dir / "slice.parquet")

    univ_dir = lake / "metadata" / "universe_snapshots"
    univ_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "listing_start": [pd.Timestamp("2020-01-01", tz="UTC")],
            "listing_end": [pd.Timestamp("2025-01-01", tz="UTC")],
        }
    ).to_parquet(univ_dir / "univ.parquet")

    return tmp_path


def test_engine_smoke_test(mock_data_root):
    from project.strategy.dsl.schema import (
        Blueprint,
        EntrySpec,
        EvaluationSpec,
        ExitSpec,
        LineageSpec,
        SizingSpec,
        SymbolScopeSpec,
    )

    bp = Blueprint(
        id="smoke_dsl",
        run_id="test",
        event_type="mock_event",
        candidate_id="mock_candidate",
        direction="long",
        symbol_scope=SymbolScopeSpec(
            mode="single_symbol", symbols=["BTCUSDT"], candidate_symbol="BTCUSDT"
        ),
        entry=EntrySpec(
            triggers=["event_detected"],
            conditions=[],
            confirmations=[],
            delay_bars=0,
            cooldown_bars=0,
            condition_logic="all",
            condition_nodes=[],
            arm_bars=0,
            reentry_lockout_bars=0,
        ),
        exit=ExitSpec(
            time_stop_bars=5,
            invalidation={"metric": "test", "operator": "==", "value": 0.0},
            stop_type="percent",
            stop_value=0.05,
            target_type="percent",
            target_value=0.05,
            trailing_stop_type="none",
            trailing_stop_value=0.0,
            break_even_r=0.0,
        ),
        sizing=SizingSpec(
            mode="fixed_risk",
            risk_per_trade=0.01,
            target_vol=None,
            max_gross_leverage=1.0,
            max_position_scale=1.0,
            portfolio_risk_budget=1.0,
            symbol_risk_budget=1.0,
        ),
        overlays=[],
        evaluation=EvaluationSpec(
            min_trades=0,
            cost_model={"fees_bps": 5.0, "slippage_bps": 0.0, "funding_included": True},
            robustness_flags={
                "oos_required": False,
                "multiplicity_required": False,
                "regime_stability_required": False,
            },
        ),
        lineage=LineageSpec(source_path="mock", compiler_version="mock", generated_at_utc="mock"),
    )

    result = run_engine(
        data_root=mock_data_root,
        run_id="test_run",
        symbols=["BTCUSDT"],
        strategies=["dsl_interpreter_v1__smoke_dsl"],
        params_by_strategy={"dsl_interpreter_v1__smoke_dsl": {"dsl_blueprint": bp.model_dump()}},
        params={},
        cost_bps=5.0,
        start_ts=pd.Timestamp("2024-01-01", tz="UTC"),
        end_ts=pd.Timestamp("2024-01-02", tz="UTC"),
    )

    assert "metrics" in result
    assert "strategies" in result["metrics"]
    assert "dsl_interpreter_v1__smoke_dsl" in result["metrics"]["strategies"]


def test_engine_smoke_accepts_executable_strategy_spec(mock_data_root):
    executable = ExecutableStrategySpec.model_validate(
        {
            "metadata": {
                "run_id": "test_run",
                "blueprint_id": "smoke_dsl",
                "candidate_id": "mock_candidate",
                "event_type": "mock_event",
                "direction": "long",
                "retail_profile": "standard",
            },
            "research_origin": {
                "source_path": "mock",
                "compiler_version": "mock",
                "generated_at_utc": "mock",
            },
            "entry": {
                "triggers": ["event_detected"],
                "conditions": [],
                "confirmations": [],
                "delay_bars": 0,
                "cooldown_bars": 0,
                "condition_logic": "all",
                "order_type_assumption": "market",
            },
            "exit": {
                "time_stop_bars": 5,
                "invalidation": {"metric": "test", "operator": "==", "value": 0.0},
                "stop_type": "percent",
                "stop_value": 0.05,
                "target_type": "percent",
                "target_value": 0.05,
                "trailing_stop_type": "none",
                "trailing_stop_value": 0.0,
                "break_even_r": 0.0,
            },
            "risk": {
                "low_capital_contract": {},
                "cost_model": {
                    "fees_bps_per_side": 5.0,
                    "slippage_bps_per_fill": 0.0,
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

    result = run_engine(
        data_root=mock_data_root,
        run_id="test_run",
        symbols=["BTCUSDT"],
        strategies=["dsl_interpreter_v1__smoke_exec"],
        params_by_strategy={
            "dsl_interpreter_v1__smoke_exec": {"executable_strategy_spec": executable.model_dump()}
        },
        params={},
        cost_bps=5.0,
        start_ts=pd.Timestamp("2024-01-01", tz="UTC"),
        end_ts=pd.Timestamp("2024-01-02", tz="UTC"),
    )

    assert "metrics" in result
    assert "dsl_interpreter_v1__smoke_exec" in result["metrics"]["strategies"]


def test_engine_smoke_accepts_allocation_spec(mock_data_root):
    executable = ExecutableStrategySpec.model_validate(
        {
            "metadata": {
                "run_id": "test_run",
                "blueprint_id": "smoke_dsl",
                "candidate_id": "mock_candidate",
                "event_type": "mock_event",
                "direction": "long",
                "retail_profile": "standard",
            },
            "research_origin": {
                "source_path": "mock",
                "compiler_version": "mock",
                "generated_at_utc": "mock",
            },
            "entry": {
                "triggers": ["event_detected"],
                "conditions": [],
                "confirmations": [],
                "delay_bars": 0,
                "cooldown_bars": 0,
                "condition_logic": "all",
                "order_type_assumption": "market",
            },
            "exit": {
                "time_stop_bars": 5,
                "invalidation": {"metric": "test", "operator": "==", "value": 0.0},
                "stop_type": "percent",
                "stop_value": 0.05,
                "target_type": "percent",
                "target_value": 0.05,
                "trailing_stop_type": "none",
                "trailing_stop_value": 0.0,
                "break_even_r": 0.0,
            },
            "risk": {
                "low_capital_contract": {},
                "cost_model": {
                    "fees_bps_per_side": 5.0,
                    "slippage_bps_per_fill": 0.0,
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
    allocation_spec = AllocationSpec.model_validate(
        {
            "metadata": {
                "run_id": "test_run",
                "blueprint_id": "smoke_dsl",
                "candidate_id": "mock_candidate",
                "event_type": "mock_event",
                "retail_profile": "standard",
            },
            "sizing_policy": {
                "mode": "fixed_risk",
                "risk_per_trade": 0.01,
                "max_gross_leverage": 1.0,
                "portfolio_risk_budget": 2.0,
                "symbol_risk_budget": 1.0,
                "signal_scaling": {},
            },
            "risk_controls": {
                "low_capital_contract": {},
                "max_concurrent_positions": 1,
                "per_position_notional_cap_usd": 1000.0,
                "fee_tier": "standard",
                "cost_model": {"fees_bps_per_side": 5.0, "slippage_bps_per_fill": 0.0},
            },
            "allocation_policy": {
                "symbol_scope": {
                    "mode": "single_symbol",
                    "symbols": ["BTCUSDT"],
                    "candidate_symbol": "BTCUSDT",
                },
                "constraints": {
                    "allocator_mode": "deterministic_optimizer",
                    "strategy_risk_budgets": {"dsl_interpreter_v1__smoke_alloc": 1.0},
                },
            },
        }
    )

    result = run_engine(
        data_root=mock_data_root,
        run_id="test_run",
        symbols=["BTCUSDT"],
        strategies=["dsl_interpreter_v1__smoke_alloc"],
        params_by_strategy={
            "dsl_interpreter_v1__smoke_alloc": {
                "executable_strategy_spec": executable.model_dump(),
                "allocation_spec": allocation_spec.model_dump(),
            }
        },
        params={},
        cost_bps=5.0,
        start_ts=pd.Timestamp("2024-01-01", tz="UTC"),
        end_ts=pd.Timestamp("2024-01-02", tz="UTC"),
    )

    assert result["manifest"]["allocator"]["mode"] == "deterministic_optimizer"
    assert result["manifest"]["allocator"]["contract"]["policy"]["strategy_risk_budgets"] == {
        "dsl_interpreter_v1__smoke_alloc": pytest.approx(1.0)
    }
