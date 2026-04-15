from __future__ import annotations

import json

import pytest

from project.research import compile_strategy_blueprints as compiler
from project.strategy.dsl.schema import (
    Blueprint,
    EntrySpec,
    EvaluationSpec,
    ExitSpec,
    LineageSpec,
    SizingSpec,
    SymbolScopeSpec,
)


def _make_blueprint(*, bp_id: str, candidate_id: str, delay_bars: int = 1) -> Blueprint:
    return Blueprint(
        id=bp_id,
        run_id="run_test",
        event_type="VOL_SHOCK",
        candidate_id=candidate_id,
        symbol_scope=SymbolScopeSpec(
            mode="single_symbol",
            symbols=["BTCUSDT"],
            candidate_symbol="BTCUSDT",
        ),
        direction="long",
        entry=EntrySpec(
            triggers=["entry_trigger"],
            conditions=["vol_regime_high"],
            confirmations=["oos_validation_pass"],
            delay_bars=delay_bars,
            cooldown_bars=12,
        ),
        exit=ExitSpec(
            time_stop_bars=8,
            invalidation={"metric": "ret_1m", "operator": "<", "value": -1.0},
            stop_type="percent",
            stop_value=0.01,
            target_type="percent",
            target_value=0.02,
            trailing_stop_type="percent",
            trailing_stop_value=0.005,
            break_even_r=1.0,
        ),
        sizing=SizingSpec(
            mode="fixed_risk",
            risk_per_trade=0.01,
            max_gross_leverage=1.0,
        ),
        overlays=[],
        evaluation=EvaluationSpec(
            min_trades=10,
            cost_model={
                "fees_bps": 2.0,
                "slippage_bps": 4.0,
                "funding_included": True,
            },
            robustness_flags={
                "oos_required": True,
                "multiplicity_required": True,
                "regime_stability_required": True,
            },
        ),
        lineage=LineageSpec(
            source_path="source.csv",
            compiler_version="strategy_dsl_v1",
            generated_at_utc="1970-01-01T00:00:00Z",
            constraints={
                "variant_one_trade_per_episode": True,
                "variant_cooldown_bars": 18,
            },
        ),
    )


def _low_cap_contract() -> dict[str, object]:
    return {
        "account_equity_usd": 25_000.0,
        "max_position_notional_usd": 15_000.0,
        "min_position_notional_usd": 25.0,
        "max_leverage": 3.0,
        "max_trades_per_day": 20,
        "max_turnover_per_day": 4.0,
        "fee_tier": "taker",
        "slippage_model_baseline_bps": 6.0,
        "stress_cost_multiplier_2x": 2.0,
        "stress_cost_multiplier_3x": 3.0,
        "spread_model": "top_book_bps",
        "entry_delay_bars_default": 1,
        "entry_delay_bars_stress": 2,
        "max_drawdown_pct": 0.20,
        "max_daily_loss_pct": 0.05,
        "stop_trading_rule": "daily_loss_breach",
        "bar_timestamp_semantics": "open_time",
        "signal_snap_side": "left",
        "active_range_semantics": "[start,end)",
    }


def test_strategy_contract_build_and_validate_against_low_cap_contract():
    blueprint = _make_blueprint(bp_id="bp_1", candidate_id="cand_1", delay_bars=1)
    contract = _low_cap_contract()
    strategy_spec = compiler._build_strategy_contract(
        blueprint=blueprint,
        run_id="run_test",
        retail_profile="capital_constrained",
        low_capital_contract=contract,
        effective_max_concurrent_positions=3,
        effective_per_position_notional_cap_usd=15_000.0,
        default_fee_tier="taker",
        fees_bps_per_side=2.0,
        slippage_bps_per_fill=4.0,
    )

    compiler._validate_strategy_contract(
        strategy_spec,
        low_capital_contract=contract,
        require_low_capital_contract=True,
    )

    payload = strategy_spec.model_dump()
    assert payload["entry"]["order_type_assumption"] == "market"
    assert payload["execution"]["throttles"]["one_trade_per_episode"] is True
    assert payload["execution"]["throttles"]["cooldown_bars"] == 18
    assert (
        payload["execution"]["policy_executor_config"]["entry_delay_bars"]
        == payload["entry"]["delay_bars"]
    )


def test_strategy_contract_validator_fails_closed_on_mismatched_entry_delay():
    blueprint = _make_blueprint(bp_id="bp_2", candidate_id="cand_2")
    strategy_spec = compiler._build_strategy_contract(
        blueprint=blueprint,
        run_id="run_test",
        retail_profile="capital_constrained",
        low_capital_contract={},
        effective_max_concurrent_positions=1,
        effective_per_position_notional_cap_usd=1000.0,
        default_fee_tier="taker",
        fees_bps_per_side=2.0,
        slippage_bps_per_fill=4.0,
    )
    strategy_spec = strategy_spec.model_copy(
        update={
            "execution": strategy_spec.execution.model_copy(
                update={
                    "policy_executor_config": {
                        **strategy_spec.execution.policy_executor_config,
                        "entry_delay_bars": 99,
                    }
                }
            )
        }
    )

    with pytest.raises(ValueError, match="entry delay mismatch"):
        compiler._validate_strategy_contract(
            strategy_spec,
            low_capital_contract={},
            require_low_capital_contract=False,
        )


def test_write_strategy_contract_artifacts_emits_per_candidate_json_and_executor_config(
    tmp_path,
):
    blueprints = [
        _make_blueprint(bp_id="bp_3", candidate_id="cand_3"),
        _make_blueprint(bp_id="bp_4", candidate_id="cand_4"),
    ]
    artifacts = compiler._write_strategy_contract_artifacts(
        blueprints=blueprints,
        out_dir=tmp_path,
        run_id="run_test",
        retail_profile="capital_constrained",
        low_capital_contract={},
        require_low_capital_contract=False,
        effective_max_concurrent_positions=2,
        effective_per_position_notional_cap_usd=5_000.0,
        default_fee_tier="taker",
        fees_bps_per_side=2.0,
        slippage_bps_per_fill=4.0,
    )

    assert artifacts["count"] == 2
    assert artifacts["executable_strategy_spec_count"] == 2
    assert artifacts["allocation_spec_count"] == 2
    for bp in blueprints:
        executable_path = (
            tmp_path / "executable_strategy_specs" / f"{bp.id}.executable_strategy_spec.json"
        )
        assert executable_path.exists()
        payload = json.loads(executable_path.read_text(encoding="utf-8"))
        assert payload["metadata"]["candidate_id"] == bp.candidate_id
        allocation_path = tmp_path / "allocation_specs" / f"{bp.id}.allocation_spec.json"
        assert allocation_path.exists()
    assert not (tmp_path / "strategy_ir").exists()
    assert not (tmp_path / "strategy_ir_index.json").exists()

    executor_lines = [
        line
        for line in (tmp_path / "policy_executor_configs.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    assert len(executor_lines) == 2
