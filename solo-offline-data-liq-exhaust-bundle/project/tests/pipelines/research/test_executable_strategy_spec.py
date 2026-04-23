from __future__ import annotations

import json

from project.compilers.executable_strategy_spec import ExecutableStrategySpec
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


def _make_blueprint() -> Blueprint:
    return Blueprint(
        id="bp_exec_1",
        run_id="run_test",
        event_type="VOL_SHOCK",
        candidate_id="cand_exec_1",
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
            delay_bars=1,
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
            cost_model={"fees_bps": 2.0, "slippage_bps": 4.0, "funding_included": True},
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
            promotion_track="standard",
            constraints={"variant_one_trade_per_episode": True, "variant_cooldown_bars": 18},
        ),
    )


def test_build_executable_strategy_spec_sections():
    spec = compiler._build_executable_strategy_spec(
        blueprint=_make_blueprint(),
        run_id="run_test",
        retail_profile="capital_constrained",
        low_capital_contract={"account_equity_usd": 25_000.0},
        effective_max_concurrent_positions=3,
        effective_per_position_notional_cap_usd=15_000.0,
        default_fee_tier="taker",
        fees_bps_per_side=2.0,
        slippage_bps_per_fill=4.0,
    )

    assert isinstance(spec, ExecutableStrategySpec)
    payload = spec.model_dump()
    assert set(payload.keys()) == {
        "metadata",
        "research_origin",
        "entry",
        "exit",
        "risk",
        "sizing",
        "execution",
        "portfolio_constraints",
    }
    assert payload["metadata"]["candidate_id"] == "cand_exec_1"
    assert payload["research_origin"]["promotion_track"] == "standard"
    assert payload["execution"]["policy_executor_config"]["entry_delay_bars"] == 1


def test_write_strategy_contract_artifacts_emits_executable_strategy_spec_artifacts(tmp_path):
    blueprints = [_make_blueprint()]
    artifacts = compiler._write_strategy_contract_artifacts(
        blueprints=blueprints,
        out_dir=tmp_path,
        run_id="run_test",
        retail_profile="capital_constrained",
        low_capital_contract={"account_equity_usd": 25_000.0},
        require_low_capital_contract=False,
        effective_max_concurrent_positions=2,
        effective_per_position_notional_cap_usd=5_000.0,
        default_fee_tier="taker",
        fees_bps_per_side=2.0,
        slippage_bps_per_fill=4.0,
    )

    assert artifacts["executable_strategy_spec_count"] == 1
    path = tmp_path / "executable_strategy_specs" / "bp_exec_1.executable_strategy_spec.json"
    assert path.exists()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["metadata"]["blueprint_id"] == "bp_exec_1"
    index_payload = json.loads(
        (tmp_path / "executable_strategy_spec_index.json").read_text(encoding="utf-8")
    )
    assert index_payload["count"] == 1
