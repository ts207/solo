from __future__ import annotations

import json

from project.portfolio.allocation_spec import AllocationSpec
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
        id="bp_alloc_1",
        run_id="run_test",
        event_type="VOL_SHOCK",
        candidate_id="cand_alloc_1",
        symbol_scope=SymbolScopeSpec(
            mode="single_symbol",
            symbols=["BTCUSDT"],
            candidate_symbol="BTCUSDT",
        ),
        direction="long",
        entry=EntrySpec(
            triggers=["entry_trigger"],
            conditions=["vol_regime_high"],
            confirmations=[],
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
        ),
        sizing=SizingSpec(
            mode="fixed_risk",
            risk_per_trade=0.01,
            max_gross_leverage=1.0,
            portfolio_risk_budget=0.8,
            symbol_risk_budget=0.4,
            signal_scaling={"mode": "confidence"},
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
            constraints={"retail_profile": "capital_constrained"},
        ),
    )


def test_build_allocation_spec_sections():
    spec = compiler._build_allocation_spec(
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

    assert isinstance(spec, AllocationSpec)
    payload = spec.model_dump()
    assert set(payload.keys()) == {
        "metadata",
        "sizing_policy",
        "risk_controls",
        "allocation_policy",
    }
    assert payload["sizing_policy"]["portfolio_risk_budget"] == 0.8
    assert payload["risk_controls"]["max_concurrent_positions"] == 3
    assert payload["allocation_policy"]["symbol_scope"]["candidate_symbol"] == "BTCUSDT"


def test_write_strategy_contract_artifacts_emits_allocation_spec_artifacts(tmp_path):
    artifacts = compiler._write_strategy_contract_artifacts(
        blueprints=[_make_blueprint()],
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

    assert artifacts["allocation_spec_count"] == 1
    path = tmp_path / "allocation_specs" / "bp_alloc_1.allocation_spec.json"
    assert path.exists()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["metadata"]["candidate_id"] == "cand_alloc_1"
    index_payload = json.loads(
        (tmp_path / "allocation_spec_index.json").read_text(encoding="utf-8")
    )
    assert index_payload["count"] == 1
