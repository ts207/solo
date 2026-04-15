from __future__ import annotations

import pytest

from project.compilers.spec_transformer import transform_blueprint_to_spec
from project.strategy.dsl.schema import (
    Blueprint,
    EntrySpec,
    EvaluationSpec,
    ExecutionSpec,
    ExitSpec,
    LineageSpec,
    SizingSpec,
    SymbolScopeSpec,
)


def _make_blueprint(*, direction: str = "long", mode: str = "market", urgency: str = "aggressive") -> Blueprint:
    return Blueprint(
        id="bp_transform_1",
        run_id="run_test",
        event_type="VOL_SHOCK",
        candidate_id="cand_1",
        symbol_scope=SymbolScopeSpec(
            mode="single_symbol",
            symbols=["BTCUSDT"],
            candidate_symbol="BTCUSDT",
        ),
        direction=direction,
        entry=EntrySpec(
            triggers=["event_detected"],
            conditions=["all"],
            confirmations=[],
            delay_bars=1,
            cooldown_bars=0,
            condition_logic="all",
            condition_nodes=[],
            arm_bars=1,
            reentry_lockout_bars=0,
        ),
        exit=ExitSpec(
            time_stop_bars=12,
            invalidation={"metric": "ret_1", "operator": "<", "value": -0.01},
            stop_type="percent",
            stop_value=0.01,
            target_type="percent",
            target_value=0.02,
        ),
        execution=ExecutionSpec(mode=mode, urgency=urgency),
        sizing=SizingSpec(
            mode="fixed_risk",
            risk_per_trade=0.01,
            max_gross_leverage=1.0,
            max_position_scale=1.0,
        ),
        overlays=[],
        evaluation=EvaluationSpec(
            min_trades=10,
            cost_model={"fees_bps": 2.0, "slippage_bps": 4.0, "funding_included": False},
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
        ),
    )


def test_transformer_maps_limit_passive_to_passive_then_cross_family_truthfully() -> None:
    passive = transform_blueprint_to_spec(_make_blueprint(mode="limit", urgency="passive"))
    delayed = transform_blueprint_to_spec(
        _make_blueprint(mode="limit", urgency="delayed_aggressive")
    )
    direct = transform_blueprint_to_spec(_make_blueprint(mode="limit", urgency="aggressive"))

    assert passive.execution.style == "passive"
    assert delayed.execution.style == "passive_then_cross"
    assert direct.execution.style == "limit"
    assert passive.data_requirements.book is True
    assert delayed.data_requirements.depth_fidelity == "top_5"


def test_transformer_rejects_unrepresentable_execution_modes() -> None:
    with pytest.raises(ValueError, match="unsupported blueprint execution mode"):
        transform_blueprint_to_spec(_make_blueprint(mode="close"))

    with pytest.raises(ValueError, match="unsupported blueprint execution mode"):
        transform_blueprint_to_spec(_make_blueprint(mode="next_open"))


def test_transformer_rejects_non_directional_blueprints() -> None:
    with pytest.raises(ValueError, match="unsupported blueprint direction"):
        transform_blueprint_to_spec(_make_blueprint(direction="both"))

    with pytest.raises(ValueError, match="unsupported blueprint direction"):
        transform_blueprint_to_spec(_make_blueprint(direction="conditional"))
