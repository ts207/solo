from __future__ import annotations

import logging

from project import PROJECT_ROOT
from project.core.config import load_configs
from project.schemas.strategy_spec import (
    DataRequirements,
    EntryCondition,
    RiskSpec,
    StrategySpec,
)
from project.schemas.strategy_spec import (
    EntrySpec as CanonicalEntrySpec,
)
from project.schemas.strategy_spec import (
    ExecutionSpec as CanonicalExecutionSpec,
)
from project.schemas.strategy_spec import (
    ExitSpec as CanonicalExitSpec,
)
from project.strategy.dsl.schema import Blueprint as DSLBlueprint

_LOG = logging.getLogger(__name__)


def _canonical_execution_style(blueprint: DSLBlueprint) -> str:
    mode = str(blueprint.execution.mode).strip().lower()
    urgency = str(blueprint.execution.urgency).strip().lower()

    if mode == "market":
        return "market"
    if mode == "limit":
        if urgency == "passive":
            return "passive"
        if urgency == "delayed_aggressive":
            return "passive_then_cross"
        return "limit"

    raise ValueError(
        f"unsupported blueprint execution mode for canonical StrategySpec: {blueprint.execution.mode}"
    )


def _canonical_direction(blueprint: DSLBlueprint) -> str:
    direction = str(blueprint.direction).strip().lower()
    if direction == "long":
        return "LONG"
    if direction == "short":
        return "SHORT"
    raise ValueError(
        f"unsupported blueprint direction for canonical StrategySpec: {blueprint.direction}"
    )


def transform_blueprint_to_spec(blueprint: DSLBlueprint) -> StrategySpec:
    """
    Transforms a research DSL Blueprint into a canonical StrategySpec.
    """
    execution_style = _canonical_execution_style(blueprint)
    direction = _canonical_direction(blueprint)

    # 1. Map Data Requirements
    # Mandate 1m bars if execution style requires it for realistic simulation
    requires_high_fidelity = execution_style in {"limit", "passive", "passive_then_cross"}

    data_reqs = DataRequirements(
        bars=["1m"] if requires_high_fidelity else ["5m"],
        book=requires_high_fidelity,
        trades=True,
        latency_class="low" if requires_high_fidelity else "medium",
        depth_fidelity="top_5" if requires_high_fidelity else "tob",
    )

    # 2. Map Entry Conditions
    canonical_conditions = []
    for node in blueprint.entry.condition_nodes:
        canonical_conditions.append(
            EntryCondition(
                feature=node.feature,
                operator=node.operator
                if node.operator in ["==", "!=", ">", "<", ">=", "<="]
                else ">",  # Simple mapping
                value=node.value,
            )
        )

    entry_spec = CanonicalEntrySpec(
        event_family=blueprint.event_type,
        conditions=canonical_conditions,
        direction=direction,
    )

    # 3. Map Exit Logic
    exit_spec = CanonicalExitSpec(
        time_stop_bars=blueprint.exit.time_stop_bars,
        take_profit_bps=blueprint.exit.target_value * 10000.0
        if blueprint.exit.target_type == "percent"
        else None,
        stop_loss_bps=blueprint.exit.stop_value * 10000.0
        if blueprint.exit.stop_type == "percent"
        else None,
    )

    # 4. Map Risk
    try:
        profiles_cfg = load_configs([str(PROJECT_ROOT / "configs" / "retail_profiles.yaml")])
        profile = profiles_cfg.get("profiles", {}).get("capital_constrained", {})
        baseline_capital = float(profile.get("account_equity_usd", 25000.0))
        max_positions = int(profile.get("max_concurrent_positions", 3))
    except Exception as e:
        _LOG.warning("Failed to load retail profiles for risk sizing, using defaults: %s", e)
        baseline_capital = 25000.0
        max_positions = 3

    risk_spec = RiskSpec(
        max_position_notional_usd=blueprint.sizing.max_position_scale * baseline_capital,
        max_concurrent_positions=max_positions,
    )

    # 5. Map Execution
    execution_spec = CanonicalExecutionSpec(
        style=execution_style,
        post_only_preference=execution_style in {"limit", "passive", "passive_then_cross"},
        slippage_assumption_bps=blueprint.execution.max_slippage_bps,
        cost_assumption_bps=blueprint.evaluation.cost_model.get("fees_bps", 1.0),
    )

    # 6. Final Spec
    spec = StrategySpec(
        strategy_id=blueprint.id,
        thesis=f"Event-driven strategy for {blueprint.event_type}",
        venue=getattr(blueprint.symbol_scope, "venue", "BINANCE"),
        instrument=blueprint.symbol_scope.candidate_symbol,
        data_requirements=data_reqs,
        entry=entry_spec,
        exit=exit_spec,
        risk=risk_spec,
        execution=execution_spec,
    )

    spec.validate_spec()
    return spec
