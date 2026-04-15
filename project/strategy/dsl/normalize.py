from __future__ import annotations

from typing import Dict, List
from project.strategy.dsl.contract_v1 import validate_feature_references
from project.strategy.dsl.schema import (
    Blueprint,
    ConditionNodeSpec,
    EntrySpec,
    EvaluationSpec,
    ExecutionSpec,
    ExitSpec,
    LineageSpec,
    OverlaySpec,
    SizingSpec,
    SymbolScopeSpec,
)


def build_blueprint(raw: Dict[str, object]) -> Blueprint:
    """
    Canonicalizes blueprint/DSL objects from loose config.
    """
    validate_feature_references(raw)
    scope = raw.get("symbol_scope", {})
    entry = raw.get("entry", {})
    exit_spec = raw.get("exit", {})
    execution = raw.get("execution", {})
    sizing = raw.get("sizing", {})
    eval_spec = raw.get("evaluation", {})
    lineage = raw.get("lineage", {})
    overlays = raw.get("overlays", [])

    if (
        not isinstance(scope, dict)
        or not isinstance(entry, dict)
        or not isinstance(exit_spec, dict)
        or not isinstance(execution, dict)
    ):
        raise ValueError("Invalid blueprint payload shape")

    overlay_rows = []
    for row in overlays if isinstance(overlays, list) else []:
        if not isinstance(row, dict):
            raise ValueError("overlay row must be an object")
        overlay_rows.append(
            OverlaySpec(name=str(row.get("name", "")), params=dict(row.get("params", {})))
        )

    condition_nodes: List[ConditionNodeSpec] = []
    raw_nodes = entry.get("condition_nodes", [])
    if isinstance(raw_nodes, list):
        for row in raw_nodes:
            if not isinstance(row, dict):
                raise ValueError("entry.condition_nodes[] must be an object")
            condition_nodes.append(
                ConditionNodeSpec(
                    feature=str(row.get("feature", "")),
                    operator=str(row.get("operator", "")),  # type: ignore[arg-type]
                    value=float(row.get("value", 0.0)),
                    value_high=(
                        None if row.get("value_high") is None else float(row.get("value_high"))
                    ),
                    lookback_bars=int(row.get("lookback_bars", 0)),
                    window_bars=int(row.get("window_bars", 0)),
                )
            )

    # Auto-normalize legacy string conditions
    from project.strategy.dsl.conditions import ConditionRegistry

    legacy_conditions = entry.get("conditions", [])
    if isinstance(legacy_conditions, list):
        for cond_str in legacy_conditions:
            if not isinstance(cond_str, str):
                continue
            if cond_str.lower() == "all":
                continue
            nodes = ConditionRegistry.resolve(cond_str)
            if nodes:
                condition_nodes.extend(nodes)
            elif cond_str.lower().startswith("symbol_"):
                # Symbol overrides are handled at normalization/routing time,
                # but we keep them in conditions for now or drop if unused.
                pass
            else:
                raise ValueError(f"Cannot normalize legacy condition: {cond_str}")

    bp = Blueprint(
        id=str(raw.get("id", "")),
        run_id=str(raw.get("run_id", "")),
        event_type=str(raw.get("event_type", "")),
        candidate_id=str(raw.get("candidate_id", "")),
        symbol_scope=SymbolScopeSpec(
            mode=str(scope.get("mode", "")),  # type: ignore[arg-type]
            symbols=[str(x) for x in scope.get("symbols", [])]
            if isinstance(scope.get("symbols", []), list)
            else [],
            candidate_symbol=str(scope.get("candidate_symbol", "")),
        ),
        direction=str(raw.get("direction", "")),  # type: ignore[arg-type]
        entry=EntrySpec(
            triggers=[str(x) for x in entry.get("triggers", [])]
            if isinstance(entry.get("triggers", []), list)
            else [],
            conditions=[str(x) for x in entry.get("conditions", [])]
            if isinstance(entry.get("conditions", []), list)
            else [],
            confirmations=[str(x) for x in entry.get("confirmations", [])]
            if isinstance(entry.get("confirmations", []), list)
            else [],
            delay_bars=int(entry.get("delay_bars", 0)),
            cooldown_bars=int(entry.get("cooldown_bars", 0)),
            condition_logic=str(entry.get("condition_logic", "all")),  # type: ignore[arg-type]
            condition_nodes=condition_nodes,
            arm_bars=int(entry.get("arm_bars", 0)),
            reentry_lockout_bars=int(entry.get("reentry_lockout_bars", 0)),
        ),
        exit=ExitSpec(
            time_stop_bars=int(exit_spec.get("time_stop_bars", 0)),
            invalidation=dict(exit_spec.get("invalidation", {})),
            stop_type=str(exit_spec.get("stop_type", "")),  # type: ignore[arg-type]
            stop_value=float(exit_spec.get("stop_value", 0.0)),
            target_type=str(exit_spec.get("target_type", "")),  # type: ignore[arg-type]
            target_value=float(exit_spec.get("target_value", 0.0)),
            trailing_stop_type=str(exit_spec.get("trailing_stop_type", "none")),  # type: ignore[arg-type]
            trailing_stop_value=float(exit_spec.get("trailing_stop_value", 0.0)),
            break_even_r=float(exit_spec.get("break_even_r", 0.0)),
        ),
        execution=ExecutionSpec.model_validate(execution),
        sizing=SizingSpec(
            mode=str(sizing.get("mode", "")),  # type: ignore[arg-type]
            risk_per_trade=(
                None
                if sizing.get("risk_per_trade") is None
                else float(sizing.get("risk_per_trade"))
            ),
            target_vol=(
                None if sizing.get("target_vol") is None else float(sizing.get("target_vol"))
            ),
            max_gross_leverage=float(sizing.get("max_gross_leverage", 0.0)),
            max_position_scale=float(sizing.get("max_position_scale", 1.0)),
            portfolio_risk_budget=float(sizing.get("portfolio_risk_budget", 1.0)),
            symbol_risk_budget=float(sizing.get("symbol_risk_budget", 1.0)),
        ),
        overlays=overlay_rows,
        evaluation=EvaluationSpec(
            min_trades=int(eval_spec.get("min_trades", 0)),
            cost_model=dict(eval_spec.get("cost_model", {})),
            robustness_flags=dict(eval_spec.get("robustness_flags", {})),
        ),
        lineage=LineageSpec(
            source_path=str(lineage.get("source_path", "")),
            compiler_version=str(lineage.get("compiler_version", "")),
            generated_at_utc=str(lineage.get("generated_at_utc", "")),
            bridge_embargo_days_used=(
                None
                if lineage.get("bridge_embargo_days_used") in (None, "")
                else int(lineage.get("bridge_embargo_days_used"))
            ),
            discovery_start=str(lineage.get("discovery_start", "")),
            discovery_end=str(lineage.get("discovery_end", "")),
            canonical_event_type=str(lineage.get("canonical_event_type", "")),
            research_family=str(lineage.get("research_family", lineage.get("canonical_family", ""))),
            canonical_family=str(lineage.get("canonical_family", "")),
            canonical_regime=str(lineage.get("canonical_regime", "")),
            subtype=str(lineage.get("subtype", "")),
            phase=str(lineage.get("phase", "")),
            evidence_mode=str(lineage.get("evidence_mode", "")),
            regime_bucket=str(lineage.get("regime_bucket", "")),
            recommended_bucket=str(lineage.get("recommended_bucket", "")),
            routing_profile_id=str(lineage.get("routing_profile_id", "")),
            ttl_days=int(lineage.get("ttl_days", 90)),
        ),
    )
    return bp
