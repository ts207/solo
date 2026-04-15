"""Live trading runtime surfaces for execution, health, and operator control.

The live package deliberately uses lazy exports so submodules such as
``project.live.contracts`` can be imported without eagerly importing the full
runtime stack. This avoids package-level circular imports between the
engine/portfolio/live layers while preserving the cosmetic package root
surface used by tests and operator code.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any, Dict, Tuple

_EXPORTS: Dict[str, Tuple[str, str]] = {
    "DataHealthMonitor": ("project.live.health_checks", "DataHealthMonitor"),
    "DecisionOutcome": ("project.live.decision", "DecisionOutcome"),
    "DetectedEvent": ("project.live.event_detector", "DetectedEvent"),
    "KillSwitchManager": ("project.live.kill_switch", "KillSwitchManager"),
    "KillSwitchReason": ("project.live.kill_switch", "KillSwitchReason"),
    "KillSwitchStatus": ("project.live.kill_switch", "KillSwitchStatus"),
    "LiveTradeContext": ("project.live.contracts", "LiveTradeContext"),
    "LiveEngineRunner": ("project.live.runner", "LiveEngineRunner"),
    "OrderPlan": ("project.live.order_planner", "OrderPlan"),
    "PromotedThesis": ("project.live.contracts", "PromotedThesis"),
    "LiveStateStore": ("project.live.state", "LiveStateStore"),
    "PositionState": ("project.live.state", "PositionState"),
    "ReplayResult": ("project.live.replay", "ReplayResult"),
    "ThesisEvidence": ("project.live.contracts", "ThesisEvidence"),
    "ThesisLineage": ("project.live.contracts", "ThesisLineage"),
    "ThesisStore": ("project.live.thesis_store", "ThesisStore"),
    "TradeIntent": ("project.live.contracts", "TradeIntent"),
    "build_runtime_certification_manifest": ("project.live.health_checks", "build_runtime_certification_manifest"),
    "build_order_plan": ("project.live.order_planner", "build_order_plan"),
    "check_kill_switch_triggers": ("project.live.health_checks", "check_kill_switch_triggers"),
    "decide_trade_intent": ("project.live.decision", "decide_trade_intent"),
    "detect_live_event": ("project.live.event_detector", "detect_live_event"),
    "evaluate_pretrade_microstructure_gate": ("project.live.health_checks", "evaluate_pretrade_microstructure_gate"),
    "replay_contexts": ("project.live.replay", "replay_contexts"),
    "validate_market_microstructure": ("project.live.health_checks", "validate_market_microstructure"),
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
