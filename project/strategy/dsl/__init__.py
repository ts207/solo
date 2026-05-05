"""Canonical umbrella namespace for strategy DSL contracts and helpers.

The public API is kept compatible while loading heavy registry-backed helpers only
when a caller asks for them.  This keeps tests that only need DSL schema models
from importing event/domain registries during collection.
"""

from __future__ import annotations

__all__ = [
    "DEFAULT_POLICY",
    "EVENT_POLICIES",
    "REGISTRY_SIGNAL_COLUMNS",
    "Blueprint",
    "ConditionNodeSpec",
    "ConditionRegistry",
    "EntrySpec",
    "EvaluationSpec",
    "ExecutionSpec",
    "ExitSpec",
    "LineageSpec",
    "NonExecutableActionError",
    "NonExecutableConditionError",
    "OverlaySpec",
    "SizingSpec",
    "SymbolScopeSpec",
    "action_to_overlays",
    "build_blueprint",
    "derive_action_delay",
    "event_direction_bias",
    "event_policy",
    "is_executable_action",
    "is_executable_condition",
    "normalize_entry_condition",
    "overlay_defaults",
    "resolve_trigger_column",
    "validate_action",
    "validate_feature_references",
    "validate_overlay_columns",
]

_SCHEMA_EXPORTS = {
    "Blueprint",
    "ConditionNodeSpec",
    "EntrySpec",
    "EvaluationSpec",
    "ExecutionSpec",
    "ExitSpec",
    "LineageSpec",
    "OverlaySpec",
    "SizingSpec",
    "SymbolScopeSpec",
}
_CONTRACT_EXPORTS = {
    "NonExecutableActionError",
    "NonExecutableConditionError",
    "action_to_overlays",
    "derive_action_delay",
    "is_executable_action",
    "is_executable_condition",
    "normalize_entry_condition",
    "resolve_trigger_column",
    "validate_action",
    "validate_feature_references",
}
_POLICY_EXPORTS = {"DEFAULT_POLICY", "EVENT_POLICIES", "event_policy", "overlay_defaults"}
_REFERENCE_EXPORTS = {"REGISTRY_SIGNAL_COLUMNS", "event_direction_bias"}


def __getattr__(name: str):  # pragma: no cover - exercised by import sites
    if name == "ConditionRegistry":
        from project.strategy.dsl.conditions import ConditionRegistry

        return ConditionRegistry
    if name in _CONTRACT_EXPORTS:
        from project.strategy.dsl import contract_v1

        return getattr(contract_v1, name)
    if name == "build_blueprint":
        from project.strategy.dsl.normalize import build_blueprint

        return build_blueprint
    if name in _POLICY_EXPORTS:
        from project.strategy.dsl import policies

        return getattr(policies, name)
    if name in _REFERENCE_EXPORTS:
        from project.strategy.dsl import references

        return getattr(references, name)
    if name in _SCHEMA_EXPORTS:
        from project.strategy.dsl import schema

        return getattr(schema, name)
    if name == "validate_overlay_columns":
        from project.strategy.dsl.validate import validate_overlay_columns

        return validate_overlay_columns
    raise AttributeError(f"module 'project.strategy.dsl' has no attribute {name!r}")
