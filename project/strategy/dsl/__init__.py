"""Canonical umbrella namespace for strategy DSL contracts and helpers."""

from project.strategy.dsl.conditions import ConditionRegistry
from project.strategy.dsl.contract_v1 import (
    NonExecutableActionError,
    NonExecutableConditionError,
    action_to_overlays,
    derive_action_delay,
    is_executable_action,
    is_executable_condition,
    normalize_entry_condition,
    resolve_trigger_column,
    validate_action,
    validate_feature_references,
)
from project.strategy.dsl.normalize import build_blueprint
from project.strategy.dsl.policies import (
    DEFAULT_POLICY,
    EVENT_POLICIES,
    event_policy,
    overlay_defaults,
)
from project.strategy.dsl.references import REGISTRY_SIGNAL_COLUMNS, event_direction_bias
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
from project.strategy.dsl.validate import validate_overlay_columns

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
