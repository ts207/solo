from __future__ import annotations

"""
Compatibility shim.

`project.strategy.dsl.schema` is the canonical Strategy DSL schema.
Keep this module only as a transitional import surface until all callers
move off `schema_v2`.
"""

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

__all__ = [
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
]
