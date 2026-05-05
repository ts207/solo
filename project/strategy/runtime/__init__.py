"""Strategy runtime public API with lazy imports for fast test collection."""

from __future__ import annotations

__all__ = [
    "DslInterpreterV1",
    "get_strategy",
    "is_dsl_strategy",
    "parse_strategy_name",
    "resolve_strategy",
]


def get_strategy(name: str):
    from project.strategy.runtime.registry import get_strategy as _get_strategy

    return _get_strategy(name)


def is_dsl_strategy(name: str) -> bool:
    from project.strategy.runtime.registry import is_dsl_strategy as _is_dsl_strategy

    return _is_dsl_strategy(name)


def parse_strategy_name(name: str):
    from project.strategy.runtime.registry import parse_strategy_name as _parse_strategy_name

    return _parse_strategy_name(name)


def resolve_strategy(name: str):
    from project.strategy.runtime.registry import resolve_strategy as _resolve_strategy

    return _resolve_strategy(name)


def __getattr__(name: str):  # pragma: no cover - exercised by import sites
    if name == "DslInterpreterV1":
        from project.strategy.runtime.dsl_interpreter_v1 import DslInterpreterV1

        return DslInterpreterV1
    raise AttributeError(f"module 'project.strategy.runtime' has no attribute {name!r}")
