"""Compilation surfaces for executable and canonical strategy specs."""

from __future__ import annotations

__all__ = [
    "EXECUTABLE_STRATEGY_SPEC_VERSION",
    "ExecutableStrategyMetadata",
    "ExecutableStrategySpec",
    "PortfolioConstraintsSpec",
    "ResearchOriginSpec",
    "transform_blueprint_to_spec",
]


def __getattr__(name: str):
    if name in {
        "EXECUTABLE_STRATEGY_SPEC_VERSION",
        "ExecutableStrategyMetadata",
        "ExecutableStrategySpec",
        "PortfolioConstraintsSpec",
        "ResearchOriginSpec",
    }:
        from project.compilers import executable_strategy_spec

        return getattr(executable_strategy_spec, name)
    if name == "transform_blueprint_to_spec":
        from project.compilers.spec_transformer import transform_blueprint_to_spec

        return transform_blueprint_to_spec
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
