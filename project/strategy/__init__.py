"""Canonical umbrella namespace for strategy models, DSL, runtime, and templates."""

__all__ = ["Blueprint", "ExecutableStrategySpec", "dsl", "runtime", "templates"]


def __getattr__(name: str):
    if name == "Blueprint":
        from project.strategy.models.blueprint import Blueprint

        return Blueprint
    if name == "ExecutableStrategySpec":
        from project.strategy.models.executable_strategy_spec import ExecutableStrategySpec

        return ExecutableStrategySpec
    if name in {"dsl", "runtime", "templates"}:
        import importlib

        return importlib.import_module(f"project.strategy.{name}")
    raise AttributeError(name)
