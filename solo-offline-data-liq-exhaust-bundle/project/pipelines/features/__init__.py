"""Feature-stage pipeline entrypoints."""

from __future__ import annotations

from importlib import import_module

__all__ = [
    "build_features",
    "build_market_context",
    "build_microstructure_rollup",
]


def __getattr__(name: str):
    if name in __all__:
        return import_module(f"{__name__}.{name}")
    raise AttributeError(name)
