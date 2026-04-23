"""Clean-stage pipeline entrypoints."""

from __future__ import annotations

from importlib import import_module

__all__ = [
    "build_basis_state_5m",
    "build_cleaned_bars",
    "build_tob_5m_agg",
    "build_tob_snapshots_1s",
    "calibrate_execution_costs",
    "validate_context_entropy",
    "validate_data_coverage",
    "validate_feature_integrity",
]


def __getattr__(name: str):
    if name in __all__:
        return import_module(f"{__name__}.{name}")
    raise AttributeError(name)
