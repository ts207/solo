"""Public reliability helpers.

Keep this package initializer lightweight: many tests import small contract helpers via
``project.reliability.contracts`` and should not pay the cost of the smoke CLI stack.
"""

from __future__ import annotations

__all__ = [
    "reconcile_bundle_outputs",
    "run_smoke_cli",
    "validate_candidate_table",
    "validate_manifest",
    "validate_portfolio_ledger",
    "validate_promotion_artifacts",
    "validate_strategy_trace",
]


def __getattr__(name: str):
    if name == "run_smoke_cli":
        from project.reliability.cli_smoke import run_smoke_cli

        return run_smoke_cli

    if name in {
        "reconcile_bundle_outputs",
        "validate_candidate_table",
        "validate_manifest",
        "validate_portfolio_ledger",
        "validate_promotion_artifacts",
        "validate_strategy_trace",
    }:
        from project.reliability import contracts

        return getattr(contracts, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
