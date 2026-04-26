"""Portfolio allocation, sizing, and risk-budget helpers.

Public symbols are loaded lazily so importing one lightweight submodule does not
initialize pandas-heavy analytics modules.
"""

from __future__ import annotations

__all__ = [
    "ALLOCATION_SPEC_VERSION",
    "THESIS_OVERLAP_SCHEMA_VERSION",
    "AdmissionResult",
    "AllocationSpec",
    "PortfolioAdmissionPolicy",
    "PortfolioCapitalDecision",
    "PortfolioDecisionEngine",
    "ThesisIntent",
    "build_thesis_overlap_graph",
    "calculate_execution_aware_target_notional",
    "calculate_portfolio_risk_multiplier",
    "calculate_target_notional",
    "get_asset_correlation_adjustment",
    "overlap_group_id_for_thesis",
    "write_portfolio_decision_trace",
    "write_thesis_overlap_artifacts",
]


def __getattr__(name: str):  # pragma: no cover - exercised by import sites
    if name in {"AdmissionResult", "PortfolioAdmissionPolicy"}:
        from project.portfolio.admission_policy import AdmissionResult, PortfolioAdmissionPolicy

        values = {
            "AdmissionResult": AdmissionResult,
            "PortfolioAdmissionPolicy": PortfolioAdmissionPolicy,
        }
        return values[name]
    if name in {"ALLOCATION_SPEC_VERSION", "AllocationSpec"}:
        from project.portfolio.allocation_spec import ALLOCATION_SPEC_VERSION, AllocationSpec

        values = {
            "ALLOCATION_SPEC_VERSION": ALLOCATION_SPEC_VERSION,
            "AllocationSpec": AllocationSpec,
        }
        return values[name]
    if name in {"PortfolioCapitalDecision", "PortfolioDecisionEngine", "ThesisIntent"}:
        from project.portfolio.engine import (
            PortfolioCapitalDecision,
            PortfolioDecisionEngine,
            ThesisIntent,
        )

        return {
            "PortfolioCapitalDecision": PortfolioCapitalDecision,
            "PortfolioDecisionEngine": PortfolioDecisionEngine,
            "ThesisIntent": ThesisIntent,
        }[name]
    if name in {"calculate_execution_aware_target_notional", "calculate_target_notional"}:
        from project.portfolio.sizing import (
            calculate_execution_aware_target_notional,
            calculate_target_notional,
        )

        return {
            "calculate_execution_aware_target_notional": calculate_execution_aware_target_notional,
            "calculate_target_notional": calculate_target_notional,
        }[name]
    if name in {"calculate_portfolio_risk_multiplier", "get_asset_correlation_adjustment"}:
        from project.portfolio.risk_budget import (
            calculate_portfolio_risk_multiplier,
            get_asset_correlation_adjustment,
        )

        return {
            "calculate_portfolio_risk_multiplier": calculate_portfolio_risk_multiplier,
            "get_asset_correlation_adjustment": get_asset_correlation_adjustment,
        }[name]
    if name == "write_portfolio_decision_trace":
        from project.portfolio.reporting import write_portfolio_decision_trace

        return write_portfolio_decision_trace
    if name in {
        "THESIS_OVERLAP_SCHEMA_VERSION",
        "build_thesis_overlap_graph",
        "overlap_group_id_for_thesis",
        "write_thesis_overlap_artifacts",
    }:
        from project.portfolio import thesis_overlap

        return getattr(thesis_overlap, name)
    raise AttributeError(f"module 'project.portfolio' has no attribute {name!r}")
