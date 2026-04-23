from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from project.research.agent_io.broad_discovery import (
        BroadDiscoveryConfig,
        BroadDiscoveryResult,
        BroadDiscoveryRunner,
        EventAttribution,
        FamilyDiscoveryResult,
        FamilyEventConfig,
        run_broad_discovery,
    )
    from project.research.agent_io.campaign_planner import (
        CampaignPlanner,
        CampaignPlannerConfig,
        CampaignPlanResult,
        run_campaign_planner_cycle,
    )
    from project.research.agent_io.closed_loop import (
        CampaignCycleRunner,
        CycleConfig,
        CycleResult,
        run_autonomous_cycle,
    )
    from project.research.agent_io.execute_proposal import build_run_all_command, execute_proposal
    from project.research.agent_io.issue_proposal import generate_run_id, issue_proposal
    from project.research.agent_io.proposal_schema import (
        AgentProposal,
        compile_single_hypothesis_to_agent_proposal,
        detect_operator_proposal_format,
        load_agent_proposal,
        load_operator_proposal,
    )
    from project.research.agent_io.proposal_to_experiment import (
        build_run_all_overrides,
        proposal_to_experiment_config,
        translate_and_validate_proposal,
    )

_EXPORTS = {
    "AgentProposal": ("project.research.agent_io.proposal_schema", "AgentProposal"),
    "BroadDiscoveryConfig": ("project.research.agent_io.broad_discovery", "BroadDiscoveryConfig"),
    "BroadDiscoveryRunner": ("project.research.agent_io.broad_discovery", "BroadDiscoveryRunner"),
    "BroadDiscoveryResult": ("project.research.agent_io.broad_discovery", "BroadDiscoveryResult"),
    "FamilyDiscoveryResult": ("project.research.agent_io.broad_discovery", "FamilyDiscoveryResult"),
    "EventAttribution": ("project.research.agent_io.broad_discovery", "EventAttribution"),
    "FamilyEventConfig": ("project.research.agent_io.broad_discovery", "FamilyEventConfig"),
    "run_broad_discovery": ("project.research.agent_io.broad_discovery", "run_broad_discovery"),
    "CampaignPlanResult": ("project.research.agent_io.campaign_planner", "CampaignPlanResult"),
    "CampaignPlanner": ("project.research.agent_io.campaign_planner", "CampaignPlanner"),
    "CampaignPlannerConfig": ("project.research.agent_io.campaign_planner", "CampaignPlannerConfig"),
    "run_campaign_planner_cycle": ("project.research.agent_io.campaign_planner", "run_campaign_planner_cycle"),
    "CycleConfig": ("project.research.agent_io.closed_loop", "CycleConfig"),
    "CycleResult": ("project.research.agent_io.closed_loop", "CycleResult"),
    "CampaignCycleRunner": ("project.research.agent_io.closed_loop", "CampaignCycleRunner"),
    "run_autonomous_cycle": ("project.research.agent_io.closed_loop", "run_autonomous_cycle"),
    "build_run_all_command": (
        "project.research.agent_io.execute_proposal",
        "build_run_all_command",
    ),
    "build_run_all_overrides": (
        "project.research.agent_io.proposal_to_experiment",
        "build_run_all_overrides",
    ),
    "execute_proposal": ("project.research.agent_io.execute_proposal", "execute_proposal"),
    "generate_run_id": ("project.research.agent_io.issue_proposal", "generate_run_id"),
    "issue_proposal": ("project.research.agent_io.issue_proposal", "issue_proposal"),
    "compile_single_hypothesis_to_agent_proposal": (
        "project.research.agent_io.proposal_schema",
        "compile_single_hypothesis_to_agent_proposal",
    ),
    "detect_operator_proposal_format": (
        "project.research.agent_io.proposal_schema",
        "detect_operator_proposal_format",
    ),
    "load_agent_proposal": ("project.research.agent_io.proposal_schema", "load_agent_proposal"),
    "load_operator_proposal": ("project.research.agent_io.proposal_schema", "load_operator_proposal"),
    "proposal_to_experiment_config": (
        "project.research.agent_io.proposal_to_experiment",
        "proposal_to_experiment_config",
    ),
    "translate_and_validate_proposal": (
        "project.research.agent_io.proposal_to_experiment",
        "translate_and_validate_proposal",
    ),
}

__all__ = [
    "AgentProposal",
    "BroadDiscoveryConfig",
    "BroadDiscoveryRunner",
    "BroadDiscoveryResult",
    "FamilyDiscoveryResult",
    "EventAttribution",
    "FamilyEventConfig",
    "run_broad_discovery",
    "CampaignPlanResult",
    "CampaignPlanner",
    "CampaignPlannerConfig",
    "run_campaign_planner_cycle",
    "CycleConfig",
    "CycleResult",
    "CampaignCycleRunner",
    "run_autonomous_cycle",
    "build_run_all_command",
    "build_run_all_overrides",
    "execute_proposal",
    "generate_run_id",
    "issue_proposal",
    "compile_single_hypothesis_to_agent_proposal",
    "detect_operator_proposal_format",
    "load_agent_proposal",
    "load_operator_proposal",
    "proposal_to_experiment_config",
    "translate_and_validate_proposal",
]


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _EXPORTS[name]
    return getattr(import_module(module_name), attr_name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
