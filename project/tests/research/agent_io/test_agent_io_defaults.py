from __future__ import annotations

from pathlib import Path

from project.research.agent_io.broad_discovery import BroadDiscoveryConfig
from project.research.agent_io.campaign_planner import CampaignPlannerConfig


def test_campaign_planner_default_entry_lags_fail_closed() -> None:
    config = CampaignPlannerConfig(program_id="program", registry_root=Path("/tmp/registry"))
    assert config.entry_lags == (1,)


def test_broad_discovery_default_entry_lags_fail_closed() -> None:
    config = BroadDiscoveryConfig(
        program_id="program",
        family="VOLATILITY_TRANSITION",
        registry_root=Path("/tmp/registry"),
    )
    assert config.entry_lags == (1,)
