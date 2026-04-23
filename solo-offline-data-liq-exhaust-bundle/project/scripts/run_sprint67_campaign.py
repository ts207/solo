"""Sprint 67 autonomous campaign.

Targets the three events with existing bootstrap evidence that have not yet
been run on full real-data (2021–2024):
  - VOL_SHOCK      (highest-priority: n=3739, stability=0.961)
  - LIQUIDATION_CASCADE (high IG: 0.000467)
  - LIQUIDITY_VACUUM    (prior bootstrap evidence)

Scan mode over the full 4-year window. Controller will self-propose,
run, update memory, and loop for max_runs iterations.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from project.core.config import get_data_root
from project.research.campaign_controller import CampaignConfig, CampaignController

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

PROGRAM_ID = "sprint67_vol_liq_btceth_2021_2024"
DATE_START = "2021-01-01"
DATE_END = "2024-10-31"
DEFAULT_MAX_RUNS = 30
DEFAULT_REGISTRY_ROOT = "project/configs/registries"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sprint 67 autonomous campaign runner.")
    parser.add_argument("--program-id", default=PROGRAM_ID)
    parser.add_argument("--registry-root", default=DEFAULT_REGISTRY_ROOT)
    parser.add_argument("--max-runs", type=int, default=DEFAULT_MAX_RUNS)
    parser.add_argument(
        "--research-mode",
        choices=["scan", "exploit", "explore"],
        default="scan",
    )
    parser.add_argument("--report", action="store_true", help="Print campaign health report and exit.")
    return parser


def main() -> None:
    args = build_parser().parse_args()

    if args.report:
        from project.research.services.campaign_memory_rollup_service import (
            build_campaign_memory_rollup,
        )
        rollup = build_campaign_memory_rollup(
            program_id=str(args.program_id).strip(),
            data_root=get_data_root(),
        )
        print(rollup)
        return

    data_root = get_data_root()
    config = CampaignConfig(
        program_id=str(args.program_id).strip(),
        max_runs=int(args.max_runs),
        research_mode=str(args.research_mode).strip(),
        # Seed the frontier with the three bootstrap-evidenced events first.
        # The controller will exhaust these before moving to other event types.
        scan_trigger_types=["EVENT"],
        enable_context_conditioning=True,         # vol_regime + carry_state conditioning
        auto_run_mi_scan=False,                   # Features already built; skip MI scan
        # Full 4-year window for all scan phases (roadmap: A2)
        scan_event_date_scope=(DATE_START, DATE_END),
        scan_general_date_scope=(DATE_START, DATE_END),
        exploit_date_scope=(DATE_START, DATE_END),
        explore_date_scope=(DATE_START, DATE_END),
        repair_date_scope=(DATE_START, "2021-03-31"),  # Narrow repair window for speed
        strict_memory_integrity=True,
    )

    controller = CampaignController(config, data_root, Path(str(args.registry_root).strip()))
    controller.run_campaign()


if __name__ == "__main__":
    main()
