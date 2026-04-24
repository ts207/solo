from __future__ import annotations

import argparse
from pathlib import Path

from project.core.config import get_data_root
from project.research.campaign_controller import CampaignConfig, CampaignController

DEFAULT_PROGRAM_ID = "btc_broad_discovery_2022_2023"
DEFAULT_REGISTRY_ROOT = "project/configs/registries"
DEFAULT_START = "2022-01-01"
DEFAULT_END = "2023-12-31"
DEFAULT_MAX_RUNS = 4


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a tightly bounded broad-discovery frontier scan over BTCUSDT 5m "
            "for a fixed historical window."
        )
    )
    parser.add_argument("--program-id", default=DEFAULT_PROGRAM_ID)
    parser.add_argument("--registry-root", default=DEFAULT_REGISTRY_ROOT)
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--max-runs", type=int, default=DEFAULT_MAX_RUNS)
    parser.add_argument(
        "--research-mode",
        choices=["scan", "exploit", "explore"],
        default="scan",
        help="Use scan for frontier discovery; exploit/explore are allowed but not recommended for first pass.",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Print the current campaign health rollup for program-id and exit.",
    )
    return parser


def _controller_from_args(args: argparse.Namespace) -> CampaignController:
    data_root = get_data_root()
    config = CampaignConfig(
        program_id=str(args.program_id).strip(),
        max_runs=int(args.max_runs),
        research_mode=str(args.research_mode).strip(),
        scan_trigger_types=["EVENT"],
        enable_context_conditioning=False,
        auto_run_mi_scan=False,
        scan_event_date_scope=(str(args.start), str(args.end)),
        scan_general_date_scope=(str(args.start), str(args.end)),
        exploit_date_scope=(str(args.start), str(args.end)),
        explore_date_scope=(str(args.start), str(args.end)),
        repair_date_scope=(str(args.start), str(args.end)),
        strict_memory_integrity=True,
    )
    return CampaignController(config, data_root, Path(args.registry_root))


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

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

    controller = _controller_from_args(args)
    controller.run_campaign()


if __name__ == "__main__":
    main()
