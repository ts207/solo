from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional

from project.research.services.promotion_service import (
    PROMOTION_CONFIG_DEFAULTS,
    PromotionConfig,
    PromotionServiceResult,
    build_promotion_config,
    execute_promotion,
)


def build_promotion_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Promote candidates.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--max_q_value", type=float, default=PROMOTION_CONFIG_DEFAULTS["max_q_value"])
    parser.add_argument("--min_events", type=int, default=PROMOTION_CONFIG_DEFAULTS["min_events"])
    parser.add_argument("--min_stability_score", type=float, default=PROMOTION_CONFIG_DEFAULTS["min_stability_score"])
    parser.add_argument("--min_sign_consistency", type=float, default=PROMOTION_CONFIG_DEFAULTS["min_sign_consistency"])
    parser.add_argument("--min_cost_survival_ratio", type=float, default=PROMOTION_CONFIG_DEFAULTS["min_cost_survival_ratio"])
    parser.add_argument("--max_negative_control_pass_rate", type=float, default=PROMOTION_CONFIG_DEFAULTS["max_negative_control_pass_rate"])
    parser.add_argument("--min_tob_coverage", type=float, default=PROMOTION_CONFIG_DEFAULTS["min_tob_coverage"])
    parser.add_argument("--require_hypothesis_audit", type=int, default=int(PROMOTION_CONFIG_DEFAULTS["require_hypothesis_audit"]))
    parser.add_argument("--allow_missing_negative_controls", type=int, default=int(PROMOTION_CONFIG_DEFAULTS["allow_missing_negative_controls"]))
    parser.add_argument("--require_multiplicity_diagnostics", type=int, default=int(PROMOTION_CONFIG_DEFAULTS["require_multiplicity_diagnostics"]))
    parser.add_argument("--min_dsr", type=float, default=PROMOTION_CONFIG_DEFAULTS["min_dsr"])
    parser.add_argument("--max_overlap_ratio", type=float, default=PROMOTION_CONFIG_DEFAULTS["max_overlap_ratio"])
    parser.add_argument("--max_profile_correlation", type=float, default=PROMOTION_CONFIG_DEFAULTS["max_profile_correlation"])
    parser.add_argument("--allow_discovery_promotion", type=int, default=int(PROMOTION_CONFIG_DEFAULTS["allow_discovery_promotion"]))
    parser.add_argument("--program_id", default=PROMOTION_CONFIG_DEFAULTS["program_id"])
    parser.add_argument("--retail_profile", default=PROMOTION_CONFIG_DEFAULTS["retail_profile"])
    parser.add_argument("--objective_name", default=PROMOTION_CONFIG_DEFAULTS["objective_name"])
    parser.add_argument("--objective_spec", default=None)
    parser.add_argument("--retail_profiles_spec", default=None)
    parser.add_argument(
        "--promotion_profile",
        choices=["auto", "research", "deploy"],
        default=PROMOTION_CONFIG_DEFAULTS["promotion_profile"],
    )
    return parser


def promotion_config_from_namespace(args: argparse.Namespace) -> PromotionConfig:
    return build_promotion_config(
        run_id=str(args.run_id),
        symbols=str(args.symbols),
        out_dir=Path(args.out_dir) if args.out_dir else None,
        max_q_value=float(args.max_q_value),
        min_events=int(args.min_events),
        min_stability_score=float(args.min_stability_score),
        min_sign_consistency=float(args.min_sign_consistency),
        min_cost_survival_ratio=float(args.min_cost_survival_ratio),
        max_negative_control_pass_rate=float(args.max_negative_control_pass_rate),
        min_tob_coverage=float(args.min_tob_coverage),
        require_hypothesis_audit=bool(args.require_hypothesis_audit),
        allow_missing_negative_controls=bool(args.allow_missing_negative_controls),
        require_multiplicity_diagnostics=bool(args.require_multiplicity_diagnostics),
        min_dsr=float(args.min_dsr),
        max_overlap_ratio=float(args.max_overlap_ratio),
        max_profile_correlation=float(args.max_profile_correlation),
        allow_discovery_promotion=bool(args.allow_discovery_promotion),
        program_id=str(args.program_id),
        retail_profile=str(args.retail_profile),
        objective_name=str(args.objective_name),
        objective_spec=str(args.objective_spec) if args.objective_spec else None,
        retail_profiles_spec=str(args.retail_profiles_spec) if args.retail_profiles_spec else None,
        promotion_profile=str(args.promotion_profile),
    )


def parse_promotion_argv(argv: Optional[List[str]] = None) -> PromotionConfig:
    parser = build_promotion_parser()
    args = parser.parse_args(argv)
    return promotion_config_from_namespace(args)


def run_promotion_cli(argv: Optional[List[str]] = None) -> PromotionServiceResult:
    return execute_promotion(parse_promotion_argv(argv))


def main(argv: Optional[List[str]] = None) -> int:
    return int(run_promotion_cli(argv).exit_code)


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv[1:]))
