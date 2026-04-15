from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional

from project.core.config import get_data_root
from project.core.timeframes import normalize_timeframe
from project.research.services.candidate_discovery_service import (
    CandidateDiscoveryConfig,
    CandidateDiscoveryResult,
    execute_candidate_discovery,
)


def build_candidate_discovery_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Discover Phase 2 candidates.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--data_root", default=None, help="Optional data root override.")
    parser.add_argument("--event_type", default="all")
    parser.add_argument("--templates", nargs="+", help="Subset of templates to run.")
    parser.add_argument("--horizons", nargs="+", help="Subset of horizons to run.")
    parser.add_argument("--directions", nargs="+", help="Subset of directions to run.")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--horizon_bars", type=int, default=24)
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--run_mode", default="exploratory")
    parser.add_argument("--split_scheme_id", default="WF_60_20_20")
    parser.add_argument("--embargo_bars", type=int, default=0)
    parser.add_argument("--purge_bars", type=int, default=0)
    parser.add_argument("--train_only_lambda_used", type=float, default=0.0)
    parser.add_argument("--discovery_profile", default="standard")
    parser.add_argument("--candidate_generation_method", default="phase2_v1")
    parser.add_argument("--concept_file", default=None)
    parser.add_argument("--entry_lag_bars", type=int, default=1)
    parser.add_argument("--entry_lags", nargs="+", type=int, help="Subset of entry lags to run.")
    parser.add_argument("--shift_labels_k", type=int, default=0)
    parser.add_argument("--fees_bps", type=float, default=None)
    parser.add_argument("--slippage_bps", type=float, default=None)
    parser.add_argument("--cost_bps", type=float, default=None)
    parser.add_argument("--cost_calibration_mode", default="auto")
    parser.add_argument("--cost_min_tob_coverage", type=float, default=0.6)
    parser.add_argument("--cost_tob_tolerance_minutes", type=int, default=5)
    parser.add_argument("--candidate_origin_run_id", default=None)
    parser.add_argument("--frozen_spec_hash", default=None)
    parser.add_argument("--program_id", help="Program ID for campaign tracking.")
    parser.add_argument("--search_budget", type=int, help="Limit total candidate expansions.")
    parser.add_argument("--experiment_config", help="Path to experiment config YAML.")
    parser.add_argument(
        "--registry_root",
        default=None,
        help="Optional registry root for experiment-plan discovery.",
    )
    parser.add_argument("--min_validation_n_obs", type=int, default=None)
    parser.add_argument("--min_test_n_obs", type=int, default=None)
    parser.add_argument("--min_total_n_obs", type=int, default=None)
    return parser


def candidate_discovery_config_from_namespace(args: argparse.Namespace) -> CandidateDiscoveryConfig:
    run_mode = str(args.run_mode).strip().lower()
    if run_mode in {"discovery", "research"}:
        run_mode = "exploratory"
    return CandidateDiscoveryConfig(
        run_id=str(args.run_id),
        symbols=tuple(s.strip().upper() for s in str(args.symbols).split(",") if s.strip()),
        config_paths=tuple(str(path) for path in getattr(args, "config", []) or ()),
        data_root=Path(args.data_root) if args.data_root else get_data_root(),
        event_type=str(args.event_type),
        templates=tuple(args.templates) if args.templates else None,
        horizons=tuple(args.horizons) if args.horizons else None,
        directions=tuple(args.directions) if args.directions else None,
        timeframe=normalize_timeframe(str(args.timeframe or "5m")),
        horizon_bars=int(args.horizon_bars),
        out_dir=Path(args.out_dir) if args.out_dir else None,
        run_mode=run_mode,
        split_scheme_id=str(args.split_scheme_id),
        embargo_bars=int(args.embargo_bars),
        purge_bars=int(args.purge_bars),
        train_only_lambda_used=float(args.train_only_lambda_used),
        discovery_profile=str(args.discovery_profile),
        candidate_generation_method=str(args.candidate_generation_method),
        concept_file=str(args.concept_file) if args.concept_file else None,
        entry_lag_bars=int(args.entry_lag_bars),
        entry_lags=tuple(args.entry_lags) if args.entry_lags else None,
        shift_labels_k=int(args.shift_labels_k),
        fees_bps=None if args.fees_bps is None else float(args.fees_bps),
        slippage_bps=None if args.slippage_bps is None else float(args.slippage_bps),
        cost_bps=None if args.cost_bps is None else float(args.cost_bps),
        cost_calibration_mode=str(args.cost_calibration_mode),
        cost_min_tob_coverage=float(args.cost_min_tob_coverage),
        cost_tob_tolerance_minutes=int(args.cost_tob_tolerance_minutes),
        candidate_origin_run_id=str(args.candidate_origin_run_id)
        if args.candidate_origin_run_id
        else None,
        frozen_spec_hash=str(args.frozen_spec_hash) if args.frozen_spec_hash else None,
        program_id=str(args.program_id) if args.program_id else None,
        search_budget=int(args.search_budget) if args.search_budget is not None else None,
        experiment_config=str(args.experiment_config) if args.experiment_config else None,
        registry_root=Path(args.registry_root) if args.registry_root else None,
        min_validation_n_obs=None
        if args.min_validation_n_obs is None
        else int(args.min_validation_n_obs),
        min_test_n_obs=None if args.min_test_n_obs is None else int(args.min_test_n_obs),
        min_total_n_obs=None if args.min_total_n_obs is None else int(args.min_total_n_obs),
    )


def parse_candidate_discovery_argv(argv: Optional[List[str]] = None) -> CandidateDiscoveryConfig:
    parser = build_candidate_discovery_parser()
    args, _unknown = parser.parse_known_args(argv)
    return candidate_discovery_config_from_namespace(args)


def run_candidate_discovery_cli(argv: Optional[List[str]] = None) -> CandidateDiscoveryResult:
    return execute_candidate_discovery(parse_candidate_discovery_argv(argv))


def main(argv: Optional[List[str]] = None) -> int:
    return int(run_candidate_discovery_cli(argv).exit_code)


if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv[1:]))
