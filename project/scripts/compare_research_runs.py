#!/usr/bin/env python3
from __future__ import annotations

from project.core.config import get_data_root

import argparse
import json
from pathlib import Path

from project.research.services.run_comparison_service import (
    DEFAULT_DRIFT_THRESHOLDS,
    write_run_comparison_report,
)


DATA_ROOT = get_data_root()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare phase2 and promotion diagnostics between two research runs."
    )
    parser.add_argument("--baseline_run_id", required=True)
    parser.add_argument("--candidate_run_id", required=True)
    parser.add_argument(
        "--data_root",
        default=str(DATA_ROOT),
        help="Data root path (default: BACKTEST_DATA_ROOT or repo/data).",
    )
    parser.add_argument(
        "--out_dir",
        default="",
        help="Output directory (default: data/reports/research_comparison/<candidate>/vs_<baseline>).",
    )
    parser.add_argument(
        "--report_out",
        default="",
        help="Optional output path for the comparison JSON report.",
    )
    parser.add_argument(
        "--summary_out",
        default="",
        help="Optional output path for the markdown summary report.",
    )
    parser.add_argument("--drift_mode", choices=["off", "warn", "enforce"], default="warn")
    parser.add_argument(
        "--max_phase2_candidate_count_delta_abs",
        type=float,
        default=DEFAULT_DRIFT_THRESHOLDS["max_phase2_candidate_count_delta_abs"],
    )
    parser.add_argument(
        "--max_phase2_survivor_count_delta_abs",
        type=float,
        default=DEFAULT_DRIFT_THRESHOLDS["max_phase2_survivor_count_delta_abs"],
    )
    parser.add_argument(
        "--max_phase2_zero_eval_rows_increase",
        type=float,
        default=DEFAULT_DRIFT_THRESHOLDS["max_phase2_zero_eval_rows_increase"],
    )
    parser.add_argument(
        "--max_phase2_survivor_q_value_increase",
        type=float,
        default=DEFAULT_DRIFT_THRESHOLDS["max_phase2_survivor_q_value_increase"],
    )
    parser.add_argument(
        "--max_phase2_survivor_estimate_bps_drop",
        type=float,
        default=DEFAULT_DRIFT_THRESHOLDS["max_phase2_survivor_estimate_bps_drop"],
    )
    parser.add_argument(
        "--max_promotion_promoted_count_delta_abs",
        type=float,
        default=DEFAULT_DRIFT_THRESHOLDS["max_promotion_promoted_count_delta_abs"],
    )
    parser.add_argument(
        "--max_reject_reason_shift_abs",
        type=float,
        default=DEFAULT_DRIFT_THRESHOLDS["max_reject_reason_shift_abs"],
    )
    parser.add_argument(
        "--max_edge_tradable_count_delta_abs",
        type=float,
        default=DEFAULT_DRIFT_THRESHOLDS["max_edge_tradable_count_delta_abs"],
    )
    parser.add_argument(
        "--max_edge_candidate_count_delta_abs",
        type=float,
        default=DEFAULT_DRIFT_THRESHOLDS["max_edge_candidate_count_delta_abs"],
    )
    parser.add_argument(
        "--max_edge_after_cost_positive_validation_count_delta_abs",
        type=float,
        default=DEFAULT_DRIFT_THRESHOLDS["max_edge_after_cost_positive_validation_count_delta_abs"],
    )
    parser.add_argument(
        "--max_edge_median_resolved_cost_bps_delta_abs",
        type=float,
        default=DEFAULT_DRIFT_THRESHOLDS["max_edge_median_resolved_cost_bps_delta_abs"],
    )
    parser.add_argument(
        "--max_edge_median_expectancy_bps_delta_abs",
        type=float,
        default=DEFAULT_DRIFT_THRESHOLDS["max_edge_median_expectancy_bps_delta_abs"],
    )
    args = parser.parse_args()

    data_root = Path(args.data_root).resolve()
    out_dir = (
        Path(args.out_dir).resolve()
        if str(args.out_dir).strip()
        else data_root
        / "reports"
        / "research_comparison"
        / args.candidate_run_id
        / f"vs_{args.baseline_run_id}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    report_out = (
        Path(args.report_out).resolve()
        if str(args.report_out).strip()
        else out_dir / "research_run_comparison.json"
    )
    summary_out = (
        Path(args.summary_out).resolve()
        if str(args.summary_out).strip()
        else out_dir / "research_run_comparison_summary.md"
    )
    output_path = write_run_comparison_report(
        data_root=data_root,
        baseline_run_id=args.baseline_run_id,
        candidate_run_id=args.candidate_run_id,
        out_dir=out_dir,
        report_out=report_out,
        summary_out=summary_out,
        drift_mode=str(args.drift_mode),
        thresholds={
            "max_phase2_candidate_count_delta_abs": float(
                args.max_phase2_candidate_count_delta_abs
            ),
            "max_phase2_survivor_count_delta_abs": float(args.max_phase2_survivor_count_delta_abs),
            "max_phase2_zero_eval_rows_increase": float(args.max_phase2_zero_eval_rows_increase),
            "max_phase2_survivor_q_value_increase": float(
                args.max_phase2_survivor_q_value_increase
            ),
            "max_phase2_survivor_estimate_bps_drop": float(
                args.max_phase2_survivor_estimate_bps_drop
            ),
            "max_promotion_promoted_count_delta_abs": float(
                args.max_promotion_promoted_count_delta_abs
            ),
            "max_reject_reason_shift_abs": float(args.max_reject_reason_shift_abs),
            "max_edge_tradable_count_delta_abs": float(args.max_edge_tradable_count_delta_abs),
            "max_edge_candidate_count_delta_abs": float(args.max_edge_candidate_count_delta_abs),
            "max_edge_after_cost_positive_validation_count_delta_abs": float(
                args.max_edge_after_cost_positive_validation_count_delta_abs
            ),
            "max_edge_median_resolved_cost_bps_delta_abs": float(
                args.max_edge_median_resolved_cost_bps_delta_abs
            ),
            "max_edge_median_expectancy_bps_delta_abs": float(
                args.max_edge_median_expectancy_bps_delta_abs
            ),
        },
    )
    report = json.loads(output_path.read_text(encoding="utf-8"))
    comparison = dict(report.get("comparison", {}))
    assessment = dict(report.get("assessment", {}))
    print(
        json.dumps(
            {
                "baseline_run_id": args.baseline_run_id,
                "candidate_run_id": args.candidate_run_id,
                "report": str(output_path),
                "summary": str(summary_out),
                "assessment_status": assessment.get("status", "unknown"),
                "phase2_candidate_delta": comparison["phase2"]["delta"]["candidate_count"],
                "promotion_promoted_delta": comparison["promotion"]["delta"]["promoted_count"],
                "edge_tradable_delta": comparison["edge_candidates"]["delta"]["tradable_count"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if assessment.get("status") == "fail" else 0


if __name__ == "__main__":
    raise SystemExit(main())
