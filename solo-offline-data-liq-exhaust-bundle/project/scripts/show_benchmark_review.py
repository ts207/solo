import argparse
import json
import logging
import os
from pathlib import Path
from typing import List, Optional

from project import PROJECT_ROOT

logger = logging.getLogger(__name__)


def get_data_root() -> Path:
    """Return the canonical data root from environment or default."""
    return Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))


def find_latest_review(data_root: Path | None = None) -> Path | None:
    """Find the most recently modified benchmark review file, preferring canonical."""
    root = data_root or get_data_root()
    
    # Priority 1: Canonical 'benchmarks'
    canonical_path = root / "reports" / "benchmarks" / "history"
    if canonical_path.exists():
        reviews = list(canonical_path.glob("**/benchmark_review.json"))
        if reviews:
            return max(reviews, key=lambda x: x.stat().st_mtime)
            
    # Priority 2: Legacy 'perf_benchmarks'
    legacy_path = root / "reports" / "perf_benchmarks" / "history"
    if legacy_path.exists():
        reviews = list(legacy_path.glob("**/benchmark_review.json"))
        if reviews:
            return max(reviews, key=lambda x: x.stat().st_mtime)
 
    return None


def find_historical_reviews(matrix_id: str, limit: int = 5) -> list[Path]:
    """Return the N latest review paths for a specific matrix_id."""
    root = get_data_root()
    search_paths = [
        root / "reports" / "benchmarks" / "history",
        root / "reports" / "perf_benchmarks" / "history",
    ]
 
    matches: list[Path] = []
    for p in search_paths:
        path_matches = []
        if p.exists():
            # Look for directories starting with matrix_id
            for d in p.iterdir():
                if d.is_dir() and d.name.startswith(f"{matrix_id}_"):
                    review_file = d / "benchmark_review.json"
                    if review_file.exists():
                        path_matches.append(review_file)
        # Sort within this priority level by mtime descending
        path_matches.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        matches.extend(path_matches)
 
    return matches[:limit]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark_run_id", type=str, help="Specific benchmark run ID")
    parser.add_argument("--latest", action="store_true", help="Show latest benchmark")
    args = parser.parse_args()

    reports_dir = Path("data/reports/benchmarks")
    if not reports_dir.exists():
        print("No benchmarks found.")
        return 1
        
    run_dir = None
    if args.latest:
        runs = sorted([d for d in reports_dir.iterdir() if d.is_dir()], key=lambda x: x.stat().st_mtime, reverse=True)
        if not runs:
            print("No benchmarks found.")
            return 1
        run_dir = runs[0]
    elif args.benchmark_run_id:
        run_dir = reports_dir / args.benchmark_run_id
        if not run_dir.exists():
            print(f"Benchmark {args.benchmark_run_id} not found.")
            return 1
    else:
        print("Please provide --benchmark_run_id or --latest")
        return 1

    summary_path = run_dir / "benchmark_summary.json"
    if not summary_path.exists():
        print(f"No summary found at {summary_path}")
        return 1
        
    with open(summary_path, "r", encoding="utf-8") as f:
        summary_data = json.load(f)
        
    print(f"=== Benchmark Review: {run_dir.name} ===")
    
    # Process summaries - handle both legacy and new schemas
    slices = summary_data.get("slices", [])
    if not slices:
        # Legacy schema: single summary object
        if "run_id" in summary_data or "slice_id" in summary_data:
            slices = [summary_data]
        else:
            print("No valid summary data found.")
            return 1
    
    for s in slices:
        slice_id = s.get("run_id") or s.get("slice_id") or "unknown"
        status = s.get("status") or ("PASS" if s.get("benchmark_pass") else "FAIL")
        print(f"\nSlice: {slice_id} | Status: {status}")
        
        # Show key metrics if available
        expectancy = s.get("top_n_median_after_cost_expectancy_bps")
        delta = s.get("delta_after_cost_expectancy_vs_baseline")
        fold_consistency = s.get("top_n_median_fold_sign_consistency")
        
        if expectancy is not None:
            print(f"  Expectancy BPS: {expectancy:.2f}")
        if delta is not None:
            print(f"  Delta vs Baseline: {delta:.2f}")
        if fold_consistency is not None:
            print(f"  Fold Sign Consistency: {fold_consistency:.2f}")
        
        # Show failed thresholds if any
        failed = s.get("failed_thresholds") or s.get("failed_checks") or []
        if failed:
            print(f"  Failed: {', '.join(str(f) for f in failed)}")
        
        # Show pass/fail gate status
        benchmark_pass = s.get("benchmark_pass")
        if benchmark_pass is not None:
            gate_status = "PASS" if benchmark_pass else "FAIL"
            print(f"  Gate: {gate_status}")
        
if __name__ == "__main__":
    main()
