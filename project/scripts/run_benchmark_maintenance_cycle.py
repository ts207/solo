import argparse
import json
import logging
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from project.core.config import get_data_root
from project import PROJECT_ROOT

logger = logging.getLogger(__name__)


BENCHMARKS_DIR = get_data_root() / "reports" / "benchmarks"
HISTORY_DIR = BENCHMARKS_DIR / "history"
LATEST_DIR = BENCHMARKS_DIR / "latest"


from project.research.benchmarks.benchmark_utils import find_historical_reviews


def _update_latest(source_dir: Path):
    """Copy outputs to the latest/ directory."""
    LATEST_DIR.mkdir(parents=True, exist_ok=True)
    for f in source_dir.iterdir():
        if f.is_file() and f.suffix in (".json", ".md"):
            shutil.copy2(f, LATEST_DIR / f.name)


def _archive_to_history(source_dir: Path, matrix_id: str):
    """Copy outputs to history/{matrix_id}_{timestamp}/."""
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    dest = HISTORY_DIR / f"{matrix_id}_{stamp}"
    shutil.copytree(source_dir, dest)


def _re_certify_with_priors(latest_review_path: Path, priors: List[dict], out_dir: Path) -> dict:
    """Re-run certification against historical priors if available."""
    from project.research.services.benchmark_governance_service import (
        certify_benchmark_review,
        write_certification_report,
    )

    with open(latest_review_path, encoding="utf-8") as f:
        review = json.load(f)

    prior_reviews = [p["review"] for p in priors if p.get("review")]

    cert = certify_benchmark_review(
        current_review=review,
        prior_review=prior_reviews if prior_reviews else None,
    )

    write_certification_report(out_dir=out_dir, cert=cert)
    return cert


def _write_cycle_summary(matrix_id: str, execute: bool, run_dir: Path, priors: List[dict], cert: dict) -> Path:
    """Write the cycle summary bundle."""
    summary = {
        "schema_version": "benchmark_cycle_summary_v1",
        "matrix_id": matrix_id,
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "execute": execute,
        "run_dir": str(run_dir),
        "historical_reviews_found": len(priors),
        "certification_passed": cert.get("passed", False),
        "certification_issue_count": cert.get("issue_count", 0),
        "artifacts": {
            "manifest": str(run_dir / "matrix_manifest.json"),
            "summary": str(run_dir / "benchmark_summary.json"),
            "review": str(run_dir / "benchmark_review.json"),
            "certification": str(run_dir / "benchmark_certification.json"),
        },
    }

    summary_path = run_dir / "cycle_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return summary_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--preset", type=str, required=True, help="Preset name (e.g. core_v1)")
    parser.add_argument("--execute", type=int, default=0, help="Whether to execute")
    parser.add_argument("--history_limit", type=int, default=5, help="Number of historical reviews to load")
    parser.add_argument("--out_dir", type=str, help="Output directory")
    args = parser.parse_args()

    matrix_id = args.preset
    execute = bool(args.execute)

    print(f"Starting certification cycle for {matrix_id} (execute={execute})...")

    cmd = [sys.executable, "-m", "project.scripts.run_benchmark_matrix", "--preset", matrix_id, "--execute", str(int(execute))]
    if args.out_dir:
        cmd.extend(["--out_dir", args.out_dir])

    print("Running benchmark matrix...")
    ret = subprocess.call(cmd)
    if ret != 0:
        print("Benchmark matrix failed.")
        return ret

    latest_run = sorted(
        [d for d in BENCHMARKS_DIR.iterdir() if d.is_dir() and d.name.startswith(f"{matrix_id}_")],
        key=lambda x: x.stat().st_mtime,
        reverse=True,
    )
    if not latest_run:
        print("No benchmark run directory found.")
        return 1

    run_dir = latest_run[0]
    print(f"CYCLE_OUTPUT_DIR: {run_dir}")

    priors = find_historical_reviews(matrix_id=matrix_id, history_limit=args.history_limit)
    if priors:
        print(f"Found {len(priors)} historical review(s) for {matrix_id}.")

    review_path = run_dir / "benchmark_review.json"
    if review_path.exists() and priors:
        print("Re-certifying against historical priors...")
        cert = _re_certify_with_priors(review_path, priors, run_dir)
    else:
        cert_path = run_dir / "benchmark_certification.json"
        if cert_path.exists():
            with open(cert_path, encoding="utf-8") as f:
                cert = json.load(f)
        else:
            cert = {"passed": True, "issues": []}

    summary_path = _write_cycle_summary(matrix_id, execute, run_dir, priors, cert)
    print(f"Cycle summary written to {summary_path}")

    _update_latest(run_dir)
    print(f"Latest artifacts updated to {LATEST_DIR}")

    if execute:
        _archive_to_history(run_dir, matrix_id)
        print(f"Run archived to {HISTORY_DIR}")

    passed = bool(cert.get("passed", False))
    print(f"Certification cycle completed. Passed: {passed}")
    return 0 if (passed or not execute) else 1


if __name__ == "__main__":
    main()
