#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from project import PROJECT_ROOT
from project.core.config import get_data_root


def _repo_root() -> Path:
    return PROJECT_ROOT.parent

def main() -> int:
    from project.research.template_regression import build_run_summary, compare_summaries
    repo_root = _repo_root()
    data_root = get_data_root()
    parser = argparse.ArgumentParser(
        description="Compare phase2 template/action/direction summary for a run against a baseline fixture."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument(
        "--baseline",
        default=str(repo_root / "tests" / "fixtures" / "phase2_template_summary_baseline.json"),
    )
    parser.add_argument(
        "--events",
        default="VOL_SHOCK,LIQUIDITY_VACUUM,BASIS_DISLOC",
        help="Comma-separated event types.",
    )
    args = parser.parse_args()

    baseline_path = Path(args.baseline)
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    events = [token.strip().upper() for token in str(args.events).split(",") if token.strip()]
    current = build_run_summary(data_root=data_root, run_id=str(args.run_id), events=events)
    failures = compare_summaries(baseline=baseline, current=current)
    if failures:
        print("Baseline regression mismatches detected:")
        for row in failures:
            print(f"  - {row}")
        return 1
    print("Baseline regression check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
