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
    from project.research.template_regression import build_run_summary
    repo_root = _repo_root()
    data_root = get_data_root()
    parser = argparse.ArgumentParser(
        description="Build baseline fixture with template/action/direction summaries from a phase2 run."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument(
        "--events",
        default="VOL_SHOCK,LIQUIDITY_VACUUM,BASIS_DISLOC",
        help="Comma-separated event types.",
    )
    parser.add_argument(
        "--out",
        default=str(repo_root / "tests" / "fixtures" / "phase2_template_summary_baseline.json"),
    )
    args = parser.parse_args()

    events = [token.strip().upper() for token in str(args.events).split(",") if token.strip()]
    summary = build_run_summary(data_root=data_root, run_id=str(args.run_id), events=events)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
