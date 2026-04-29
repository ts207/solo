#!/usr/bin/env python3
"""Build a governed reproduction report from existing run artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from project.research.governed_reproduction import run_governed_reproduction


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-run-id", required=True)
    parser.add_argument("--reproduction-run-id", required=True)
    parser.add_argument("--candidate-id")
    parser.add_argument("--data-root", default="data")
    args = parser.parse_args(argv)

    report = run_governed_reproduction(
        source_run_id=args.source_run_id,
        reproduction_run_id=args.reproduction_run_id,
        candidate_id=args.candidate_id,
        data_root=Path(args.data_root),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("status") in {"pass", "unknown"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
