#!/usr/bin/env python3
"""Extract candidate-level event return traces."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from project.research.candidate_traces import extract_candidate_traces


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--candidate-id", required=True)
    parser.add_argument("--data-root", default="data")
    args = parser.parse_args(argv)

    report = extract_candidate_traces(
        run_id=args.run_id,
        candidate_id=args.candidate_id,
        data_root=Path(args.data_root),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report.get("status") == "extracted" else 1


if __name__ == "__main__":
    raise SystemExit(main())
