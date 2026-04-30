#!/usr/bin/env python3
"""Extract candidate control traces for specificity reporting."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from project.research.control_traces import build_control_traces, result_to_jsonable


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--candidate-id", required=True)
    parser.add_argument("--data-root", default="data")
    args = parser.parse_args(argv)

    result = build_control_traces(
        run_id=args.run_id,
        candidate_id=args.candidate_id,
        data_root=Path(args.data_root),
    )
    print(json.dumps(result_to_jsonable(result), indent=2, sort_keys=True))
    return 0 if result.status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
