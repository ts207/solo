#!/usr/bin/env python3
"""Write a structured mechanism decision artifact."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from project.research.mechanism_decisions import (
    forced_flow_reversal_pause_decision,
    write_mechanism_decision,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mechanism-id", required=True)
    parser.add_argument("--data-root", default="data")
    args = parser.parse_args(argv)

    if args.mechanism_id != "forced_flow_reversal":
        raise SystemExit(f"No built-in decision template for {args.mechanism_id}")
    paths = write_mechanism_decision(
        forced_flow_reversal_pause_decision(),
        data_root=Path(args.data_root),
    )
    print(json.dumps(paths, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
