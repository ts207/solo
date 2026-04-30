#!/usr/bin/env python3
"""Write the current research no-go/go decision snapshot."""

from __future__ import annotations

import argparse
from pathlib import Path

from project.research.research_decision_snapshot import (
    DEFAULT_OUTPUT_DIR,
    ResearchDecisionSnapshotRequest,
    build_research_decision_snapshot,
    write_research_decision_snapshot,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--mechanism-id", default="funding_squeeze")
    parser.add_argument("--output-dir")
    args = parser.parse_args(argv)

    output_dir = Path(args.output_dir) if args.output_dir else DEFAULT_OUTPUT_DIR
    snapshot = build_research_decision_snapshot(
        ResearchDecisionSnapshotRequest(
            data_root=Path(args.data_root),
            mechanism_id=args.mechanism_id,
        )
    )
    write_research_decision_snapshot(snapshot, output_dir=output_dir)
    print(
        f"Updated {output_dir} "
        f"(mechanism={snapshot['mechanism_id']}, decision={snapshot['decision']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
