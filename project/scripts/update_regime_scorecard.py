#!/usr/bin/env python3
"""Aggregate completed regime baseline runs into a regime scorecard."""

from __future__ import annotations

import argparse
from pathlib import Path

from project.research.regime_scorecard import update_regime_scorecard


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--run-id")
    parser.add_argument("--matrix-id")
    parser.add_argument("--all-runs", action="store_true")
    parser.add_argument("--output-dir")
    args = parser.parse_args(argv)

    data_root = Path(args.data_root)
    output_dir = Path(args.output_dir) if args.output_dir else None
    if output_dir is not None and not output_dir.is_absolute():
        output_dir = Path(output_dir)
    scorecard = update_regime_scorecard(
        data_root=data_root,
        run_id=args.run_id,
        matrix_id=args.matrix_id,
        all_runs=bool(args.all_runs),
        output_dir=output_dir,
    )
    out_dir = output_dir or data_root / "reports" / "regime_baselines"
    print(f"Updated {out_dir} ({len(scorecard)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
