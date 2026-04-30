#!/usr/bin/env python3
"""Diagnose whether funding fields are truly stale or cadence-valid stepwise data."""

from __future__ import annotations

import argparse
from pathlib import Path

from project.research.funding_data_triage import (
    FundingDataTriageRequest,
    default_run_id,
    run_funding_data_triage,
    write_funding_data_triage_outputs,
)


def _parse_symbols(value: str) -> tuple[str, ...]:
    symbols = tuple(item.strip().upper() for item in str(value or "").split(",") if item.strip())
    if not symbols:
        raise ValueError("--symbols must include at least one symbol")
    return symbols


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=default_run_id())
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--source-run-id")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--output-root")
    args = parser.parse_args(argv)

    try:
        data_root = Path(args.data_root)
        rows = run_funding_data_triage(
            FundingDataTriageRequest(
                run_id=args.run_id,
                symbols=_parse_symbols(args.symbols),
                data_root=data_root,
                source_run_id=args.source_run_id,
                timeframe=args.timeframe,
            )
        )
    except ValueError as exc:
        print(f"fail: {exc}")
        return 1

    output_root = Path(args.output_root) if args.output_root else data_root / "reports" / "funding_data_triage"
    output_dir = output_root / args.run_id
    write_funding_data_triage_outputs(rows, output_dir=output_dir)
    print(f"Updated {output_dir} (rows={len(rows)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
