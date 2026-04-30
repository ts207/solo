#!/usr/bin/env python3
"""Run read-only market-context data quality audits for mechanism observables."""

from __future__ import annotations

import argparse
from pathlib import Path

from project.research.data_quality_audit import (
    DEFAULT_OUTPUT_ROOT,
    DataQualityAuditRequest,
    default_run_id,
    run_data_quality_audit,
    write_data_quality_audit_outputs,
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
        symbols = _parse_symbols(args.symbols)
        data_root = Path(args.data_root)
        request = DataQualityAuditRequest(
            run_id=args.run_id,
            symbols=symbols,
            data_root=data_root,
            source_run_id=args.source_run_id,
            timeframe=args.timeframe,
        )
        field_rows, mechanism_payload = run_data_quality_audit(request)
    except ValueError as exc:
        print(f"fail: {exc}")
        return 1

    output_root = Path(args.output_root) if args.output_root else data_root / "reports" / "data_quality_audit"
    if args.output_root is None and DEFAULT_OUTPUT_ROOT.name != "data_quality_audit":
        output_root = DEFAULT_OUTPUT_ROOT
    output_dir = output_root / args.run_id
    write_data_quality_audit_outputs(field_rows, mechanism_payload, output_dir=output_dir)
    print(f"Updated {output_dir} (rows={len(field_rows)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
