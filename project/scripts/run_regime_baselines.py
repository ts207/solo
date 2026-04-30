#!/usr/bin/env python3
"""Run predeclared regime baseline studies."""

from __future__ import annotations

import argparse
from pathlib import Path

from project.research.regime_baselines import (
    RegimeBaselineRequest,
    run_regime_baselines,
    write_regime_baseline_outputs,
)


def _parse_csv(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in str(value or "").split(",") if item.strip())


def _parse_horizons(value: str) -> tuple[int, ...]:
    return tuple(int(item) for item in _parse_csv(value))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--matrix-id", default="core_v1")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    parser.add_argument("--horizons", default="12,24,48")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--source-run-id")
    parser.add_argument("--output-root")
    args = parser.parse_args(argv)

    data_root = Path(args.data_root)
    request = RegimeBaselineRequest(
        run_id=args.run_id,
        matrix_id=args.matrix_id,
        symbols=_parse_csv(args.symbols),
        horizons=_parse_horizons(args.horizons),
        data_root=data_root,
        source_run_id=args.source_run_id,
    )
    try:
        df, burden, source_run_id = run_regime_baselines(request)
    except ValueError as exc:
        print(f"fail: {exc}")
        return 1
    output_root = Path(args.output_root) if args.output_root else data_root / "reports" / "regime_baselines"
    if not output_root.is_absolute():
        output_root = data_root / "reports" / "regime_baselines"
    output_dir = output_root / args.run_id
    write_regime_baseline_outputs(
        df,
        burden,
        output_dir=output_dir,
        source_run_id=source_run_id,
    )
    print(f"Updated {output_dir} ({len(df)} rows, source_run_id={source_run_id or ''})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
