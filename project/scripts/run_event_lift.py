#!/usr/bin/env python3
"""Run event incremental-lift tests behind regime scorecard gating."""

from __future__ import annotations

import argparse
from pathlib import Path

from project.research.event_lift import (
    EventLiftGateError,
    EventLiftRequest,
    run_event_lift,
    write_event_lift_outputs,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--mechanism-id", required=True)
    parser.add_argument("--regime-id", required=True)
    parser.add_argument("--event-id", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--direction", required=True, choices=["long", "short"])
    parser.add_argument("--horizon-bars", required=True, type=int)
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--source-run-id")
    parser.add_argument("--event-source-run-id")
    parser.add_argument("--output-root")
    parser.add_argument("--allow-nonviable-regime-audit", action="store_true")
    args = parser.parse_args(argv)

    data_root = Path(args.data_root)
    request = EventLiftRequest(
        run_id=args.run_id,
        mechanism_id=args.mechanism_id,
        regime_id=args.regime_id,
        event_id=args.event_id,
        symbol=args.symbol,
        direction=args.direction,
        horizon_bars=args.horizon_bars,
        data_root=data_root,
        source_run_id=args.source_run_id,
        event_source_run_id=args.event_source_run_id,
        allow_nonviable_regime_audit=bool(args.allow_nonviable_regime_audit),
    )
    try:
        result = run_event_lift(request)
    except EventLiftGateError as exc:
        print(f"fail: {exc}")
        return 1
    except ValueError as exc:
        print(f"fail: {exc}")
        return 1

    output_root = Path(args.output_root) if args.output_root else data_root / "reports" / "event_lift"
    output_dir = output_root / args.run_id
    write_event_lift_outputs(result, output_dir=output_dir)
    print(
        f"Updated {output_dir} "
        f"(classification={result['classification']}, decision={result['decision']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
