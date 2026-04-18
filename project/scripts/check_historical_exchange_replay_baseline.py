from __future__ import annotations

import argparse
from pathlib import Path

from project.tests.events.fixtures.deployable_core_historical_exchange_replay import (
    BASELINE_PATH,
    build_historical_exchange_replay_baseline,
    compare_historical_exchange_replay_baseline,
    load_historical_exchange_replay_baseline,
    write_historical_exchange_replay_baseline,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compare deployable-core detector outputs against pinned historical "
            "exchange-data replay baselines."
        )
    )
    parser.add_argument(
        "--baseline",
        default=str(BASELINE_PATH),
        help="Historical exchange replay baseline JSON path.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Rewrite the baseline with current detector outputs.",
    )
    args = parser.parse_args()

    baseline_path = Path(args.baseline)
    current = build_historical_exchange_replay_baseline()

    if args.update:
        write_historical_exchange_replay_baseline(path=baseline_path, baseline=current)
        print(f"Updated historical exchange replay baseline: {baseline_path}")
        return 0

    if not baseline_path.exists():
        print(f"Missing historical exchange replay baseline: {baseline_path}")
        print(
            "Run with --update only after reviewing the intended historical "
            "detector-output baseline."
        )
        return 1

    baseline = load_historical_exchange_replay_baseline(baseline_path)
    failures = compare_historical_exchange_replay_baseline(baseline=baseline, current=current)
    if failures:
        print("Historical exchange replay baseline drift detected:")
        for failure in failures:
            print(f"- {failure}")
        print("Run with --update only after reviewing the detector-output changes.")
        return 1

    print("Historical exchange replay baseline matches current detector outputs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
