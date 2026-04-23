from __future__ import annotations

import argparse
from pathlib import Path

from project.tests.events.fixtures.deployable_core_replay_baseline import (
    BASELINE_PATH,
    build_deployable_core_replay_baseline,
    compare_deployable_core_replay_baseline,
    load_deployable_core_replay_baseline,
    write_deployable_core_replay_baseline,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare deployable-core detector outputs against the checked-in replay baseline."
    )
    parser.add_argument("--baseline", default=str(BASELINE_PATH), help="Replay baseline JSON path.")
    parser.add_argument("--update", action="store_true", help="Rewrite the baseline with current detector outputs.")
    args = parser.parse_args()

    baseline_path = Path(args.baseline)
    current = build_deployable_core_replay_baseline()

    if args.update:
        write_deployable_core_replay_baseline(path=baseline_path, baseline=current)
        print(f"Updated deployable-core replay baseline: {baseline_path}")
        return 0

    if not baseline_path.exists():
        print(f"Missing deployable-core replay baseline: {baseline_path}")
        print("Run with --update after reviewing the intended detector-output baseline.")
        return 1

    baseline = load_deployable_core_replay_baseline(baseline_path)
    failures = compare_deployable_core_replay_baseline(baseline=baseline, current=current)
    if failures:
        print("Deployable-core replay baseline drift detected:")
        for failure in failures:
            print(f"- {failure}")
        print("Run with --update only after reviewing the detector-output changes.")
        return 1

    print("Deployable-core replay baseline matches current detector outputs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
