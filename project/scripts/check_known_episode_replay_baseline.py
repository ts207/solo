from __future__ import annotations

import argparse
from pathlib import Path

from project.tests.events.fixtures.deployable_core_known_episode_replay import (
    BASELINE_PATH,
    build_known_episode_replay_baseline,
    compare_known_episode_replay_baseline,
    load_known_episode_replay_baseline,
    write_known_episode_replay_baseline,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare deployable-core detector outputs against known-episode replay baselines."
    )
    parser.add_argument("--baseline", default=str(BASELINE_PATH), help="Known-episode baseline JSON path.")
    parser.add_argument("--update", action="store_true", help="Rewrite the baseline with current detector outputs.")
    args = parser.parse_args()

    baseline_path = Path(args.baseline)
    current = build_known_episode_replay_baseline()

    if args.update:
        write_known_episode_replay_baseline(path=baseline_path, baseline=current)
        print(f"Updated known-episode replay baseline: {baseline_path}")
        return 0

    if not baseline_path.exists():
        print(f"Missing known-episode replay baseline: {baseline_path}")
        print("Run with --update after reviewing the intended episode-level detector baseline.")
        return 1

    baseline = load_known_episode_replay_baseline(baseline_path)
    failures = compare_known_episode_replay_baseline(baseline=baseline, current=current)
    if failures:
        print("Known-episode replay baseline drift detected:")
        for failure in failures:
            print(f"- {failure}")
        print("Run with --update only after reviewing the detector-output changes.")
        return 1

    print("Known-episode replay baseline matches current detector outputs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
