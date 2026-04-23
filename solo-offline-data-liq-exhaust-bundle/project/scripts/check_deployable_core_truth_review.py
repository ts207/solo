from __future__ import annotations

import argparse
from pathlib import Path

from project.tests.events.fixtures.deployable_core_truth_review import (
    review_deployable_core_episode_truth,
    write_truth_review,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run false-positive, false-negative, and quality review gates for deployable-core known episodes."
    )
    parser.add_argument("--json-out", default="", help="Optional path for the full truth-review report.")
    args = parser.parse_args()

    review = review_deployable_core_episode_truth()
    if args.json_out:
        write_truth_review(Path(args.json_out), review=review)

    if review["status"] != "pass":
        print("Deployable-core truth review failed:")
        for failure in review["failures"]:
            print(
                f"- {failure['kind']}: {failure['episode_id']}/{failure['event_name']}: {failure['message']}"
            )
        return 1

    print("Deployable-core truth review passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
