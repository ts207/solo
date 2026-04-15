from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import argparse
import json
from pathlib import Path

from project.scripts.baseline._common import BASELINE_ROOT, compare_snapshot_dirs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-events", type=Path, default=BASELINE_ROOT / "events")
    parser.add_argument("--candidate-events", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            compare_snapshot_dirs(args.baseline_events, args.candidate_events),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
