from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import argparse
import json

from project.scripts.baseline._common import build_baseline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()
    print(json.dumps(build_baseline(strict=args.strict), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
