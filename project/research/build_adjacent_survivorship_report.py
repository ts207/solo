from __future__ import annotations

import argparse
import sys
from pathlib import Path

from project.core.config import get_data_root
from project.research.services.confirmatory_candidate_service import (
    write_adjacent_survivorship_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Report which exported survivors persist across an adjacent target window."
    )
    parser.add_argument("--origin_run_id", required=True)
    parser.add_argument("--target_run_id", required=True)
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    data_root = Path(args.data_root) if args.data_root else get_data_root()
    out_dir = Path(args.out_dir) if args.out_dir else None
    out_path = write_adjacent_survivorship_report(
        data_root=data_root,
        origin_run_id=str(args.origin_run_id),
        target_run_id=str(args.target_run_id),
        out_dir=out_dir,
    )
    print(str(out_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
