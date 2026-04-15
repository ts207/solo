from __future__ import annotations

import argparse
import sys
from pathlib import Path

from project.core.config import get_data_root
from project.research.services.shadow_playbook_service import write_shadow_playbook_report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a deduplicated shadow playbook from exported research survivors."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--confirmatory_report_path", default=None)
    parser.add_argument("--adjacent_survivorship_report_path", default=None)
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    data_root = Path(args.data_root) if args.data_root else get_data_root()
    out_dir = Path(args.out_dir) if args.out_dir else None
    confirmatory = Path(args.confirmatory_report_path) if args.confirmatory_report_path else None
    adjacent_survivorship = (
        Path(args.adjacent_survivorship_report_path)
        if args.adjacent_survivorship_report_path
        else None
    )
    outputs = write_shadow_playbook_report(
        data_root=data_root,
        run_id=str(args.run_id),
        confirmatory_report_path=confirmatory,
        adjacent_survivorship_report_path=adjacent_survivorship,
        out_dir=out_dir,
    )
    print(str(outputs["json"]))
    print(str(outputs["summary"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
