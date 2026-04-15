from __future__ import annotations

import argparse
from pathlib import Path

from project.core.config import get_data_root
from project.research.services.context_mode_comparison_service import (
    build_context_mode_comparison_payload,
    write_context_mode_comparison_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare hard-label and confidence-aware context evaluation on the same hypothesis slice."
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--min_sample_size", type=int, default=30)
    parser.add_argument("--search_space_path", default=None)
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    data_root = Path(args.data_root) if args.data_root else get_data_root()
    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else data_root / "reports" / "context_mode_comparison" / args.run_id
    )
    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]

    comparison = build_context_mode_comparison_payload(
        data_root=data_root,
        run_id=str(args.run_id),
        symbols=symbols,
        timeframe=str(args.timeframe),
        min_sample_size=int(args.min_sample_size),
        search_space_path=Path(args.search_space_path) if args.search_space_path else None,
    )
    out_path = write_context_mode_comparison_report(
        out_path=out_dir / "context_mode_comparison.json",
        comparison=comparison,
    )
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
