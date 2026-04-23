from __future__ import annotations

import argparse
from pathlib import Path

from project.live.thesis_store import ThesisStore
from project.portfolio.thesis_overlap import write_thesis_overlap_artifacts

DOCS = Path("docs/generated")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build thesis overlap artifacts from an explicit thesis batch source."
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--run_id", help="Run id whose exported promoted_theses.json should be loaded.")
    source.add_argument("--thesis_path", help="Explicit path to a promoted_theses.json artifact.")
    parser.add_argument("--data_root", default=None, help="Optional data root for run_id resolution.")
    parser.add_argument("--docs_dir", default=str(DOCS), help="Output docs directory.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    docs_dir = Path(args.docs_dir)
    if args.thesis_path:
        store = ThesisStore.from_path(args.thesis_path)
        source_run_id = store.run_id or "thesis_overlap_artifacts"
    else:
        store = ThesisStore.from_run_id(
            str(args.run_id),
            data_root=Path(args.data_root) if args.data_root else None,
        )
        source_run_id = store.run_id or str(args.run_id)
    write_thesis_overlap_artifacts(store.all(), docs_dir, source_run_id=source_run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
