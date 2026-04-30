#!/usr/bin/env python3
"""Build canonical regime/event inventory sidecars."""

from __future__ import annotations

import argparse
from pathlib import Path

from project.research.regime_event_inventory import REPO_ROOT, write_inventory_outputs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=str(REPO_ROOT))
    parser.add_argument("--output-dir")
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    output_dir = Path(args.output_dir).resolve() if args.output_dir else None
    df = write_inventory_outputs(root=root, output_dir=output_dir)
    out = output_dir or root / "data" / "reports" / "regime_event_inventory"
    print(f"Updated {out} ({len(df)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
