from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from project.core.config import get_data_root
from project.reliability.cli_smoke import run_smoke_cli


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run deterministic smoke workflows and artifact validation."
    )
    parser.add_argument(
        "--mode",
        choices=["engine", "research", "promotion", "full", "validate-artifacts"],
        default="full",
    )
    parser.add_argument("--root", default=None)
    parser.add_argument("--seed", type=int, default=20260101)
    parser.add_argument("--storage-mode", choices=["auto", "csv-fallback"], default="auto")
    args = parser.parse_args(argv)
    root = Path(args.root) if args.root else (get_data_root() / "reliability" / "smoke")
    try:
        run_smoke_cli(args.mode, root=root, seed=args.seed, storage_mode=args.storage_mode)
        return 0
    except Exception as exc:
        (root / "reliability").mkdir(parents=True, exist_ok=True)
        (root / "reliability" / "smoke_failure.json").write_text(
            json.dumps({"error": str(exc), "mode": args.mode}, indent=2), encoding="utf-8"
        )
        raise


if __name__ == "__main__":
    sys.exit(main())
