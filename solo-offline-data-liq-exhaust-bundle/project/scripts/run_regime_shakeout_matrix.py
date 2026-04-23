from __future__ import annotations

import argparse
import json
from pathlib import Path

from project import PROJECT_ROOT
from project.core.config import get_data_root
from project.research.services.regime_shakeout_service import (
    default_shakeout_out_dir,
    run_regime_shakeout_matrix,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the regime-first vs raw-control empirical shakeout matrix."
    )
    parser.add_argument(
        "--matrix",
        default=str(PROJECT_ROOT.parent / "spec" / "benchmarks" / "regime_shakeout_matrix.yaml"),
        help="Path to the shakeout matrix YAML.",
    )
    parser.add_argument(
        "--registry_root",
        default=str(PROJECT_ROOT / "configs" / "registries"),
        help="Registry root passed to proposal translation/execution.",
    )
    parser.add_argument("--data_root", default=None, help="Override data root.")
    parser.add_argument("--out_dir", default=None, help="Output directory for manifest/audit bundle.")
    parser.add_argument("--execute", type=int, default=0, help="If 1, invoke run_all for each slice.")
    parser.add_argument(
        "--plan_only",
        type=int,
        default=1,
        help="If 1, execute proposal planning only. Set 0 for full runs.",
    )
    parser.add_argument("--dry_run", type=int, default=0, help="Pass --dry_run 1 into run_all.")
    parser.add_argument("--check", type=int, default=0, help="Pass subprocess check=True.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    matrix_path = Path(args.matrix).resolve()
    registry_root = Path(args.registry_root).resolve()
    data_root = Path(args.data_root).resolve() if args.data_root else get_data_root()
    out_dir = (
        Path(args.out_dir).resolve()
        if args.out_dir
        else default_shakeout_out_dir(matrix_id=matrix_path.stem, data_root=data_root)
    )
    result = run_regime_shakeout_matrix(
        matrix_path=matrix_path,
        out_dir=out_dir,
        registry_root=registry_root,
        data_root=data_root,
        execute=bool(args.execute),
        plan_only=bool(args.plan_only),
        dry_run=bool(args.dry_run),
        check=bool(args.check),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if int(result.get("failures", 0)) > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
