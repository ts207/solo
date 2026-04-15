from __future__ import annotations

import argparse
import json
from pathlib import Path

from project import PROJECT_ROOT
from project.core.config import get_data_root
from project.research.agent_io.execute_proposal import execute_proposal


def _default_proposal() -> Path:
    return PROJECT_ROOT.parent / "spec" / "proposals" / "demo_synthetic_fast.yaml"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the packaged fast synthetic proposal demo without requiring a historical data lake."
    )
    parser.add_argument("--proposal", default=str(_default_proposal()))
    parser.add_argument("--run_id", default="synthetic_demo_fast")
    parser.add_argument("--registry_root", default="project/configs/registries")
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--plan_only", type=int, default=1)
    parser.add_argument("--dry_run", type=int, default=0)
    parser.add_argument("--check", type=int, default=0)
    args = parser.parse_args(argv)

    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else (PROJECT_ROOT.parent / "artifacts" / "proposal_demo_synthetic")
    )
    result = execute_proposal(
        args.proposal,
        run_id=str(args.run_id),
        registry_root=Path(args.registry_root),
        out_dir=Path(out_dir),
        data_root=Path(args.data_root) if args.data_root else get_data_root(),
        plan_only=bool(args.plan_only),
        dry_run=bool(args.dry_run),
        check=bool(args.check),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return int(result["returncode"])


if __name__ == "__main__":
    raise SystemExit(main())
