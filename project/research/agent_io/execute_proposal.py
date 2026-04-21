from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

from project import PROJECT_ROOT
from project.core.config import get_data_root
from project.research.agent_io.proposal_to_experiment import translate_and_validate_proposal
from project.research.CANONICAL_PIPELINE import persist_canonical_pipeline_artifact


def _to_cli_tokens(flag: str, value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, bool):
        return [flag, "1" if value else "0"]
    if isinstance(value, (list, tuple)):
        if not value:
            return []
        return [flag, *[str(item) for item in value]]
    return [flag, str(value)]


def build_run_all_command(
    *,
    run_id: str,
    registry_root: Path,
    experiment_config_path: Path,
    run_all_overrides: Dict[str, Any],
    symbols: List[str],
    start: str,
    end: str,
    plan_only: bool,
    dry_run: bool,
) -> List[str]:
    cmd = [
        sys.executable,
        "-m",
        "project.pipelines.run_all",
        "--run_id",
        str(run_id),
        "--experiment_config",
        str(experiment_config_path),
        "--registry_root",
        str(registry_root),
        "--symbols",
        ",".join(str(symbol).strip() for symbol in symbols if str(symbol).strip()),
        "--start",
        str(start),
        "--end",
        str(end),
    ]
    for key, value in sorted(run_all_overrides.items()):
        if key in {"symbols"}:
            continue
        if key == "config" and isinstance(value, (list, tuple)):
            for item in value:
                cmd.extend(["--config", str(item)])
            continue
        cmd.extend(_to_cli_tokens(f"--{key}", value))
    if plan_only:
        cmd.extend(["--plan_only", "1"])
    if dry_run:
        cmd.extend(["--dry_run", "1"])
    return cmd


def _run_env(*, data_root: Path) -> Dict[str, str]:
    env = os.environ.copy()
    repo_root = str(PROJECT_ROOT.parent)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{repo_root}:{existing_pythonpath}" if existing_pythonpath else repo_root
    env["BACKTEST_DATA_ROOT"] = str(data_root)
    return env


def execute_proposal(
    proposal_path: str | Path,
    *,
    run_id: str,
    registry_root: Path,
    out_dir: Path,
    data_root: Path | None = None,
    plan_only: bool = True,
    dry_run: bool = False,
    check: bool = False,
) -> Dict[str, Any]:
    resolved_data_root = Path(data_root) if data_root is not None else get_data_root()
    resolved_out_dir = Path(out_dir)
    resolved_out_dir.mkdir(parents=True, exist_ok=True)
    config_path = resolved_out_dir / "experiment.yaml"
    overrides_path = resolved_out_dir / "run_all_overrides.json"

    translation = translate_and_validate_proposal(
        proposal_path,
        registry_root=registry_root,
        out_dir=resolved_out_dir,
        config_path=config_path,
    )

    # Staged discover runs must not run internal promote_candidates.
    # Promotion is an explicit downstream stage: `edge validate run` then `edge promote run`.
    # Zero surviving candidates is a valid discover outcome, not a failure.
    if not plan_only:
        translation["run_all_overrides"]["run_candidate_promotion"] = 0

    overrides_path.write_text(
        json.dumps(translation["run_all_overrides"], indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    canonical_path_artifact = persist_canonical_pipeline_artifact(
        resolved_out_dir,
        run_id=run_id,
        stage="discover",
        used_module="project.research.agent_io.execute_proposal",
        extra={
            "plan_only": bool(plan_only),
            "dry_run": bool(dry_run),
            "proposal_path": str(proposal_path),
            "experiment_config_path": str(config_path),
            "run_all_overrides_path": str(overrides_path),
        },
    )

    proposal = translation["proposal"]
    command = build_run_all_command(
        run_id=run_id,
        registry_root=registry_root,
        experiment_config_path=config_path,
        run_all_overrides=translation["run_all_overrides"],
        symbols=list(proposal["symbols"]),
        start=str(proposal["start"]),
        end=str(proposal["end"]),
        plan_only=bool(plan_only),
        dry_run=bool(dry_run),
    )
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT.parent),
        env=_run_env(data_root=resolved_data_root),
        check=check,
    )
    return {
        "run_id": run_id,
        "proposal_path": str(proposal_path),
        "experiment_config_path": str(config_path),
        "run_all_overrides_path": str(overrides_path),
        "canonical_research_path_path": str(canonical_path_artifact),
        "command": command,
        "returncode": int(result.returncode),
        "stdout": result.stdout,
        "stderr": result.stderr,
        "validated_plan": translation["validated_plan"],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Translate a proposal and invoke run_all with the validated config."
    )
    parser.add_argument("--proposal", required=True)
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--registry_root", default="project/configs/registries")
    parser.add_argument("--out_dir", required=True)
    parser.add_argument("--data_root", default=None)
    parser.add_argument("--plan_only", type=int, default=1)
    parser.add_argument("--dry_run", type=int, default=0)
    parser.add_argument("--check", type=int, default=0)
    return parser


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = execute_proposal(
        args.proposal,
        run_id=str(args.run_id),
        registry_root=Path(args.registry_root),
        out_dir=Path(args.out_dir),
        data_root=Path(args.data_root) if args.data_root else None,
        plan_only=bool(args.plan_only),
        dry_run=bool(args.dry_run),
        check=bool(args.check),
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return int(result["returncode"])


if __name__ == "__main__":
    raise SystemExit(main())
