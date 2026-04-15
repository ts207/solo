from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Sequence

import pandas as pd

from project.core.config import get_data_root
from project.contracts.schemas import validate_dataframe_for_schema
from project.reliability.contracts import validate_promotion_artifacts


CONTRACT_TESTS: tuple[str, ...] = (
    "project/tests/research/agent_io/test_proposal_schema.py",
    "project/tests/research/agent_io/test_issue_proposal.py",
    "project/tests/research/test_regime_routing.py",
    "project/tests/research/test_phase2_search_engine_regime_metadata.py",
    "project/tests/research/test_search_intelligence_regimes.py",
    "project/tests/contracts/test_phase5_contracts.py",
    "project/tests/contracts/test_promotion_artifacts_schema.py",
)


def _run(cmd: Sequence[str], *, cwd: Path) -> None:
    result = subprocess.run(cmd, cwd=str(cwd), check=False)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


def _load_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} does not contain a JSON object")
    return payload


def _resolve_phase2_artifact(phase2_dir: Path, filename: str) -> Path | None:
    candidates = [
        phase2_dir / filename,
        phase2_dir / "search_engine" / filename,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _verify_contracts(repo_root: Path) -> None:
    _run(
        [sys.executable, "-m", "project.scripts.regime_routing_audit", "--check"],
        cwd=repo_root,
    )
    _run(
        [sys.executable, "-m", "project.scripts.build_event_deep_analysis_suite", "--check"],
        cwd=repo_root,
    )
    _run([sys.executable, "-m", "pytest", *CONTRACT_TESTS, "-q"], cwd=repo_root)


def _verify_experiment_artifacts(
    *,
    repo_root: Path,
    data_root: Path,
    run_id: str,
    baseline_run_id: str = "",
) -> None:
    run_manifest = data_root / "runs" / run_id / "run_manifest.json"
    phase2_dir = data_root / "reports" / "phase2" / run_id
    phase2_candidates = _resolve_phase2_artifact(phase2_dir, "phase2_candidates.parquet")
    phase2_diagnostics = _resolve_phase2_artifact(phase2_dir, "phase2_diagnostics.json")
    promotion_dir = data_root / "reports" / "promotions" / run_id

    required = [run_manifest, phase2_candidates, phase2_diagnostics, promotion_dir]
    missing = [str(path) for path in required if not path or not path.exists()]
    if missing:
        raise FileNotFoundError("missing required run artifacts: " + ", ".join(missing))

    _load_json(run_manifest)
    _load_json(phase2_diagnostics)
    phase2_df = pd.read_parquet(phase2_candidates)
    validate_dataframe_for_schema(phase2_df, "phase2_candidates")
    validate_promotion_artifacts(promotion_dir)

    if baseline_run_id:
        _run(
            [
                sys.executable,
                "project/scripts/compare_research_runs.py",
                "--baseline_run_id",
                baseline_run_id,
                "--candidate_run_id",
                run_id,
                "--data_root",
                str(data_root),
            ],
            cwd=repo_root,
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the bounded autonomous-research verification block."
    )
    parser.add_argument("--mode", choices=("contracts", "experiment"), required=True)
    parser.add_argument("--run-id", default="")
    parser.add_argument("--baseline-run-id", default="")
    parser.add_argument("--data-root", default=str(get_data_root()))
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[2]
    data_root = Path(args.data_root).resolve()

    _verify_contracts(repo_root)

    if args.mode == "experiment":
        run_id = str(args.run_id).strip()
        if not run_id:
            raise ValueError("--run-id is required for --mode experiment")
        _verify_experiment_artifacts(
            repo_root=repo_root,
            data_root=data_root,
            run_id=run_id,
            baseline_run_id=str(args.baseline_run_id).strip(),
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
