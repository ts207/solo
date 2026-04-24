from __future__ import annotations

"""Replay the late-stage research chain from recorded manifests.

This helper is intentionally compatibility-oriented: historical run folders may
contain per-event bridge manifests with suffixed names, while newer planner-
owned discovery uses the unsuffixed `phase2_search_engine` stage. The replay
path therefore discovers existing bridge manifests from the run folder instead
of assuming a fixed timeframe suffix.
"""

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

RUN_ID = "btc_2020_2021_full_r6_production_budget500_selectorfix"
RUN_DIR = Path("data/runs") / RUN_ID
VENV_PYTHON = str(Path(".venv/bin/python"))

MODULES = {
    "bridge_evaluate_phase2": "project.research.bridge_evaluate_phase2",
    "export_edge_candidates": "project.research.export_edge_candidates",
    "promote_candidates": "project.research.cli.promotion_cli",
    "analyze_conditional_expectancy": "project.research.analyze_conditional_expectancy",
    "validate_expectancy_traps": "project.research.validate_expectancy_traps",
    "generate_recommendations_checklist": "project.research.generate_recommendations_checklist",
    "compile_strategy_blueprints": "project.research.compile_strategy_blueprints",
    "build_strategy_candidates": "project.research.build_strategy_candidates",
    "select_profitable_strategies": "project.research.select_profitable_strategies",
}


def _param_args(params: dict[str, object]) -> list[str]:
    args: list[str] = []
    for key, value in params.items():
        if value is None:
            continue
        flag = f"--{key}"
        if isinstance(value, bool):
            args.extend([flag, "1" if value else "0"])
        elif isinstance(value, list):
            args.extend([flag, ",".join(str(item) for item in value)])
        else:
            args.extend([flag, str(value)])
    return args


def _stage_key(stage_file: Path) -> str:
    stem = stage_file.stem
    if stem.startswith("bridge_evaluate_phase2__"):
        return "bridge_evaluate_phase2"
    return stem


def _bridge_stage_files(events: list[str], run_dir: Path) -> list[Path]:
    stage_files: list[Path] = []
    for event in events:
        matches = sorted(run_dir.glob(f"bridge_evaluate_phase2__{event}_*.json"))
        if matches:
            stage_files.extend(matches)
            continue
        fallback = run_dir / "bridge_evaluate_phase2.json"
        if fallback.exists():
            stage_files.append(fallback)
            continue
        raise FileNotFoundError(
            f"Missing bridge manifest for {event}: expected bridge_evaluate_phase2__{event}_*.json or {fallback}"
        )
    return stage_files


def _stage_sequence() -> list[Path]:
    promo_audit = pd.read_parquet(
        Path("data/reports/promotions") / RUN_ID / "promotion_statistical_audit.parquet"
    )
    events = sorted(set(promo_audit["event_type"].astype(str)))
    bridge_stage_files = _bridge_stage_files(events, RUN_DIR)

    return bridge_stage_files + [
        RUN_DIR / "export_edge_candidates.json",
        RUN_DIR / "promote_candidates.json",
        RUN_DIR / "analyze_conditional_expectancy.json",
        RUN_DIR / "validate_expectancy_traps.json",
        RUN_DIR / "generate_recommendations_checklist.json",
        RUN_DIR / "compile_strategy_blueprints.json",
        RUN_DIR / "build_strategy_candidates.json",
        RUN_DIR / "select_profitable_strategies.json",
    ]


def main() -> int:
    for stage_file in _stage_sequence():
        payload = json.loads(stage_file.read_text(encoding="utf-8"))
        module = MODULES[_stage_key(stage_file)]
        params = dict(payload.get("parameters", {}))
        cmd = [VENV_PYTHON, "-m", module, *_param_args(params)]
        print(f"RUN {stage_file.name}")
        proc = subprocess.run(cmd, text=True, capture_output=True)
        print(f"EXIT {proc.returncode}")
        if proc.stdout.strip():
            print(f"STDOUT_LAST {proc.stdout.strip().splitlines()[-1]}")
        if proc.stderr.strip():
            print(f"STDERR_LAST {proc.stderr.strip().splitlines()[-1]}")
        if proc.returncode != 0:
            if proc.stdout:
                print(proc.stdout)
            if proc.stderr:
                print(proc.stderr, file=sys.stderr)
            return int(proc.returncode)
    print("REPLAY_DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
