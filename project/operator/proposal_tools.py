from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

from project.core.config import get_data_root
from project.operator.bounded import validate_bounded_proposal
from project.research.agent_io.proposal_schema import (
    detect_operator_proposal_format,
    load_operator_proposal,
)
from project.research.agent_io.proposal_to_experiment import translate_and_validate_proposal


def lint_proposal(
    *,
    proposal_path: str | Path,
    registry_root: str | Path = "project/configs/registries",
    data_root: str | Path | None = None,
    out_dir: str | Path | None = None,
) -> dict[str, Any]:
    resolved_data_root = Path(data_root) if data_root is not None else get_data_root()
    proposal = load_operator_proposal(proposal_path)
    translation = translate_and_validate_proposal(
        proposal,
        registry_root=Path(registry_root),
        out_dir=Path(out_dir) if out_dir is not None else None,
    )
    bounded = validate_bounded_proposal(proposal, data_root=resolved_data_root)
    warnings: list[str] = []
    estimated = int(translation["validated_plan"].get("estimated_hypothesis_count", 0) or 0)
    if estimated > 250:
        warnings.append(f"broad_search_surface:{estimated}")
    result = {
        "status": "pass",
        "proposal_path": str(proposal_path),
        "program_id": proposal.program_id,
        "warnings": warnings,
        "validated_plan": translation["validated_plan"],
        "bounded_validation": bounded.to_dict() if bounded is not None else None,
    }
    if warnings:
        result["status"] = "warn"
    return result


def explain_proposal(
    *,
    proposal_path: str | Path,
    registry_root: str | Path = "project/configs/registries",
    data_root: str | Path | None = None,
    out_dir: str | Path | None = None,
) -> dict[str, Any]:
    resolved_data_root = Path(data_root) if data_root is not None else get_data_root()
    proposal_format = detect_operator_proposal_format(proposal_path)
    proposal = load_operator_proposal(proposal_path)
    translation = translate_and_validate_proposal(
        proposal,
        registry_root=Path(registry_root),
        out_dir=Path(out_dir) if out_dir is not None else None,
    )
    bounded = validate_bounded_proposal(proposal, data_root=resolved_data_root)
    experiment_config = dict(translation.get("experiment_config", {}) or {})
    experiment_summary = {
        "instrument_scope": dict(experiment_config.get("instrument_scope", {}) or {}),
        "templates": dict(experiment_config.get("templates", {}) or {}),
        "evaluation": dict(experiment_config.get("evaluation", {}) or {}),
        "promotion": dict(experiment_config.get("promotion", {}) or {}),
        "bounded": experiment_config.get("bounded"),
    }
    return {
        "proposal_path": str(proposal_path),
        "proposal_format": proposal_format,
        "program_id": proposal.program_id,
        "description": proposal.description,
        "normalized_proposal": proposal.to_dict(),
        "compiled_trigger_space": dict(proposal.trigger_space),
        "resolved_experiment_summary": experiment_summary,
        "run_mode": proposal.run_mode,
        "objective_name": proposal.objective_name,
        "symbols": list(proposal.symbols),
        "templates": list(proposal.templates),
        "horizons_bars": list(proposal.horizons_bars),
        "directions": list(proposal.directions),
        "entry_lags": list(proposal.entry_lags),
        "contexts": dict(proposal.contexts),
        "bounded": bounded.to_dict() if bounded is not None else None,
        "required_detectors": list(translation["validated_plan"].get("required_detectors", [])),
        "required_features": list(translation["validated_plan"].get("required_features", [])),
        "required_states": list(translation["validated_plan"].get("required_states", [])),
        "estimated_hypothesis_count": int(translation["validated_plan"].get("estimated_hypothesis_count", 0) or 0),
        "run_all_overrides": translation["run_all_overrides"],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect bounded proposal files.")
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("lint", "explain"):
        cmd = sub.add_parser(command)
        cmd.add_argument("--proposal", required=True)
        cmd.add_argument("--registry_root", default="project/configs/registries")
        cmd.add_argument("--data_root", default=None)
        cmd.add_argument("--out_dir", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    fn = lint_proposal if args.command == "lint" else explain_proposal
    result = fn(
        proposal_path=args.proposal,
        registry_root=args.registry_root,
        data_root=args.data_root,
        out_dir=args.out_dir,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("status", "pass") != "block" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
