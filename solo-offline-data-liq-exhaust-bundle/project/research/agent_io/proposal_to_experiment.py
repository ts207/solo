from __future__ import annotations

import argparse
import importlib
import json
import tempfile
from pathlib import Path
from typing import Any, Dict

import yaml

from project.research.agent_io.proposal_schema import AgentProposal, load_operator_proposal
from project.spec_registry import load_yaml_path


def _build_experiment_plan(*args: Any, **kwargs: Any) -> Any:
    module = importlib.import_module("project.research.experiment_engine")
    return module.build_experiment_plan(*args, **kwargs)


def _normalize_entry_lags(values: Any, *, field_name: str) -> list[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    for raw in list(values or []):
        lag = int(raw)
        if lag < 1:
            raise ValueError(f"{field_name} must be >= 1 to prevent same-bar entry leakage")
        if lag not in seen:
            normalized.append(lag)
            seen.add(lag)
    if not normalized:
        raise ValueError(f"{field_name} must contain at least one lag >= 1")
    return normalized


def _load_search_limit_defaults(registry_root: Path) -> Dict[str, Any]:
    payload = load_yaml_path(registry_root / "search_limits.yaml")
    if not isinstance(payload, dict):
        return {}
    defaults = payload.get("defaults", {}) if isinstance(payload.get("defaults"), dict) else {}
    limits = payload.get("limits", {}) if isinstance(payload.get("limits"), dict) else {}
    return {
        "horizons_bars": list(defaults.get("horizons_bars", [1, 3, 12, 24, 72])),
        "directions": list(defaults.get("directions", ["long", "short"])),
        "entry_lags": _normalize_entry_lags(
            defaults.get("entry_lags", [1, 2]), field_name="search_limit_defaults.entry_lags"
        ),
        "max_hypotheses_total": int(limits.get("max_hypotheses_total", 1000)),
        "max_hypotheses_per_template": int(limits.get("max_hypotheses_per_template", 250)),
        "max_hypotheses_per_event_family": int(limits.get("max_hypotheses_per_event_family", 300)),
    }


def proposal_to_experiment_config(
    proposal: AgentProposal,
    *,
    registry_root: Path,
) -> Dict[str, Any]:
    defaults = _load_search_limit_defaults(registry_root)
    search_control = {
        "max_hypotheses_total": int(
            proposal.search_control.get("max_hypotheses_total", defaults["max_hypotheses_total"])
        ),
        "max_hypotheses_per_template": int(
            proposal.search_control.get(
                "max_hypotheses_per_template",
                defaults["max_hypotheses_per_template"],
            )
        ),
        "max_hypotheses_per_event_family": int(
            proposal.search_control.get(
                "max_hypotheses_per_event_family",
                defaults["max_hypotheses_per_event_family"],
            )
        ),
        "random_seed": int(proposal.search_control.get("random_seed", 42)),
    }
    horizons = proposal.horizons_bars or list(defaults["horizons_bars"])
    directions = proposal.directions or list(defaults["directions"])
    entry_lags = _normalize_entry_lags(
        proposal.entry_lags or list(defaults["entry_lags"]), field_name="proposal.entry_lags"
    )
    promotion_enabled = proposal.promotion_profile != "disabled"

    return {
        "program_id": proposal.program_id,
        "run_mode": proposal.run_mode,
        "description": proposal.description,
        "objective_name": proposal.objective_name,
        "promotion_profile": proposal.promotion_profile,
        "instrument_scope": {
            "instrument_classes": list(proposal.instrument_classes),
            "symbols": list(proposal.symbols),
            "timeframe": proposal.timeframe,
            "start": proposal.start,
            "end": proposal.end,
        },
        "trigger_space": dict(proposal.trigger_space),
        "templates": {"include": list(proposal.templates)},
        "evaluation": {
            "horizons_bars": list(horizons),
            "directions": list(directions),
            "entry_lags": list(entry_lags),
        },
        "contexts": {"include": dict(proposal.contexts)},
        "avoid_region_keys": list(proposal.avoid_region_keys),
        "search_control": search_control,
        "promotion": {
            "enabled": bool(promotion_enabled),
            "track": "standard",
            "multiplicity_scope": "program_id",
        },
        "artifacts": dict(proposal.artifacts),
        "bounded": proposal.bounded.to_dict() if proposal.bounded is not None else None,
    }


def build_run_all_overrides(proposal: AgentProposal) -> Dict[str, Any]:
    overrides: Dict[str, Any] = {
        "program_id": proposal.program_id,
        "mode": proposal.run_mode,
        "objective_name": proposal.objective_name,
        "promotion_profile": proposal.promotion_profile,
        "symbols": ",".join(proposal.symbols),
        "discovery_profile": proposal.discovery_profile,
        "phase2_gate_profile": proposal.phase2_gate_profile,
        "search_spec": proposal.search_spec,
    }
    if proposal.config_overlays:
        overrides["config"] = list(proposal.config_overlays)
    if proposal.bounded is not None and proposal.bounded.compare_to_baseline:
        overrides["research_compare_baseline_run_id"] = proposal.bounded.baseline_run_id
    if proposal.promotion_profile == "disabled":
        overrides["run_candidate_promotion"] = 0
    for key, value in sorted(proposal.knobs.items()):
        overrides[key] = value
    return overrides


def translate_and_validate_proposal(
    proposal_or_path: AgentProposal | str | Path | Dict[str, Any],
    *,
    registry_root: Path,
    out_dir: Path | None = None,
    config_path: Path | None = None,
) -> Dict[str, Any]:
    proposal = (
        proposal_or_path
        if isinstance(proposal_or_path, AgentProposal)
        else load_operator_proposal(proposal_or_path)
    )
    experiment_config = proposal_to_experiment_config(proposal, registry_root=registry_root)
    resolved_config_path = config_path
    if resolved_config_path is None:
        base_dir = out_dir if out_dir is not None else (Path.cwd() / "tmp")
        base_dir.mkdir(parents=True, exist_ok=True)
        resolved_config_path = base_dir / f"{proposal.program_id}_proposal_experiment.yaml"
    resolved_config_path.parent.mkdir(parents=True, exist_ok=True)
    rendered_config = yaml.safe_dump(experiment_config, sort_keys=False)

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".yaml",
        prefix=f".{resolved_config_path.stem}__staged__",
        dir=str(resolved_config_path.parent),
        delete=False,
        encoding="utf-8",
    ) as handle:
        handle.write(rendered_config)
        staged_config_path = Path(handle.name)

    try:
        plan = _build_experiment_plan(
            staged_config_path,
            registry_root,
            out_dir=out_dir,
        )
        staged_config_path.replace(resolved_config_path)
    finally:
        staged_config_path.unlink(missing_ok=True)

    run_all_overrides = build_run_all_overrides(proposal)
    return {
        "proposal": proposal.to_dict(),
        "experiment_config": experiment_config,
        "experiment_config_path": str(resolved_config_path),
        "run_all_overrides": run_all_overrides,
        "validated_plan": {
            "program_id": plan.program_id,
            "estimated_hypothesis_count": int(plan.estimated_hypothesis_count),
            "required_detectors": list(plan.required_detectors),
            "required_features": list(plan.required_features),
            "required_states": list(plan.required_states),
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Translate an agent proposal into a validated experiment config."
    )
    parser.add_argument("--proposal", required=True, help="Path to proposal JSON/YAML")
    parser.add_argument("--registry_root", default="project/configs/registries")
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--config_path", default=None)
    parser.add_argument("--overrides_path", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    registry_root = Path(args.registry_root)
    out_dir = Path(args.out_dir) if args.out_dir else None
    config_path = Path(args.config_path) if args.config_path else None
    result = translate_and_validate_proposal(
        args.proposal,
        registry_root=registry_root,
        out_dir=out_dir,
        config_path=config_path,
    )
    if args.overrides_path:
        Path(args.overrides_path).write_text(
            json.dumps(result["run_all_overrides"], indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
