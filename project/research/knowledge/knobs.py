from __future__ import annotations

import argparse
import importlib
from pathlib import Path
from typing import Any

from project.research.knowledge.schemas import canonical_json, stable_hash
from project.spec_registry import load_yaml_path


def _value_type(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "object"
    if value is None:
        return "null"
    return type(value).__name__


def _knob_row(
    *,
    scope: str,
    group: str,
    name: str,
    value: Any,
    source_module: str,
    description: str,
    cli_flag: str = "",
    choices: list[Any] | None = None,
    agent_level: str = "advanced",
    mutability: str = "inspect_only",
    risk: str = "medium",
) -> dict[str, Any]:
    return {
        "knob_id": stable_hash((scope, group, name, cli_flag, source_module)),
        "scope": scope,
        "group": group,
        "name": name,
        "cli_flag": cli_flag,
        "value_type": _value_type(value),
        "default_value_json": canonical_json(value),
        "choices_json": canonical_json(choices or []),
        "description": description,
        "source_module": source_module,
        "agent_level": agent_level,
        "mutability": mutability,
        "risk": risk,
    }


def _parser_knobs(
    parser: argparse.ArgumentParser,
    *,
    scope: str,
    group: str,
    source_module: str,
    include_prefixes: tuple[str, ...],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for action in parser._actions:
        option_strings = list(getattr(action, "option_strings", []) or [])
        if not option_strings:
            continue
        flag = next((opt for opt in option_strings if opt.startswith("--")), "")
        if not flag:
            continue
        dest = str(getattr(action, "dest", "") or "").strip()
        if not dest or not any(dest.startswith(prefix) for prefix in include_prefixes):
            continue
        choices = list(getattr(action, "choices", []) or [])
        default_value = getattr(action, "default", None)
        rows.append(
            _knob_row(
                scope=scope,
                group=group,
                name=dest,
                value=default_value,
                source_module=source_module,
                description=str(getattr(action, "help", "") or ""),
                cli_flag=flag,
                choices=choices,
                agent_level=_default_agent_level(group, dest),
                mutability=_default_mutability(group, dest, cli_flag=flag),
                risk=_default_risk(group, dest),
            )
        )
    return rows


def _default_agent_level(group: str, name: str) -> str:
    token = f"{group}.{name}"
    if group in {"campaign_memory", "campaign_memory_stage", "promotion", "search_limits"}:
        return "core"
    if group in {"promotion_profile_defaults", "promotion_policy_resolution"}:
        return "advanced"
    if group == "promotion_timeframe_consensus":
        return "internal"
    if any(
        key in token
        for key in ("retail_profiles", "objective", "phase2_gates", "shrinkage_parameters")
    ):
        return "advanced"
    return "advanced"


def _default_mutability(group: str, name: str, *, cli_flag: str = "") -> str:
    token = f"{group}.{name}"
    if cli_flag:
        return "proposal_settable"
    if group == "promotion_profile_defaults":
        return "proposal_settable"
    if group == "search_limits":
        return "proposal_settable"
    if group in {"promotion_policy_resolution", "promotion_timeframe_consensus"}:
        return "inspect_only"
    if any(
        key in token
        for key in ("retail_profiles", "objective", "phase2_gates", "shrinkage_parameters")
    ):
        return "inspect_only"
    return "inspect_only"


def _default_risk(group: str, name: str) -> str:
    token = f"{group}.{name}"
    if group in {"campaign_memory", "campaign_memory_stage", "search_limits"}:
        return "low"
    if group == "promotion":
        return "medium"
    if group in {"promotion_policy_resolution", "promotion_timeframe_consensus"}:
        return "high"
    if any(
        key in token
        for key in ("objective", "retail_profiles", "phase2_gates", "shrinkage_parameters")
    ):
        return "high"
    return "medium"


def build_agent_knob_rows() -> list[dict[str, Any]]:
    build_run_all_parser = importlib.import_module(
        "project.pipelines.pipeline_planning"
    ).build_parser
    build_memory_update_parser = importlib.import_module(
        "project.research.update_campaign_memory"
    ).build_parser
    profile_promotion_defaults = importlib.import_module(
        "project.pipelines.stages.research"
    )._PROFILE_PROMOTION_DEFAULTS

    rows: list[dict[str, Any]] = []
    rows.extend(
        _parser_knobs(
            build_run_all_parser(),
            scope="agent",
            group="promotion",
            source_module="project.pipelines.pipeline_planning",
            include_prefixes=(
                "candidate_promotion_",
                "run_phase2_conditional",
                "funding_",
                "discovery_profile",
                "phase2_gate_profile",
                "search_spec",
                "search_min_n",
                "use_context_quality",
                "event_parameter_overrides",
            ),
        )
    )
    rows.extend(
        _parser_knobs(
            build_run_all_parser(),
            scope="agent",
            group="promotion",
            source_module="project.pipelines.pipeline_planning",
            include_prefixes=("cost_bps",),
        )
    )
    rows.extend(
        _parser_knobs(
            build_run_all_parser(),
            scope="agent",
            group="campaign_memory",
            source_module="project.pipelines.pipeline_planning",
            include_prefixes=("campaign_memory_", "run_campaign_memory_update"),
        )
    )
    rows.extend(
        _parser_knobs(
            build_memory_update_parser(),
            scope="agent",
            group="campaign_memory_stage",
            source_module="project.research.update_campaign_memory",
            include_prefixes=(
                "promising_top_k",
                "avoid_top_k",
                "repair_top_k",
                "exploit_top_k",
                "frontier_untested_top_k",
                "frontier_repair_top_k",
                "exhausted_failure_threshold",
            ),
        )
    )
    rows.extend(build_config_knob_rows())
    rows.extend(build_code_knob_rows(profile_promotion_defaults))
    return rows


def _flatten_config_rows(prefix: str, payload: Any) -> list[tuple[str, Any]]:
    rows: list[tuple[str, Any]] = []
    if isinstance(payload, dict):
        for key, value in sorted(payload.items()):
            next_prefix = f"{prefix}.{key}" if prefix else str(key)
            rows.extend(_flatten_config_rows(next_prefix, value))
        return rows
    rows.append((prefix, payload))
    return rows


def _config_group_rows(
    *,
    group: str,
    payload: dict[str, Any],
    source_module: str,
    path: Path,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name, value in _flatten_config_rows("", payload):
        if not name:
            continue
        rows.append(
            _knob_row(
                scope="agent",
                group=group,
                name=name,
                value=value,
                source_module=source_module,
                description=f"Config-backed agent knob from {path.name}.",
                agent_level=_default_agent_level(group, name),
                mutability=_default_mutability(group, name),
                risk=_default_risk(group, name),
            )
        )
    return rows


def build_config_knob_rows() -> list[dict[str, Any]]:
    repo_root = Path.cwd()
    specs = [
        (
            "phase2_gates",
            repo_root / "spec" / "gates.yaml",
            "spec.gates",
            (
                "gate_v1_phase2",
                "gate_v1_phase2_profiles",
                "promotion_confirmatory_gates",
                "gate_v1_bridge",
                "shrinkage_parameters",
            ),
        ),
        (
            "retail_profiles",
            repo_root / "project" / "configs" / "retail_profiles.yaml",
            "project.configs.retail_profiles",
            ("profiles",),
        ),
        (
            "objective",
            repo_root / "spec" / "objectives" / "retail_profitability.yaml",
            "spec.objectives.retail_profitability",
            ("objective",),
        ),
        (
            "search_limits",
            repo_root / "project" / "configs" / "registries" / "search_limits.yaml",
            "project.configs.registries.search_limits",
            ("limits", "defaults"),
        ),
    ]
    rows: list[dict[str, Any]] = []
    for group, path, source_module, top_keys in specs:
        payload = load_yaml_path(path)
        if not isinstance(payload, dict):
            continue
        subset = {key: payload.get(key) for key in top_keys if key in payload}
        rows.extend(
            _config_group_rows(
                group=group,
                payload=subset,
                source_module=source_module,
                path=path,
            )
        )
    return rows


def build_code_knob_rows(profile_defaults: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for profile_name, values in sorted(profile_defaults.items()):
        for key, value in sorted(values.items()):
            rows.append(
                _knob_row(
                    scope="agent",
                    group="promotion_profile_defaults",
                    name=f"{profile_name}.{key}",
                    value=value,
                    source_module="project.pipelines.stages.research",
                    description=f"Default threshold for the {profile_name} promotion profile.",
                    agent_level="core",
                    mutability="proposal_settable",
                    risk="medium",
                )
            )

    policy_rows = [
        (
            "promotion_policy_resolution",
            "auto_profile.production_like_modes",
            ["production", "certification", "confirmatory", "promotion", "deploy"],
            "Run modes that resolve auto promotion policy to deploy mode.",
            "project.research.services.promotion_service",
        ),
        (
            "promotion_policy_resolution",
            "auto_profile.other_modes_default",
            "research",
            "Fallback auto promotion profile when the run mode is not deploy-like.",
            "project.research.services.promotion_service",
        ),
        (
            "promotion_policy_resolution",
            "research.min_net_expectancy_bps_cap",
            1.5,
            "Research promotion caps retail net expectancy hardness at this value even if the objective spec is stricter.",
            "project.research.services.promotion_service",
        ),
        (
            "promotion_policy_resolution",
            "research.require_retail_viability",
            False,
            "Research promotion does not hard-block on retail viability.",
            "project.research.services.promotion_service",
        ),
        (
            "promotion_policy_resolution",
            "research.require_low_capital_viability",
            False,
            "Research promotion does not hard-block on low-capital viability.",
            "project.research.services.promotion_service",
        ),
        (
            "promotion_policy_resolution",
            "research.enforce_baseline_beats_complexity",
            False,
            "Research promotion treats baseline-vs-complexity as non-blocking.",
            "project.research.services.promotion_service",
        ),
        (
            "promotion_policy_resolution",
            "research.enforce_placebo_controls",
            False,
            "Research promotion treats the placebo-control bundle as non-blocking.",
            "project.research.services.promotion_service",
        ),
        (
            "promotion_policy_resolution",
            "research.enforce_timeframe_consensus",
            False,
            "Research promotion treats timeframe consensus as non-blocking.",
            "project.research.services.promotion_service",
        ),
        (
            "promotion_policy_resolution",
            "deploy.dynamic_min_events_from_state_registry",
            True,
            "Deploy promotion inflates event minimums using state_registry event floors.",
            "project.research.services.promotion_service",
        ),
        (
            "promotion_policy_resolution",
            "deploy.require_retail_viability",
            True,
            "Deploy promotion hard-blocks on retail viability when the resolved objective contract requires it.",
            "project.research.services.promotion_service",
        ),
        (
            "promotion_policy_resolution",
            "deploy.require_low_capital_viability",
            True,
            "Deploy promotion hard-blocks on low-capital viability when the resolved objective contract requires it.",
            "project.research.services.promotion_service",
        ),
        (
            "promotion_policy_resolution",
            "deploy.enforce_baseline_beats_complexity",
            True,
            "Deploy promotion hard-blocks when the candidate does not beat the baseline complexity benchmark.",
            "project.research.services.promotion_service",
        ),
        (
            "promotion_policy_resolution",
            "deploy.enforce_placebo_controls",
            True,
            "Deploy promotion requires the placebo-control bundle to pass.",
            "project.research.services.promotion_service",
        ),
        (
            "promotion_policy_resolution",
            "deploy.enforce_timeframe_consensus",
            True,
            "Deploy promotion requires timeframe consensus to pass.",
            "project.research.services.promotion_service",
        ),
        (
            "promotion_timeframe_consensus",
            "base_timeframe",
            "5m",
            "Base timeframe used by promotion-timeframe consensus evaluation.",
            "project.research.promotion.promotion_decisions",
        ),
        (
            "promotion_timeframe_consensus",
            "alternate_timeframes",
            ["1m", "15m"],
            "Alternate timeframes checked during promotion-timeframe consensus evaluation.",
            "project.research.promotion.promotion_decisions",
        ),
        (
            "promotion_timeframe_consensus",
            "min_consensus_ratio",
            0.3,
            "Hard-coded minimum retention ratio used by promotion-timeframe consensus evaluation.",
            "project.research.promotion.promotion_decisions",
        ),
    ]

    for group, name, value, description, source_module in policy_rows:
        rows.append(
            _knob_row(
                scope="agent",
                group=group,
                name=name,
                value=value,
                source_module=source_module,
                description=description,
                agent_level=_default_agent_level(group, name),
                mutability=_default_mutability(group, name),
                risk=_default_risk(group, name),
            )
        )
    return rows
