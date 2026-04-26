from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from project.experiments.config_loader import (
    _deep_merge,
    _set_nested_key,
    resolve_experiment_config,
)
from project.spec_registry import load_yaml_path

EFFECTIVE_CONFIG_SCHEMA_VERSION = "effective_run_config_v1"


def _argv_has_option(raw_argv: Iterable[str], option: str) -> bool:
    prefix = f"{option}="
    return any(token == option or str(token).startswith(prefix) for token in raw_argv)


def detect_explicit_cli_destinations(
    parser: argparse.ArgumentParser, raw_argv: list[str]
) -> list[str]:
    explicit: set[str] = set()
    for action in parser._actions:
        option_strings = list(getattr(action, "option_strings", []) or [])
        if not option_strings:
            continue
        if any(_argv_has_option(raw_argv, opt) for opt in option_strings):
            explicit.add(str(action.dest))
    return sorted(explicit)


def _load_overlay_config(path: str) -> dict[str, Any]:
    payload = load_yaml_path(Path(path)) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Overlay config must be a mapping: {path}")
    return dict(payload)


def _parser_defaults(parser: argparse.ArgumentParser) -> dict[str, Any]:
    namespace = parser.parse_args([])
    return vars(namespace)


def _filtered_args(parser: argparse.ArgumentParser, payload: dict[str, Any]) -> argparse.Namespace:
    allowed = {str(action.dest) for action in parser._actions if getattr(action, "dest", None)}
    namespace = argparse.Namespace()
    for key, value in payload.items():
        if key in allowed:
            setattr(namespace, key, value)
    return namespace


def _normalize_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(k): _normalize_jsonable(v)
            for k, v in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, list):
        return [_normalize_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [_normalize_jsonable(v) for v in value]
    return value


def resolve_effective_args(
    parser: argparse.ArgumentParser,
    raw_argv: list[str],
) -> tuple[argparse.Namespace, dict[str, Any]]:
    parsed_args = parser.parse_args(raw_argv)
    defaults = _parser_defaults(parser)
    effective: dict[str, Any] = dict(defaults)

    experiment_config_path = (
        str(getattr(parsed_args, "experiment_config", "") or "").strip() or None
    )
    experiment_config: dict[str, Any] = {}
    if experiment_config_path:
        experiment_config = resolve_experiment_config(experiment_config_path, overrides=[])
        _deep_merge(effective, experiment_config)

    config_overlay_paths = [str(path) for path in getattr(parsed_args, "config", []) or []]
    config_overlays: list[dict[str, Any]] = []
    for path in config_overlay_paths:
        overlay = _load_overlay_config(path)
        config_overlays.append({"path": path, "values": overlay})
        _deep_merge(effective, overlay)

    explicit_cli_destinations = detect_explicit_cli_destinations(parser, raw_argv)
    parsed_values = vars(parsed_args)
    for dest in explicit_cli_destinations:
        effective[dest] = parsed_values.get(dest)

    override_values = [str(value) for value in getattr(parsed_args, "override", []) or []]
    for override in override_values:
        if "=" not in override:
            continue
        key_path, value = override.split("=", 1)
        _set_nested_key(effective, key_path.strip(), value.strip())

    args = _filtered_args(parser, effective)
    resolution = {
        "schema_version": EFFECTIVE_CONFIG_SCHEMA_VERSION,
        "precedence_order": [
            "parser_defaults",
            "experiment_config",
            "config_overlays",
            "explicit_cli_flags",
            "override_values",
        ],
        "raw_inputs": {
            "cli_argv": list(raw_argv),
            "experiment_config_path": experiment_config_path,
            "config_overlay_paths": config_overlay_paths,
            "override_values": override_values,
        },
        "defaults": _normalize_jsonable(defaults),
        "experiment_config": _normalize_jsonable(experiment_config),
        "config_overlays": _normalize_jsonable(config_overlays),
        "explicit_cli_destinations": explicit_cli_destinations,
        "resolved_args": _normalize_jsonable(vars(args)),
    }
    return args, resolution


def build_effective_config_payload(
    *,
    run_id: str,
    resolution: dict[str, Any],
    preflight: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "schema_version": EFFECTIVE_CONFIG_SCHEMA_VERSION,
        "run_id": run_id,
        "generated_at_utc": None,
        "raw_inputs": resolution["raw_inputs"],
        "config_resolution": {
            "precedence_order": resolution["precedence_order"],
            "explicit_cli_destinations": resolution["explicit_cli_destinations"],
            "defaults_version": "argparse_defaults_v1",
            "experiment_config_path": resolution["raw_inputs"]["experiment_config_path"],
            "config_overlay_paths": resolution["raw_inputs"]["config_overlay_paths"],
            "override_values": resolution["raw_inputs"]["override_values"],
            "cli_argv": resolution["raw_inputs"]["cli_argv"],
            "normalized_symbols": list(preflight["parsed_symbols"]),
            "normalized_timeframes": str(preflight.get("normalized_timeframes_csv", "")).split(","),
        },
        "resolved": {
            "args": resolution["resolved_args"],
            "preflight": {
                "run_id": run_id,
                "parsed_symbols": list(preflight["parsed_symbols"]),
                "timeframes": str(preflight.get("normalized_timeframes_csv", "")).split(","),
                "objective_name": preflight["objective_name"],
                "objective_spec_path": preflight["objective_spec_path"],
                "objective_spec_hash": preflight["objective_spec_hash"],
                "retail_profile_name": preflight["retail_profile_name"],
                "retail_profile_spec_path": preflight["retail_profile_spec_path"],
                "retail_profile_spec_hash": preflight["retail_profile_spec_hash"],
                "runtime_invariants_mode": preflight["runtime_invariants_mode"],
                "search_spec": preflight["search_spec"],
            },
        },
    }
    return _normalize_jsonable(payload)


def write_effective_config(
    *,
    data_root: Path,
    run_id: str,
    payload: dict[str, Any],
) -> tuple[Path, str]:
    path = Path(data_root) / "runs" / run_id / "effective_config.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    path.write_text(serialized, encoding="utf-8")
    digest = "sha256:" + hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    return path, digest
