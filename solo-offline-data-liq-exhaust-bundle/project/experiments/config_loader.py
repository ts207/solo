from __future__ import annotations

import json
from pathlib import Path
from project.spec_registry import load_yaml_path
from typing import Any, Dict, List, Optional, Union


def _deep_merge(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    """Deeply merges 'incoming' dictionary into 'base' dictionary."""
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def _set_nested_key(data: Dict[str, Any], path: str, value: str) -> None:
    """Sets a value in a nested dictionary using a dot-separated path."""
    keys = path.split(".")
    curr = data
    for key in keys[:-1]:
        if key not in curr or not isinstance(curr[key], dict):
            curr[key] = {}
        curr = curr[key]

    # Try to parse value as JSON to handle types (bool, int, list, etc.)
    try:
        # Check if value is a string that represents a valid JSON
        # This handles lists [1,2,3], dicts {"a":1}, booleans true/false, numbers 123
        parsed_value = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        # Fallback to literal string if it's not valid JSON
        parsed_value = value

    curr[keys[-1]] = parsed_value


def resolve_experiment_config(
    config_path: Path | str, overrides: List[str] = None
) -> Dict[str, Any]:
    """
    Loads an experiment configuration from YAML, resolving inheritance and applying overrides.

    Inherited configs are deep-merged, with the current config taking precedence.
    Overrides are strings in 'key.path=value' format.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Experiment config not found: {path}")

    config = load_yaml_path(path) or {}

    if not isinstance(config, dict):
        raise ValueError(f"Experiment config must be a YAML mapping: {path}")

    # Handle 'inherits' recursively
    if "inherits" in config:
        inherits = config.pop("inherits")
        if isinstance(inherits, str):
            inherits = [inherits]

        merged_base: Dict[str, Any] = {}
        for base_path_str in inherits:
            base_path = Path(base_path_str)
            if not base_path.is_absolute():
                base_path = path.parent / base_path

            # Recursive call to resolve_experiment_config handles nested inheritance
            # Overrides are only applied at the final stage
            base_config = resolve_experiment_config(base_path)
            _deep_merge(merged_base, base_config)

        # Merge current config on top of merged bases
        config = _deep_merge(merged_base, config)

    # Apply overrides
    if overrides:
        for override in overrides:
            if "=" in override:
                key_path, value = override.split("=", 1)
                _set_nested_key(config, key_path.strip(), value.strip())

    return config
