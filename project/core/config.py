from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Iterable

import yaml

from project import PROJECT_ROOT
from project.core.exceptions import ConfigurationError


def get_data_root() -> Path:
    """Resolves the root directory for data storage."""
    # Prioritize environment variable, then fallback to standard project-local path.
    # Prefer the explicit Edge session override when both are set. Several
    # regression tests rely on per-test EDGE_DATA_ROOT to shadow any leaked
    # BACKTEST_DATA_ROOT from earlier integration runs.
    raw = os.getenv("EDGE_DATA_ROOT") or os.getenv("BACKTEST_DATA_ROOT")
    if raw:
        return Path(raw).resolve()
    return (PROJECT_ROOT.parent / "data").resolve()


def _deep_merge(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            base[key] = _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def load_configs(paths: Iterable[str]) -> Dict[str, Any]:
    """
    Load and merge one or more YAML config files.
    Later configs override values from earlier ones.
    """
    merged: Dict[str, Any] = {}
    for path_str in paths:
        path = Path(path_str)
        if not path.exists():
            raise ConfigurationError(f"Config not found: {path}")
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception as e:
            raise ConfigurationError(f"Failed to parse YAML config {path}: {e}") from e
        if not isinstance(data, dict):
            raise ConfigurationError(f"Config must be a mapping: {path}")
        merged = _deep_merge(merged, data)
    return merged
