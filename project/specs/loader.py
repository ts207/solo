from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

from project.spec_registry import load_yaml_path
from project.specs.schema_validation import ObjectiveSpec, validate_spec

DEFAULT_GLOBAL_DEFAULTS_ENV_VAR = "BACKTEST_GLOBAL_DEFAULTS_PATH"
DEFAULT_OBJECTIVE_SPEC_ENV_VAR = "BACKTEST_OBJECTIVE_SPEC_PATH"
DEFAULT_OBJECTIVE_NAME_ENV_VAR = "BACKTEST_OBJECTIVE_NAME"
DEFAULT_RETAIL_PROFILES_SPEC_ENV_VAR = "BACKTEST_RETAIL_PROFILES_PATH"
DEFAULT_RETAIL_PROFILE_NAME_ENV_VAR = "BACKTEST_RETAIL_PROFILE"


def _safe_defaults(payload: object) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    defaults = payload.get("defaults", {})
    if not isinstance(defaults, dict):
        return {}
    return dict(defaults)


def _safe_objective(payload: object) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    raw = payload.get("objective", payload)
    if not isinstance(raw, dict):
        return {}
    out = dict(raw)
    if not isinstance(out.get("score_weights"), dict):
        out["score_weights"] = {}
    if not isinstance(out.get("hard_gates"), dict):
        out["hard_gates"] = {}
    if not isinstance(out.get("constraints"), dict):
        out["constraints"] = {}
    return out


def _safe_profiles(payload: object) -> Dict[str, Dict[str, Any]]:
    if not isinstance(payload, dict):
        return {}
    raw_profiles = payload.get("profiles", payload)
    if not isinstance(raw_profiles, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for name, cfg in raw_profiles.items():
        key = str(name).strip()
        if key and isinstance(cfg, dict):
            out[key] = dict(cfg)
    return out


def load_global_defaults(
    *,
    project_root: Path,
    explicit_path: str | Path | None = None,
    env_var: str = DEFAULT_GLOBAL_DEFAULTS_ENV_VAR,
    required: bool = False,
) -> Dict[str, Any]:
    candidates: list[Path] = []
    if explicit_path:
        candidates.append(Path(explicit_path))
    env_path = str(os.getenv(env_var, "")).strip()
    if env_path:
        candidates.append(Path(env_path))
    candidates.append(Path(project_root).resolve().parent / "spec" / "global_defaults.yaml")

    for path in candidates:
        if path.exists():
            return _safe_defaults(load_yaml_path(path))

    if required:
        raise FileNotFoundError(
            "Unable to locate global defaults spec. Checked: "
            + ", ".join(str(p) for p in candidates)
        )
    return {}


def load_objective_spec(
    *,
    project_root: Path,
    objective_name: str = "retail_profitability",
    explicit_path: str | Path | None = None,
    env_var: str = DEFAULT_OBJECTIVE_SPEC_ENV_VAR,
    required: bool = False,
) -> Dict[str, Any]:
    resolved_name = (
        str(objective_name).strip()
        or str(os.getenv(DEFAULT_OBJECTIVE_NAME_ENV_VAR, "")).strip()
        or "retail_profitability"
    )

    candidates: list[Path] = []
    if explicit_path:
        candidates.append(Path(explicit_path))
    env_path = str(os.getenv(env_var, "")).strip()
    if env_path:
        candidates.append(Path(env_path))
    candidates.append(
        Path(project_root).resolve().parent / "spec" / "objectives" / f"{resolved_name}.yaml"
    )

    for path in candidates:
        if path.exists():
            raw = load_yaml_path(path)
            loaded = _safe_objective(raw)
            if loaded:
                loaded.setdefault("id", resolved_name)
                # Validate using Pydantic to catch structural issues
                try:
                    validate_spec(loaded, ObjectiveSpec)
                except ValueError as exc:
                    raise ValueError(f"Invalid objective spec at {path}: {exc}") from exc
            return loaded

    if required:
        raise FileNotFoundError(
            "Unable to locate objective spec. Checked: " + ", ".join(str(p) for p in candidates)
        )
    return {}


def load_retail_profiles_spec(
    *,
    project_root: Path,
    explicit_path: str | Path | None = None,
    env_var: str = DEFAULT_RETAIL_PROFILES_SPEC_ENV_VAR,
    required: bool = False,
) -> Dict[str, Dict[str, Any]]:
    candidates: list[Path] = []
    if explicit_path:
        candidates.append(Path(explicit_path))
    env_path = str(os.getenv(env_var, "")).strip()
    if env_path:
        candidates.append(Path(env_path))
    candidates.append(Path(project_root).resolve() / "configs" / "retail_profiles.yaml")

    for path in candidates:
        if path.exists():
            profiles = _safe_profiles(load_yaml_path(path))
            if profiles:
                return profiles

    if required:
        raise FileNotFoundError(
            "Unable to locate retail profiles spec. Checked: "
            + ", ".join(str(p) for p in candidates)
        )
    return {}


def load_retail_profile(
    *,
    project_root: Path,
    profile_name: str = "capital_constrained",
    explicit_path: str | Path | None = None,
    env_var: str = DEFAULT_RETAIL_PROFILES_SPEC_ENV_VAR,
    required: bool = False,
) -> Dict[str, Any]:
    resolved_name = (
        str(profile_name).strip()
        or str(os.getenv(DEFAULT_RETAIL_PROFILE_NAME_ENV_VAR, "")).strip()
        or "capital_constrained"
    )
    profiles = load_retail_profiles_spec(
        project_root=project_root,
        explicit_path=explicit_path,
        env_var=env_var,
        required=required,
    )
    if not profiles:
        return {}
    profile = profiles.get(resolved_name)
    if isinstance(profile, dict):
        out = dict(profile)
        out.setdefault("id", resolved_name)
        return out
    if required:
        available = ", ".join(sorted(profiles.keys())) or "<none>"
        raise KeyError(
            f"Retail profile '{resolved_name}' not found in spec. Available: {available}"
        )
    return {}
