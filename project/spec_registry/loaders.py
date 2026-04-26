from __future__ import annotations

import copy
import functools
import json
import os
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import yaml

from project import PROJECT_ROOT
from project.spec_registry.policy import _DEFAULT_BLUEPRINT_POLICY

REPO_ROOT = PROJECT_ROOT.parent
SPEC_ROOT = REPO_ROOT / "spec"

ONTOLOGY_SPEC_RELATIVE_PATHS: dict[str, str] = {
    "taxonomy": "spec/multiplicity/taxonomy.yaml",
    "canonical_event_registry": "spec/events/canonical_event_registry.yaml",
    "template_registry": "spec/templates/registry.yaml",
    "regime_registry": "spec/regimes/registry.yaml",
    "state_registry": "spec/states/state_registry.yaml",
    "state_family_registry": "spec/states/state_families.yaml",
    "thesis_registry": "spec/theses/thesis_registry.yaml",
    "template_verb_lexicon": "spec/hypotheses/template_verb_lexicon.yaml",
    "domain_graph": "spec/domain/domain_graph.yaml",
}

RUNTIME_SPEC_RELATIVE_PATHS: dict[str, str] = {
    "lanes": "spec/runtime/lanes.yaml",
    "firewall": "spec/runtime/firewall.yaml",
    "hashing": "spec/runtime/hashing.yaml",
}

_STATE_GENERATED_FILENAMES = {"state_registry.yaml", "state_families.yaml"}
_STATE_DEFAULTS_FILENAME = "state_defaults.yaml"


def repo_root() -> Path:
    return REPO_ROOT


def spec_root() -> Path:
    return SPEC_ROOT


def _read_yaml(path: Path, required: bool = True) -> dict[str, Any]:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Spec file missing: {path}")
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"Failed to parse YAML spec {path}: {exc}") from exc
    if not isinstance(payload, dict) and payload is not None:
         # Some YAML files might be empty or just a list, but our loaders expect dicts
         if required:
             raise ValueError(f"Spec file {path} must be a dictionary, got {type(payload)}")
    return payload if isinstance(payload, dict) else {}


def _deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = copy.deepcopy(dict(base))
    for key, value in dict(override).items():
        if isinstance(value, Mapping) and isinstance(out.get(key), Mapping):
            out[str(key)] = _deep_merge(dict(out[key]), dict(value))
        else:
            out[str(key)] = copy.deepcopy(value)
    return out


def resolve_relative_spec_path(relative_path: str | Path, repo_root: Path | None = None) -> Path:
    rel = Path(relative_path)
    base = Path(repo_root).resolve() if repo_root is not None else REPO_ROOT
    return (base / rel).resolve()


@functools.cache
def load_yaml_relative(relative_path: str) -> dict[str, Any]:
    return _read_yaml(resolve_relative_spec_path(relative_path))


def load_yaml_path(path: str | Path) -> dict[str, Any]:
    return _read_yaml(Path(path))


@functools.lru_cache(maxsize=1)
def load_gates_spec() -> dict[str, Any]:
    return load_yaml_relative("spec/gates.yaml")


@functools.lru_cache(maxsize=1)
def load_family_specs() -> dict[str, Any]:
    return load_yaml_relative("spec/multiplicity/families.yaml")


def load_family_spec(family_id: str) -> dict[str, Any]:
    payload = load_family_specs()
    families = payload.get("families", {}) if isinstance(payload, dict) else {}
    if not isinstance(families, dict):
        return {}
    row = families.get(family_id, {})
    return dict(row) if isinstance(row, dict) else {}


@functools.lru_cache(maxsize=1)
def load_unified_event_registry() -> dict[str, Any]:
    return load_yaml_relative("project/configs/registries/events.yaml")


@functools.lru_cache(maxsize=1)
def load_event_ontology_mapping() -> dict[str, Any]:
    return load_yaml_relative("spec/events/event_ontology_mapping.yaml")


@functools.lru_cache(maxsize=1)
def load_template_registry() -> dict[str, Any]:
    return load_yaml_relative("spec/templates/registry.yaml")


@functools.lru_cache(maxsize=1)
def load_regime_registry() -> dict[str, Any]:
    return load_yaml_relative("spec/regimes/registry.yaml")


def _iter_state_spec_paths() -> Iterable[Path]:
    state_dir = SPEC_ROOT / "states"
    for path in sorted(state_dir.glob("*.yaml")):
        if path.name in _STATE_GENERATED_FILENAMES:
            continue
        yield path


def _iter_state_definition_rows() -> Iterable[tuple[Path, dict[str, Any]]]:
    for path in _iter_state_spec_paths():
        if path.name == _STATE_DEFAULTS_FILENAME:
            continue
        row = _read_yaml(path, required=False)
        if not isinstance(row, dict):
            continue
        state_id = str(row.get("state_id", "")).strip().upper()
        if not state_id:
            continue
        normalized = copy.deepcopy(row)
        normalized["state_id"] = state_id
        normalized.setdefault("version", 1)
        normalized.setdefault("kind", "state_definition")
        yield path, normalized


def _iter_context_dimension_rows() -> Iterable[tuple[Path, dict[str, Any]]]:
    for path in _iter_state_spec_paths():
        if path.name == _STATE_DEFAULTS_FILENAME:
            continue
        row = _read_yaml(path, required=False)
        if not isinstance(row, dict):
            continue
        state_name = str(row.get("state_name", row.get("dimension_id", ""))).strip()
        if not state_name:
            continue
        mapping = row.get("mapping", {})
        if not isinstance(mapping, dict) or not mapping:
            continue
        normalized_mapping = {
            str(label).strip(): str(state_id).strip().upper()
            for label, state_id in mapping.items()
            if str(label).strip() and str(state_id).strip()
        }
        if not normalized_mapping:
            continue
        allowed_values = row.get("allowed_values", list(normalized_mapping.keys()))
        if not isinstance(allowed_values, list):
            allowed_values = list(normalized_mapping.keys())
        normalized = {
            "version": int(row.get("version", 1) or 1),
            "kind": str(row.get("kind", "state_context_dimension")).strip() or "state_context_dimension",
            "state_name": state_name,
            "source_feature": str(row.get("source_feature", "")).strip(),
            "allowed_values": [
                str(item).strip() for item in allowed_values if str(item).strip()
            ],
            "mapping": normalized_mapping,
        }
        for key in ("acceptance_test_id", "canonical_metrics", "thresholds", "update_cadence", "description"):
            value = row.get(key)
            if value in (None, "", [], {}):
                continue
            normalized[key] = copy.deepcopy(value)
        yield path, normalized


def _load_state_defaults() -> dict[str, Any]:
    path = SPEC_ROOT / "states" / _STATE_DEFAULTS_FILENAME
    payload = _read_yaml(path, required=False)
    defaults = payload.get("defaults", payload) if isinstance(payload, dict) else {}
    if not isinstance(defaults, dict):
        defaults = {}
    runtime = defaults.get("runtime", {})
    if not isinstance(runtime, dict):
        runtime = {}
    normalized = copy.deepcopy(defaults)
    normalized["state_scope"] = str(defaults.get("state_scope", "source_only")).strip() or "source_only"
    normalized["min_events"] = int(defaults.get("min_events", 200) or 200)
    normalized["runtime"] = {
        "enabled": bool(runtime.get("enabled", True)),
        "instrument_classes": [
            str(item).strip() for item in runtime.get("instrument_classes", ["crypto"]) if str(item).strip()
        ] or ["crypto"],
        "tags": [str(item).strip() for item in runtime.get("tags", []) if str(item).strip()],
    }
    return normalized


def _context_dimensions_from_registry() -> dict[str, dict[str, Any]]:
    payload = load_yaml_relative("project/configs/registries/contexts.yaml")
    raw_dimensions = payload.get("dimensions", {}) if isinstance(payload, dict) else {}
    if not isinstance(raw_dimensions, dict):
        return {}
    dimensions: dict[str, dict[str, Any]] = {}
    for raw_name, raw_cfg in raw_dimensions.items():
        name = str(raw_name).strip()
        if not name or not isinstance(raw_cfg, Mapping):
            continue
        raw_values = raw_cfg.get("values", {})
        if isinstance(raw_values, Mapping) or isinstance(raw_values, list):
            allowed_values = [str(value).strip() for value in raw_values if str(value).strip()]
        else:
            allowed_values = []
        if not allowed_values:
            continue
        dimensions[name] = {
            "allowed_values": allowed_values,
            "mapping": {},
        }
    return dimensions


@functools.lru_cache(maxsize=1)
def load_state_registry() -> dict[str, Any]:
    defaults = _load_state_defaults()
    context_dimensions = {
        str(row["state_name"]).strip(): {
            "allowed_values": list(row["allowed_values"]),
            "mapping": dict(row["mapping"]),
        }
        for _, row in _iter_context_dimension_rows()
    }
    context_dimensions.update(_context_dimensions_from_registry())
    state_rows = [row for _, row in _iter_state_definition_rows()]
    state_rows.sort(key=lambda row: str(row.get("state_id", "")).strip().upper())
    return {
        "version": 1,
        "kind": "state_registry",
        "metadata": {
            "status": "generated",
            "generated_notice": "GENERATED FILE - DO NOT EDIT",
            "authored_sources": [
                "spec/states/*.yaml",
            ],
        },
        "defaults": copy.deepcopy(defaults),
        "context_dimensions": copy.deepcopy(context_dimensions),
        "states": state_rows,
    }


@functools.lru_cache(maxsize=1)
def load_state_family_registry() -> dict[str, Any]:
    defaults = _load_state_defaults()
    context_rows = [row for _, row in _iter_context_dimension_rows()]
    context_rows.sort(key=lambda row: str(row.get("state_name", "")).strip())
    context_dimensions = {
        str(row["state_name"]).strip(): {
            "allowed_values": list(row["allowed_values"]),
            "mapping": dict(row["mapping"]),
        }
        for row in context_rows
    }
    context_dimensions.update(_context_dimensions_from_registry())
    family_rows: list[dict[str, Any]] = []
    for row in context_rows:
        family_row: dict[str, Any] = {
            "name": str(row["state_name"]).strip(),
            "allowed_values": list(row["allowed_values"]),
        }
        if row.get("source_feature"):
            family_row["source_feature"] = str(row["source_feature"]).strip()
        if row.get("canonical_metrics"):
            family_row["canonical_metrics"] = copy.deepcopy(row["canonical_metrics"])
        if row.get("thresholds"):
            family_row["thresholds"] = copy.deepcopy(row["thresholds"])
        if row.get("update_cadence"):
            family_row["update_cadence"] = row["update_cadence"]
        if row.get("acceptance_test_id"):
            family_row["acceptance_test_id"] = row["acceptance_test_id"]
        family_rows.append(family_row)
    existing_family_names = {str(row.get("name", "")).strip() for row in family_rows}
    for family_name, cfg in sorted(context_dimensions.items()):
        if family_name in existing_family_names:
            continue
        family_rows.append(
            {
                "name": family_name,
                "allowed_values": list(cfg.get("allowed_values", [])),
            }
        )
    return {
        "version": 1,
        "kind": "state_family_registry",
        "metadata": {
            "status": "generated",
            "generated_notice": "GENERATED FILE - DO NOT EDIT",
            "authored_sources": ["spec/states/*.yaml"],
            "read_model": True,
        },
        "defaults": copy.deepcopy(defaults),
        "context_dimensions": copy.deepcopy(context_dimensions),
        "state_families": family_rows,
    }


@functools.lru_cache(maxsize=1)
def load_thesis_registry() -> dict[str, Any]:
    return load_yaml_relative("spec/theses/thesis_registry.yaml")


@functools.lru_cache(maxsize=1)
def load_event_contract_overrides() -> dict[str, Any]:
    return load_yaml_relative("spec/events/event_contract_overrides.yaml")


@functools.cache
def load_runtime_spec(name: str) -> dict[str, Any]:
    normalized = str(name).strip().lower()
    if not normalized:
        return {}
    return load_yaml_relative(f"spec/runtime/{normalized}.yaml")


@functools.lru_cache(maxsize=1)
def load_blueprint_policy_spec(policy_path: str | None = None) -> dict[str, Any]:
    if policy_path:
        raw = load_yaml_path(Path(policy_path).resolve())
    else:
        raw = load_yaml_relative("spec/blueprint_policies.yaml")
    return _deep_merge(_DEFAULT_BLUEPRINT_POLICY, raw)


def _safe_objective(payload: object) -> dict[str, Any]:
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


def _safe_profiles(payload: object) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict):
        return {}
    raw_profiles = payload.get("profiles", payload)
    if not isinstance(raw_profiles, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for name, cfg in raw_profiles.items():
        key = str(name).strip()
        if key and isinstance(cfg, dict):
            out[key] = dict(cfg)
    return out


def load_objective_spec(
    *,
    objective_name: str = "retail_profitability",
    explicit_path: str | Path | None = None,
    required: bool = False,
) -> dict[str, Any]:
    resolved_name = str(objective_name).strip() or "retail_profitability"
    candidates: list[Path] = []
    if explicit_path:
        candidates.append(Path(explicit_path))
    env_path = str(os.getenv("BACKTEST_OBJECTIVE_SPEC_PATH", "")).strip()
    if env_path:
        candidates.append(Path(env_path))
    candidates.append(SPEC_ROOT / "objectives" / f"{resolved_name}.yaml")
    for path in candidates:
        if path.exists():
            loaded = _safe_objective(_read_yaml(path))
            if loaded:
                loaded.setdefault("id", resolved_name)
            return loaded
    if required:
        raise FileNotFoundError(
            "Unable to locate objective spec. Checked: " + ", ".join(str(p) for p in candidates)
        )
    return {}


def load_retail_profiles_spec(
    *, explicit_path: str | Path | None = None, required: bool = False
) -> dict[str, dict[str, Any]]:
    candidates: list[Path] = []
    if explicit_path:
        candidates.append(Path(explicit_path))
    env_path = str(os.getenv("BACKTEST_RETAIL_PROFILES_PATH", "")).strip()
    if env_path:
        candidates.append(Path(env_path))
    candidates.append(PROJECT_ROOT / "configs" / "retail_profiles.yaml")
    for path in candidates:
        if path.exists():
            profiles = _safe_profiles(_read_yaml(path))
            if profiles:
                return profiles
    if required:
        raise FileNotFoundError(
            "Unable to locate retail profile spec. Checked: "
            + ", ".join(str(p) for p in candidates)
        )
    return {}


def load_retail_profile(
    *,
    profile_name: str = "capital_constrained",
    explicit_path: str | Path | None = None,
    required: bool = False,
) -> dict[str, Any]:
    resolved_name = str(profile_name).strip() or "capital_constrained"
    profiles = load_retail_profiles_spec(explicit_path=explicit_path, required=required)
    if not profiles:
        return {}
    row = profiles.get(resolved_name)
    if isinstance(row, dict):
        out = dict(row)
        out.setdefault("id", resolved_name)
        return out
    if required:
        available = ", ".join(sorted(profiles.keys())) or "<none>"
        raise KeyError(f"Retail profile '{resolved_name}' not found. Available: {available}")
    return {}


@functools.cache
def load_hypothesis_spec(name: str) -> dict[str, Any]:
    normalized = str(name).strip()
    if not normalized:
        return {}
    return load_yaml_relative(f"spec/hypotheses/{normalized}.yaml")


@functools.cache
def load_concept_spec(concept_id: str) -> dict[str, Any]:
    normalized = str(concept_id).strip()
    if not normalized:
        return {}
    return load_yaml_relative(f"spec/concepts/{normalized}.yaml")


@functools.cache
def load_global_defaults() -> dict[str, Any]:
    return load_yaml_relative("spec/global_defaults.yaml")


@functools.cache
def load_event_spec(event_type: str) -> dict[str, Any]:
    normalized = str(event_type).strip()
    if not normalized:
        return {}
    return load_yaml_relative(f"spec/events/{normalized}.yaml")


def ontology_spec_paths(repo_root: Path | None = None) -> dict[str, Path]:
    return {
        key: resolve_relative_spec_path(rel, repo_root=repo_root)
        for key, rel in ONTOLOGY_SPEC_RELATIVE_PATHS.items()
    }


def runtime_spec_paths(repo_root: Path | None = None) -> dict[str, Path]:
    return {
        key: resolve_relative_spec_path(rel, repo_root=repo_root)
        for key, rel in RUNTIME_SPEC_RELATIVE_PATHS.items()
    }


def feature_schema_registry_path(version: str | None = None) -> Path:
    token = str(version or os.getenv("BACKTEST_FEATURE_SCHEMA_VERSION", "v2")).strip().lower()
    if token != "v2":
        token = "v2"
    return (PROJECT_ROOT / "schemas" / f"feature_schema_{token}.json").resolve()


def load_feature_schema_registry(version: str | None = None) -> dict[str, Any]:
    path = feature_schema_registry_path(version)
    if not path.exists():
        raise ValueError(f"Feature schema registry missing: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Feature schema registry is invalid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Feature schema registry must be a JSON object: {path}")
    return payload


_GENERATED_SPEC_SUBDIRS = ("ontology",)
_GENERATED_SPEC_FILES = ("templates/event_template_registry.yaml",)


def iter_spec_yaml_files(repo_root: Path | None = None) -> list[Path]:
    base = (
        spec_root()
        if repo_root is None
        else resolve_relative_spec_path("spec", repo_root=repo_root)
    )
    generated_dirs = {base / d for d in _GENERATED_SPEC_SUBDIRS}
    generated_files = {base / f for f in _GENERATED_SPEC_FILES}
    files = [
        p
        for p in base.rglob("*.yaml")
        if p.is_file()
        and not any(p == g or g in p.parents for g in generated_dirs)
        and p not in generated_files
    ]
    return sorted(files)


def canonical_yaml_hash(path: Path) -> str:
    payload = load_yaml_path(path)
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    )


def compute_spec_digest(relative_paths: Iterable[str | Path]) -> str:
    entries: list[dict[str, Any]] = []
    for rel in relative_paths:
        path = resolve_relative_spec_path(rel)
        if not path.exists():
            entries.append({"path": str(rel), "exists": False})
            continue
        entries.append(
            {
                "path": str(rel),
                "exists": True,
                "content": path.read_text(encoding="utf-8"),
            }
        )
    return json.dumps(entries, sort_keys=True)


def clear_caches() -> None:
    load_yaml_relative.cache_clear()
    load_gates_spec.cache_clear()
    load_family_specs.cache_clear()
    load_unified_event_registry.cache_clear()
    load_event_ontology_mapping.cache_clear()
    load_template_registry.cache_clear()
    load_regime_registry.cache_clear()
    load_state_family_registry.cache_clear()
    load_state_registry.cache_clear()
    load_thesis_registry.cache_clear()
    load_event_contract_overrides.cache_clear()
    load_runtime_spec.cache_clear()
    load_blueprint_policy_spec.cache_clear()
    load_hypothesis_spec.cache_clear()
    load_concept_spec.cache_clear()
    load_global_defaults.cache_clear()
    load_event_spec.cache_clear()
