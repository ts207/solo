from __future__ import annotations

from pathlib import Path
from typing import Any

from project import PROJECT_ROOT
from project.domain.registry_loader import compile_domain_registry_from_sources
from project.spec_registry import load_template_registry, resolve_relative_spec_path

REPO_ROOT = PROJECT_ROOT.parent
_STATE_GENERATED_FILENAMES = {"state_registry.yaml", "state_families.yaml"}


def _repo_relative(path: str | Path) -> str:
    candidate = Path(path)
    if not candidate.is_absolute():
        return candidate.as_posix()
    try:
        return candidate.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except Exception:
        return candidate.as_posix()


def build_canonical_semantic_registry_views(
    *,
    domain_registry: Any | None = None,
) -> dict[str, dict[str, Any]]:
    domain = domain_registry or compile_domain_registry_from_sources()
    template_registry = load_template_registry()

    event_rows: dict[str, dict[str, Any]] = {}
    for event_id, spec in sorted(domain.event_definitions.items()):
        family_name = str(spec.research_family or spec.canonical_family or spec.canonical_regime).strip().upper()
        event_rows[event_id] = {
            "enabled": bool(spec.enabled),
            "planning_eligible": bool(spec.planning_eligible),
            "family": family_name,
            "research_family": str(spec.research_family).strip().upper(),
            "canonical_family": str(spec.canonical_family).strip().upper(),
            "canonical_regime": str(spec.canonical_regime).strip().upper(),
            "instrument_classes": list(spec.instrument_classes),
            "requires_features": list(spec.requires_features),
            "sequence_eligible": bool(spec.sequence_eligible),
            "tags": list(spec.runtime_tags),
            "detector": str(spec.detector_name).strip(),
            "tier": str(spec.tier).strip().upper(),
            "operational_role": str(spec.operational_role).strip(),
            "source_path": _repo_relative(spec.spec_path),
        }

    state_rows: dict[str, dict[str, Any]] = {}
    for state_id, spec in sorted(domain.state_definitions.items()):
        state_rows[state_id] = {
            "enabled": bool(spec.enabled),
            "family": str(spec.family).strip().upper(),
            "source_event_type": str(spec.source_event_type).strip().upper(),
            "instrument_classes": list(spec.instrument_classes),
            "tags": list(spec.runtime_tags),
            "state_engine": str(spec.state_engine).strip(),
            "description": str(spec.description).strip(),
            "allowed_templates": list(spec.allowed_templates),
            "source_path": _repo_relative(spec.spec_path),
        }

    operators = template_registry.get("operators", {})
    if not isinstance(operators, dict):
        operators = {}
    template_rows: dict[str, dict[str, Any]] = {}
    for template_id, row in sorted(operators.items()):
        if not isinstance(row, dict):
            continue
        template_rows[str(template_id)] = {
            "enabled": bool(row.get("enabled", True)),
            "template_kind": str(row.get("template_kind", "")).strip().lower(),
            "supports_contexts": bool(row.get("supports_contexts", True)),
            "supports_directions": [
                str(item).strip()
                for item in row.get("supports_directions", [])
                if str(item).strip()
            ],
            "supports_trigger_types": [
                str(item).strip().upper()
                for item in row.get("supports_trigger_types", [])
                if str(item).strip()
            ],
            "compatible_families": [
                str(item).strip().upper()
                for item in row.get("compatible_families", [])
                if str(item).strip()
            ],
            "source_path": "spec/templates/registry.yaml",
        }

    families = template_registry.get("families", {})
    if not isinstance(families, dict):
        families = {}
    family_rows: dict[str, dict[str, Any]] = {}
    for family_name, row in sorted(families.items()):
        if not isinstance(row, dict):
            continue
        family_rows[str(family_name).strip().upper()] = {
            "allowed_templates": [
                str(item).strip()
                for item in row.get("templates", row.get("allowed_templates", []))
                if str(item).strip()
            ],
            "source_path": "spec/templates/registry.yaml",
        }

    return {
        "events": {"events": event_rows},
        "states": {"states": state_rows},
        "templates": {"templates": template_rows, "families": family_rows},
    }


def canonical_semantic_source_paths() -> dict[str, list[Path]]:
    event_dir = resolve_relative_spec_path("spec/events", repo_root=REPO_ROOT)
    event_paths = [
        path
        for path in sorted(event_dir.glob("*.yaml"))
        if not path.name.startswith("_")
        and path.name not in {
            "canonical_event_registry.yaml",
            "event_contract_overrides.yaml",
            "event_ontology_mapping.yaml",
            "event_registry_unified.yaml",
        }
    ]
    state_dir = resolve_relative_spec_path("spec/states", repo_root=REPO_ROOT)
    state_paths = [
        path
        for path in sorted(state_dir.glob("*.yaml"))
        if path.name not in _STATE_GENERATED_FILENAMES
    ]
    return {
        "events": event_paths,
        "templates": [
            resolve_relative_spec_path("spec/templates/registry.yaml", repo_root=REPO_ROOT)
        ],
        "states": state_paths,
    }


def runtime_config_source_paths(registry_root: Path) -> dict[str, list[Path]]:
    root = Path(registry_root)
    return {
        "contexts": [root / "contexts.yaml"],
        "detectors": [root / "detectors.yaml"],
        "features": [root / "features.yaml"],
        "search_limits": [root / "search_limits.yaml"],
    }
