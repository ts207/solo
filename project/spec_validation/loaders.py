from pathlib import Path
from typing import Any

import yaml

from project.spec_registry import load_state_registry
from project.specs.ontology import normalize_state_registry_records

REPO_ROOT = Path(__file__).parents[2]
# print(f"DEBUG: REPO_ROOT is {REPO_ROOT.absolute()}")
# print(f"DEBUG: SPEC_DIR is {SPEC_DIR.absolute()}")
SPEC_DIR = REPO_ROOT / "spec"
ONTOLOGY_DIR = SPEC_DIR / "ontology"
GRAMMAR_DIR = SPEC_DIR / "grammar"
SEARCH_DIR = SPEC_DIR / "search"


def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_ontology_events() -> dict[str, dict[str, Any]]:
    from project.events.contract_registry import load_active_event_contracts

    events: dict[str, dict[str, Any]] = {}
    for event_type, contract in load_active_event_contracts().items():
        raw = dict(contract.get("raw", {}))
        raw.setdefault("event_type", event_type)
        raw.setdefault("research_family", contract.get("research_family", contract.get("canonical_family", "")))
        raw.setdefault("canonical_family", contract.get("canonical_family", ""))
        raw.setdefault("canonical_regime", contract.get("canonical_regime", ""))
        raw.setdefault("family", contract.get("research_family", contract.get("canonical_family", "")))
        raw.setdefault("tier", contract.get("tier", ""))
        raw.setdefault("operational_role", contract.get("operational_role", ""))
        events[event_type] = raw
    return events


def load_ontology_states() -> dict[str, dict[str, Any]]:
    states = {}
    canonical_registry = load_state_registry()
    for row in normalize_state_registry_records(canonical_registry):
        state_id = str(row.get("state_id", "")).strip()
        if state_id:
            states[state_id] = dict(row)
    return states


def load_family_registry(root: Path | None = None) -> dict[str, Any]:
    base = root if root else REPO_ROOT
    return load_yaml(base / "spec" / "grammar" / "family_registry.yaml")


def load_template_registry(root: Path | None = None) -> dict[str, Any]:
    base = root if root else REPO_ROOT
    return load_yaml(base / "spec" / "templates" / "registry.yaml")


def load_search_spec(name: str, repo_root: Path | None = None) -> dict[str, Any]:
    # e.g. name="phase1" -> spec/search/search_phase1.yaml
    base = repo_root if repo_root else REPO_ROOT
    path = base / "spec" / "search" / f"search_{name}.yaml"
    if not path.exists():
        # fallback for direct paths
        path = Path(name)
        if repo_root and not path.is_absolute():
            path = repo_root / name

    doc = load_yaml(path)
    from project.spec_validation.search import validate_search_spec_doc

    validate_search_spec_doc(doc, source=str(path))
    return doc
