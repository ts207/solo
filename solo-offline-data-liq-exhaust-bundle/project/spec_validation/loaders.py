import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional

from project.spec_registry import load_state_registry
from project.specs.ontology import normalize_state_registry_records

REPO_ROOT = Path(__file__).parents[2]
# print(f"DEBUG: REPO_ROOT is {REPO_ROOT.absolute()}")
# print(f"DEBUG: SPEC_DIR is {SPEC_DIR.absolute()}")
SPEC_DIR = REPO_ROOT / "spec"
ONTOLOGY_DIR = SPEC_DIR / "ontology"
GRAMMAR_DIR = SPEC_DIR / "grammar"
SEARCH_DIR = SPEC_DIR / "search"


def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_ontology_events() -> Dict[str, Dict[str, Any]]:
    from project.events.contract_registry import load_active_event_contracts

    events: Dict[str, Dict[str, Any]] = {}
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


def load_ontology_states() -> Dict[str, Dict[str, Any]]:
    states = {}
    canonical_registry = load_state_registry()
    for row in normalize_state_registry_records(canonical_registry):
        state_id = str(row.get("state_id", "")).strip()
        if state_id:
            states[state_id] = dict(row)
    return states


def load_family_registry() -> Dict[str, Any]:
    return load_yaml(GRAMMAR_DIR / "family_registry.yaml")


def load_template_registry() -> Dict[str, Any]:
    return load_yaml(SPEC_DIR / "templates" / "registry.yaml")


def load_search_spec(name: str) -> Dict[str, Any]:
    # e.g. name="phase1" -> spec/search/search_phase1.yaml
    path = SEARCH_DIR / f"search_{name}.yaml"
    if not path.exists():
        # fallback for direct paths
        path = Path(name)
    doc = load_yaml(path)
    from project.spec_validation.search import validate_search_spec_doc

    validate_search_spec_doc(doc, source=str(path))
    return doc
