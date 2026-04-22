from typing import List, Optional, Tuple

from project.domain.compiled_registry import get_domain_registry
from project.spec_validation.loaders import load_family_registry, load_ontology_events, load_ontology_states


def validate_ontology() -> List[Tuple[str, str]]:
    errors = []
    events = load_ontology_events()
    states = load_ontology_states()
    family_registry = load_family_registry()

    event_fams = set((family_registry or {}).get("event_families", {}))
    state_fams = set((family_registry or {}).get("state_families", {}))

    for eid, spec in events.items():
        fam = spec.get("canonical_family") or spec.get("family") or spec.get("research_family")
        if not fam:
            errors.append((f"ontology/events/{eid}.yaml", "Missing family/research_family"))
        elif fam not in event_fams:
            errors.append((f"ontology/events/{eid}.yaml", f"Undefined event family: {fam}"))

    for sid, spec in states.items():
        fam = spec.get("family")
        if not fam:
            errors.append((f"ontology/states/{sid}.yaml", "Missing family"))
        elif fam not in state_fams:
            errors.append((f"ontology/states/{sid}.yaml", f"Undefined state family: {fam}"))

    return errors


def get_event_ids_for_family(family_name: str) -> List[str]:
    return list(get_domain_registry().get_event_ids_for_family(family_name))


def get_event_ids_for_regime(family_name: str, *, executable_only: bool = False) -> List[str]:
    return list(get_domain_registry().get_event_ids_for_regime(family_name, executable_only=executable_only))


def get_state_ids_for_family(family_name: str) -> List[str]:
    return list(get_domain_registry().get_state_ids_for_family(family_name))


def get_event_family(event_id: str) -> Optional[str]:
    """Return the family name for a single event_id, or None if not found."""
    event = get_domain_registry().get_event(event_id)
    return event.canonical_regime if event is not None else None


def get_searchable_event_families() -> List[str]:
    return list(get_domain_registry().searchable_event_families)


def get_searchable_state_families() -> List[str]:
    return list(get_domain_registry().searchable_state_families)
