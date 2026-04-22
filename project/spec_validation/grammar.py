from typing import List, Tuple

from project.domain.compiled_registry import get_domain_registry
from project.spec_validation.loaders import load_family_registry


def validate_grammar() -> List[Tuple[str, str]]:
    errors = []
    family_registry = load_family_registry()
    event_fams = set((family_registry or {}).get("event_families", {}))
    state_fams = set((family_registry or {}).get("state_families", {}))

    templates = get_domain_registry().template_registry()
    template_fams = templates.get("families", {}) if isinstance(templates, dict) else {}
    for tfam in template_fams:
        if tfam not in event_fams and tfam not in state_fams:
            errors.append(("templates/registry.yaml", f"Undefined family in templates: {tfam}"))

    return errors
