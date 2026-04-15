from typing import List, Tuple

from project.domain.compiled_registry import get_domain_registry


def validate_grammar() -> List[Tuple[str, str]]:
    errors = []
    registry = get_domain_registry()

    event_fams = registry.event_family_rows()
    state_fams = registry.state_family_rows()

    # Check template family defaults against searchable families
    templates = registry.template_registry()
    template_fams = templates.get("families", {}) if isinstance(templates, dict) else {}
    for tfam in template_fams:
        if tfam not in event_fams and tfam not in state_fams:
            errors.append(
                (
                    "templates/registry.yaml",
                    f"Undefined family in templates: {tfam}",
                )
            )

    return errors
