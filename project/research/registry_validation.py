from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

from project.spec_registry import load_template_registry

_LOG = logging.getLogger(__name__)


def validate_agent_selections(
    *,
    events: Optional[List[str]] = None,
    templates: Optional[List[str]] = None,
    horizons: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Validates agent-provided subsets against the authoritative registry.

    Returns a dict with validated subsets or raises ValueError if invalid selections are made.
    """
    registry = load_template_registry()
    if not registry:
        _LOG.warning("No event template registry found. Skipping validation.")
        return {
            "events": events,
            "templates": templates,
            "horizons": horizons,
        }

    allowed_events = set(registry.get("events", {}).keys())
    allowed_templates = set(registry.get("defaults", {}).get("templates", []))
    for fam in registry.get("families", {}).values():
        allowed_templates.update(fam.get("templates", []))
    for evt in registry.get("events", {}).values():
        allowed_templates.update(evt.get("templates", []))

    allowed_horizons = set(registry.get("defaults", {}).get("horizons", []))
    for evt in registry.get("events", {}).values():
        allowed_horizons.update(evt.get("horizons", []))

    validated_events = []
    if events:
        for evt in events:
            if evt not in allowed_events:
                raise ValueError(f"Event ID '{evt}' is not in the authoritative registry.")
            validated_events.append(evt)

    validated_templates = []
    if templates:
        for tpl in templates:
            if tpl not in allowed_templates:
                raise ValueError(f"Template '{tpl}' is not in the authoritative registry.")
            validated_templates.append(tpl)

    validated_horizons = []
    if horizons:
        for hz in horizons:
            if hz not in allowed_horizons:
                # Some horizons might be dynamic, but we should at least log or check common ones
                _LOG.info(
                    "Horizon '%s' not explicitly in registry defaults, allowing as dynamic.", hz
                )
            validated_horizons.append(hz)

    return {
        "events": validated_events if validated_events else None,
        "templates": validated_templates if validated_templates else None,
        "horizons": validated_horizons if validated_horizons else None,
    }


def filter_event_chain(
    full_chain: List[tuple[str, str, List[str]]],
    selected_events: Optional[List[str]] = None,
) -> List[tuple[str, str, List[str]]]:
    """Filters the PHASE2_EVENT_CHAIN based on agent-selected events."""
    if not selected_events:
        return full_chain

    selected_set = set(selected_events)
    return [item for item in full_chain if item[0] in selected_set]
