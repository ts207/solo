from __future__ import annotations

from project.events.event_specs import EVENT_REGISTRY_SPECS, REGISTRY_BACKED_SIGNALS


def test_event_registry_specs_loaded_from_registry():
    assert EVENT_REGISTRY_SPECS
    assert REGISTRY_BACKED_SIGNALS
