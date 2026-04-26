from __future__ import annotations

from .factory import ScenarioSpec

try:
    from . import definitions  # noqa: F401
except ImportError:
    pass

SCENARIO_REGISTRY: dict[str, ScenarioSpec] = {}


def _populate_registry():
    global SCENARIO_REGISTRY
    SCENARIO_REGISTRY = {}
    try:
        from . import definitions
        for attr_name in dir(definitions):
            attr = getattr(definitions, attr_name)
            if isinstance(attr, ScenarioSpec):
                SCENARIO_REGISTRY[attr.name] = attr
            elif isinstance(attr, dict):
                for key, val in attr.items():
                    if isinstance(val, ScenarioSpec):
                        SCENARIO_REGISTRY[key] = val
    except ImportError:
        pass


_populate_registry()


def list_scenarios() -> list[str]:
    return sorted(SCENARIO_REGISTRY.keys())


def get_scenarios_for_event(event_type: str) -> list[ScenarioSpec]:
    return [
        spec for spec in SCENARIO_REGISTRY.values()
        if spec.event_type == event_type
    ]


def get_scenario(name: str) -> ScenarioSpec | None:
    return SCENARIO_REGISTRY.get(name)
