from __future__ import annotations

from typing import Dict, Sequence, Set
from project.events.event_specs import (
    EventRegistrySpec,
    EVENT_REGISTRY_SPECS,
    SIGNAL_TO_EVENT_TYPE,
    REGISTRY_BACKED_SIGNALS,
    REGISTRY_EVENT_COLUMNS,
    VALID_DIRECTIONS,
)

# This module provides a narrowed interface for external domain packages (like strategy_dsl)
# to access event metadata without depending on the full registry implementation.


def get_event_spec(event_type: str) -> EventRegistrySpec | None:
    return EVENT_REGISTRY_SPECS.get(str(event_type).upper())


def get_event_type_from_signal(signal_column: str) -> str | None:
    return SIGNAL_TO_EVENT_TYPE.get(str(signal_column).strip())


def is_registry_backed_signal(signal_column: str) -> bool:
    return str(signal_column).strip() in REGISTRY_BACKED_SIGNALS
