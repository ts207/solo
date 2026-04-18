from __future__ import annotations

from typing import TYPE_CHECKING

# This module provides a narrowed interface for external domain packages (like strategy_dsl)
# to access event metadata without depending on the full registry implementation.
# All lookups delegate to the compiled domain registry — spec/domain/domain_graph.yaml —
# rather than rebuilding truth from raw YAML.

if TYPE_CHECKING:
    from project.domain.models import EventDefinition


def get_event_spec(event_type: str) -> "EventDefinition | None":
    from project.domain.compiled_registry import get_domain_registry
    return get_domain_registry().get_event(str(event_type).strip().upper())


def get_event_type_from_signal(signal_column: str) -> str | None:
    from project.domain.compiled_registry import get_domain_registry
    col = str(signal_column).strip()
    for event_type, spec in get_domain_registry().event_definitions.items():
        if spec.signal_column == col:
            return event_type
    return None


def is_registry_backed_signal(signal_column: str) -> bool:
    return get_event_type_from_signal(signal_column) is not None
