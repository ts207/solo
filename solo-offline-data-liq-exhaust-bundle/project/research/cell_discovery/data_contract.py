from __future__ import annotations

from typing import Any

from project.research.cell_discovery.models import DiscoveryRegistry


def required_event_types(registry: DiscoveryRegistry) -> list[str]:
    return sorted({atom.event_type for atom in registry.event_atoms})


def required_condition_keys(registry: DiscoveryRegistry) -> list[str]:
    keys: set[str] = set()
    for cell in registry.context_cells:
        if cell.dimension:
            keys.add(cell.dimension)
        if cell.required_feature_key:
            keys.add(cell.required_feature_key)
    return sorted(keys)


def contract_summary(registry: DiscoveryRegistry) -> dict[str, Any]:
    return {
        "schema_version": "edge_cell_data_contract_v1",
        "event_types": required_event_types(registry),
        "condition_keys": required_condition_keys(registry),
        "event_atom_count": len(registry.event_atoms),
        "context_cell_count": len(registry.context_cells),
    }
