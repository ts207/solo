from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def _read_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"YAML must be a mapping: {path}")
    return payload


def _event_list(values: Any) -> list[str]:
    return sorted({str(value).strip().upper() for value in list(values or []) if str(value).strip()})


def _surface_id(spec_root: Path, path: Path) -> str:
    if path.parent == spec_root:
        return "default"
    return str(path.parent.relative_to(spec_root))


def build_cell_coverage_audit(
    *,
    spec_root: str | Path = "spec/discovery",
    search_spec: str | Path = "spec/search_space.yaml",
    event_registry: str | Path = "spec/events/event_registry_unified.yaml",
) -> dict[str, Any]:
    root = Path(spec_root)
    registry_path = Path(event_registry)
    search_path = Path(search_spec)

    registry_doc = _read_yaml(registry_path)
    search_doc = _read_yaml(search_path)
    registry_events_raw = registry_doc.get("events", {})
    if not isinstance(registry_events_raw, dict):
        raise ValueError(f"event registry must contain mapping field 'events': {registry_path}")

    registry_events = _event_list(registry_events_raw.keys())
    search_events = _event_list((search_doc.get("triggers", {}) or {}).get("events", []))
    surface_events: dict[str, list[str]] = {}
    event_to_surfaces: dict[str, list[str]] = {}

    for atoms_path in sorted(root.rglob("event_atoms.yaml")):
        doc = _read_yaml(atoms_path)
        events = _event_list(item.get("event_type") for item in doc.get("event_atoms", []) or [])
        surface = _surface_id(root, atoms_path)
        surface_events[surface] = events
        for event in events:
            event_to_surfaces.setdefault(event, []).append(surface)

    cell_events = sorted(event_to_surfaces)
    registry_by_regime: dict[str, list[str]] = {}
    missing_cell_by_regime: dict[str, list[str]] = {}
    for event, meta in registry_events_raw.items():
        if not isinstance(meta, dict):
            continue
        event_id = str(event).strip().upper()
        regime = str(meta.get("canonical_regime", "UNKNOWN")).strip().upper() or "UNKNOWN"
        registry_by_regime.setdefault(regime, []).append(event_id)
        if event_id not in event_to_surfaces:
            missing_cell_by_regime.setdefault(regime, []).append(event_id)

    return {
        "exit_code": 0,
        "status": "ok",
        "spec_root": str(root),
        "search_spec": str(search_path),
        "event_registry": str(registry_path),
        "registry_event_count": len(registry_events),
        "default_search_event_count": len(search_events),
        "cell_surface_count": len(surface_events),
        "cell_event_count": len(cell_events),
        "cell_coverage_fraction_of_registry": round(
            len(cell_events) / len(registry_events), 4
        )
        if registry_events
        else 0.0,
        "cell_coverage_fraction_of_default_search": round(
            len(set(cell_events) & set(search_events)) / len(search_events), 4
        )
        if search_events
        else 0.0,
        "cell_events": cell_events,
        "cell_surfaces": {
            surface: {
                "event_count": len(events),
                "events": events,
            }
            for surface, events in sorted(surface_events.items())
        },
        "event_to_cell_surfaces": {
            event: sorted(surfaces) for event, surfaces in sorted(event_to_surfaces.items())
        },
        "missing_from_cell_by_regime": {
            regime: sorted(events) for regime, events in sorted(missing_cell_by_regime.items())
        },
        "registry_by_regime": {
            regime: sorted(events) for regime, events in sorted(registry_by_regime.items())
        },
        "default_search_events_missing_from_cell": sorted(set(search_events) - set(cell_events)),
        "cell_events_not_in_default_search": sorted(set(cell_events) - set(search_events)),
    }
