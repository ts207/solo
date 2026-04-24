from __future__ import annotations

from pathlib import Path

from project.events.detectors.registry import (
    list_registered_event_types,
    load_all_detectors,
)
from project.spec_registry import load_yaml_path


def test_runtime_detector_registry_matches_detector_ownership_registry() -> None:
    load_all_detectors()
    registry_path = Path("project/configs/registries/detectors.yaml")
    ownership = load_yaml_path(registry_path).get("detector_ownership", {})

    registered = set(list_registered_event_types())
    owned = {str(event_type).strip().upper() for event_type in ownership}
