from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Type

from project.events.detectors.base import BaseEventDetector
from project.events.detectors.catalog import load_detector_family_modules

_DETECTORS: Dict[str, Type[BaseEventDetector]] = {}


def register_detector(event_type: str, detector_cls: Type[BaseEventDetector]) -> None:
    _DETECTORS[event_type.upper()] = detector_cls


def get_detector(event_type: str) -> Optional[BaseEventDetector]:
    cls = _DETECTORS.get(event_type.upper())
    return cls() if cls else None


def list_registered_event_types() -> List[str]:
    return sorted(_DETECTORS.keys())


def load_all_detectors() -> None:
    """Import detector family modules from the explicit catalog."""
    load_detector_family_modules()


# --- Auto-registration helpers ---
def register_family_detectors(detectors: Dict[str, Type[BaseEventDetector]]) -> None:
    for et, cls in detectors.items():
        register_detector(et, cls)
