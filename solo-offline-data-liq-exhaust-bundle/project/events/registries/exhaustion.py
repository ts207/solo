from __future__ import annotations

from project.events.detectors.exhaustion import EXHAUSTION_DETECTORS
from project.events.detectors.registry import register_detector


def ensure_exhaustion_detectors_registered() -> None:
    for event_type, detector_cls in EXHAUSTION_DETECTORS.items():
        register_detector(event_type, detector_cls)


__all__ = [
    "EXHAUSTION_DETECTORS",
    "ensure_exhaustion_detectors_registered",
]
