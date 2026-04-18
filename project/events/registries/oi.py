from __future__ import annotations

from project.events.detectors.positioning_base import (
    OIFlushDetectorV2,
    OISpikeNegativeDetectorV2,
    OISpikePositiveDetectorV2,
)
from project.events.detectors.registry import register_detector


OI_DETECTORS = {
    "OI_SPIKE_POSITIVE": OISpikePositiveDetectorV2,
    "OI_SPIKE_NEGATIVE": OISpikeNegativeDetectorV2,
    "OI_FLUSH": OIFlushDetectorV2,
}


def ensure_oi_detectors_registered() -> None:
    for event_type, detector_cls in OI_DETECTORS.items():
        register_detector(event_type, detector_cls)


__all__ = [
    "OI_DETECTORS",
    "ensure_oi_detectors_registered",
]
