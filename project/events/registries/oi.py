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


def _get_oi_family_detectors() -> dict[str, type]:
    from project.events.families.oi import DeleveragingWaveDetector

    return {"DELEVERAGING_WAVE": DeleveragingWaveDetector}


def get_oi_detectors() -> dict[str, type]:
    detectors = dict(OI_DETECTORS)
    detectors.update(_get_oi_family_detectors())
    return detectors


def ensure_oi_detectors_registered() -> None:
    for event_type, detector_cls in get_oi_detectors().items():
        register_detector(event_type, detector_cls)


__all__ = [
    "OI_DETECTORS",
    "ensure_oi_detectors_registered",
    "get_oi_detectors",
]
