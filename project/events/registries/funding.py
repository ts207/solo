from __future__ import annotations

from project.events.detectors.positioning_base import (
    FundingExtremeOnsetDetectorV2,
    FundingFlipDetectorV2,
    FundingNormalizationDetectorV2,
    FundingPersistenceDetectorV2,
)
from project.events.detectors.registry import register_detector

FUNDING_DETECTORS = {
    "FUNDING_EXTREME_ONSET": FundingExtremeOnsetDetectorV2,
    "FUNDING_PERSISTENCE_TRIGGER": FundingPersistenceDetectorV2,
    "FUNDING_NORMALIZATION_TRIGGER": FundingNormalizationDetectorV2,
    "FUNDING_FLIP": FundingFlipDetectorV2,
}


def ensure_funding_detectors_registered() -> None:
    for event_type, detector_cls in FUNDING_DETECTORS.items():
        register_detector(event_type, detector_cls)


__all__ = [
    "FUNDING_DETECTORS",
    "ensure_funding_detectors_registered",
]
