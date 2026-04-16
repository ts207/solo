from __future__ import annotations

from project.events.detectors.funding import (
    FundingDetector,
    FundingExtremeOnsetDetector,
    FundingFlipDetector,
    FundingNormalizationDetector,
    FundingPersistenceDetector,
)
from project.events.detectors.registry import register_detector


FUNDING_DETECTORS = {
    "FUNDING_EXTREME_ONSET": FundingExtremeOnsetDetector,
    "FUNDING_PERSISTENCE_TRIGGER": FundingPersistenceDetector,
    "FUNDING_NORMALIZATION_TRIGGER": FundingNormalizationDetector,
    "FUNDING_FLIP": FundingFlipDetector,
}


def ensure_funding_detectors_registered() -> None:
    for event_type, detector_cls in FUNDING_DETECTORS.items():
        register_detector(event_type, detector_cls)


__all__ = [
    "FUNDING_DETECTORS",
    "ensure_funding_detectors_registered",
]
