from __future__ import annotations

from project.events.detectors.positioning_base import (
    FundingExtremeOnsetDetectorV2,
    FundingFlipDetectorV2,
    FundingFlipToNegativeDetectorV2,
    FundingFlipToPositiveDetectorV2,
    FundingNegExtremeOnsetDetectorV2,
    FundingNegNormalizationDetectorV2,
    FundingNegPersistenceDetectorV2,
    FundingNormalizationDetectorV2,
    FundingPersistenceDetectorV2,
    FundingPosExtremeOnsetDetectorV2,
    FundingPosNormalizationDetectorV2,
    FundingPosPersistenceDetectorV2,
)
from project.events.detectors.registry import register_detector

FUNDING_DETECTORS = {
    "FUNDING_EXTREME_ONSET": FundingExtremeOnsetDetectorV2,
    "FUNDING_PERSISTENCE_TRIGGER": FundingPersistenceDetectorV2,
    "FUNDING_NORMALIZATION_TRIGGER": FundingNormalizationDetectorV2,
    "FUNDING_FLIP": FundingFlipDetectorV2,
    "FUNDING_POS_EXTREME_ONSET": FundingPosExtremeOnsetDetectorV2,
    "FUNDING_NEG_EXTREME_ONSET": FundingNegExtremeOnsetDetectorV2,
    "FUNDING_POS_PERSISTENCE": FundingPosPersistenceDetectorV2,
    "FUNDING_NEG_PERSISTENCE": FundingNegPersistenceDetectorV2,
    "FUNDING_POS_NORMALIZATION": FundingPosNormalizationDetectorV2,
    "FUNDING_NEG_NORMALIZATION": FundingNegNormalizationDetectorV2,
    "FUNDING_FLIP_TO_POSITIVE": FundingFlipToPositiveDetectorV2,
    "FUNDING_FLIP_TO_NEGATIVE": FundingFlipToNegativeDetectorV2,
}


def ensure_funding_detectors_registered() -> None:
    for event_type, detector_cls in FUNDING_DETECTORS.items():
        register_detector(event_type, detector_cls)


__all__ = [
    "FUNDING_DETECTORS",
    "ensure_funding_detectors_registered",
]
