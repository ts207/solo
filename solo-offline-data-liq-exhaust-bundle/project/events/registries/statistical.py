from __future__ import annotations

from project.events.detectors.registry import register_detector

STATISTICAL_EVENT_TYPES = (
    "ZSCORE_STRETCH",
    "BAND_BREAK",
    "OVERSHOOT_AFTER_SHOCK",
    "GAP_OVERSHOOT",
)


def get_statistical_detectors() -> dict[str, type]:
    from project.events.families.statistical import (
        BandBreakDetector,
        GapOvershootDetector,
        OvershootDetector,
        ZScoreStretchDetector,
    )

    return {
        "ZSCORE_STRETCH": ZScoreStretchDetector,
        "BAND_BREAK": BandBreakDetector,
        "OVERSHOOT_AFTER_SHOCK": OvershootDetector,
        "GAP_OVERSHOOT": GapOvershootDetector,
    }


def ensure_statistical_detectors_registered() -> None:
    for event_type, detector_cls in get_statistical_detectors().items():
        register_detector(event_type, detector_cls)


__all__ = [
    "STATISTICAL_EVENT_TYPES",
    "ensure_statistical_detectors_registered",
    "get_statistical_detectors",
]
