from __future__ import annotations

from project.events.detectors.registry import register_detector
from project.events.detectors.trend import (
    FalseBreakoutDetector,
    PullbackPivotDetector,
    RangeBreakoutDetector,
    SREventDetector,
    TrendAccelerationDetector,
    TrendDecelerationDetector,
)

TREND_DETECTORS = {
    "RANGE_BREAKOUT": RangeBreakoutDetector,
    "FALSE_BREAKOUT": FalseBreakoutDetector,
    "TREND_ACCELERATION": TrendAccelerationDetector,
    "TREND_DECELERATION": TrendDecelerationDetector,
    "PULLBACK_PIVOT": PullbackPivotDetector,
    "SUPPORT_RESISTANCE_BREAK": SREventDetector,
}


def ensure_trend_detectors_registered() -> None:
    for event_type, detector_cls in TREND_DETECTORS.items():
        register_detector(event_type, detector_cls)


__all__ = [
    "TREND_DETECTORS",
    "ensure_trend_detectors_registered",
]
