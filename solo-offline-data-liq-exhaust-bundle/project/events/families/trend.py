from __future__ import annotations

from project.events.adapters.trend_analysis import (
    analyze_trend_family,
    detect_trend_family,
)
from project.events.detectors.trend import (
    FalseBreakoutDetector,
    PullbackPivotDetector,
    RangeBreakoutDetector,
    SREventDetector,
    TrendAccelerationDetector,
    TrendBase,
    TrendDecelerationDetector,
)
from project.events.registries.trend import TREND_DETECTORS, ensure_trend_detectors_registered


ensure_trend_detectors_registered()

_DETECTORS = TREND_DETECTORS

__all__ = [
    "FalseBreakoutDetector",
    "PullbackPivotDetector",
    "RangeBreakoutDetector",
    "SREventDetector",
    "TrendAccelerationDetector",
    "TrendBase",
    "TrendDecelerationDetector",
    "detect_trend_family",
    "analyze_trend_family",
]
