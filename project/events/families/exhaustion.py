from __future__ import annotations

from project.events.adapters.exhaustion_analysis import (
    analyze_exhaustion_family,
    detect_exhaustion_family,
)
from project.events.detectors.exhaustion import (
    EXHAUSTION_DETECTORS,
    ClimaxVolumeDetector,
    FailedContinuationDetector,
    FlowExhaustionDetector,
    MomentumDivergenceDetector,
    PostDeleveragingReboundDetector,
    TrendExhaustionDetector,
)
from project.events.registries.exhaustion import ensure_exhaustion_detectors_registered

ensure_exhaustion_detectors_registered()

_DETECTORS = EXHAUSTION_DETECTORS

__all__ = [
    "ClimaxVolumeDetector",
    "FailedContinuationDetector",
    "FlowExhaustionDetector",
    "MomentumDivergenceDetector",
    "PostDeleveragingReboundDetector",
    "TrendExhaustionDetector",
    "detect_exhaustion_family",
    "analyze_exhaustion_family",
]
