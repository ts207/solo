from __future__ import annotations

from project.events.adapters.volatility_analysis import (
    analyze_volatility_family,
    detect_volatility_family,
)
from project.events.detectors.volatility import (
    BreakoutTriggerDetector,
    RangeCompressionDetector,
    VolatilityBase,
    VolClusterShiftDetector,
    VolSpikeDetector,
)
from project.events.detectors.volatility_base import (
    VolRelaxationStartDetectorV2,
    VolShockDetectorV2,
    VolSpikeDetectorV2,
)
from project.events.registries.volatility import (
    VOLATILITY_DETECTORS,
    ensure_volatility_detectors_registered,
)

ensure_volatility_detectors_registered()

_DETECTORS = VOLATILITY_DETECTORS

__all__ = [
    "BreakoutTriggerDetector",
    "RangeCompressionDetector",
    "VolClusterShiftDetector",
    "VolSpikeDetector",
    "VolRelaxationStartDetectorV2",
    "VolShockDetectorV2",
    "VolSpikeDetectorV2",
    "VolatilityBase",
    "detect_volatility_family",
    "analyze_volatility_family",
]
