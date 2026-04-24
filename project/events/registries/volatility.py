from __future__ import annotations

from project.events.detectors.registry import register_detector
from project.events.detectors.volatility import (
    BreakoutTriggerDetector,
    RangeCompressionDetector,
    VolClusterShiftDetector,
)
from project.events.detectors.volatility_base import (
    VolRelaxationStartDetectorV2,
    VolShockDetectorV2,
    VolSpikeDetectorV2,
)

VOLATILITY_DETECTORS = {
    "VOL_SPIKE": VolSpikeDetectorV2,
    "VOL_RELAXATION_START": VolRelaxationStartDetectorV2,
    "VOL_CLUSTER_SHIFT": VolClusterShiftDetector,
    "RANGE_COMPRESSION_END": RangeCompressionDetector,
    "BREAKOUT_TRIGGER": BreakoutTriggerDetector,
    "VOL_SHOCK": VolShockDetectorV2,
}


def ensure_volatility_detectors_registered() -> None:
    for event_type, detector_cls in VOLATILITY_DETECTORS.items():
        register_detector(event_type, detector_cls)


__all__ = [
    "VOLATILITY_DETECTORS",
    "ensure_volatility_detectors_registered",
]
