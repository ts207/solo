from __future__ import annotations

from project.events.detectors.liquidation_base import (
    LiquidationCascadeDetectorV2,
    LiquidationCascadeProxyDetectorV2,
)
from project.events.detectors.registry import register_detector


LIQUIDATION_DETECTORS = {
    "LIQUIDATION_CASCADE": LiquidationCascadeDetectorV2,
    "LIQUIDATION_CASCADE_PROXY": LiquidationCascadeProxyDetectorV2,
}


def ensure_liquidation_detectors_registered() -> None:
    for event_type, detector_cls in LIQUIDATION_DETECTORS.items():
        register_detector(event_type, detector_cls)


__all__ = [
    "LIQUIDATION_DETECTORS",
    "ensure_liquidation_detectors_registered",
]
