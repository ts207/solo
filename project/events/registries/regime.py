from __future__ import annotations

from project.events.detectors.desync_base import (
    BetaSpikeDetectorV2,
    CorrelationBreakdownDetectorV2,
)
from project.events.detectors.registry import register_detector


REGIME_DETECTORS = {
    "CORRELATION_BREAKDOWN_EVENT": CorrelationBreakdownDetectorV2,
    "BETA_SPIKE_EVENT": BetaSpikeDetectorV2,
}


def ensure_regime_detectors_registered() -> None:
    for event_type, detector_cls in REGIME_DETECTORS.items():
        register_detector(event_type, detector_cls)


__all__ = [
    "REGIME_DETECTORS",
    "ensure_regime_detectors_registered",
]
