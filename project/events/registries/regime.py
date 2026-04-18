from __future__ import annotations

from project.events.detectors.desync_base import (
    BetaSpikeDetectorV2,
    CorrelationBreakdownDetectorV2,
)
from project.events.detectors.registry import register_detector


def _get_regime_family_detectors() -> dict[str, type]:
    from project.events.families.regime import (
        ChopToTrendDetector,
        TrendToChopDetector,
        VolRegimeShiftDetector,
    )

    return {
        "VOL_REGIME_SHIFT": VolRegimeShiftDetector,
        "VOL_REGIME_SHIFT_EVENT": VolRegimeShiftDetector,
        "TREND_TO_CHOP_SHIFT": TrendToChopDetector,
        "CHOP_TO_TREND_SHIFT": ChopToTrendDetector,
    }


REGIME_PROMOTION_DETECTORS = {
    "CORRELATION_BREAKDOWN_EVENT": CorrelationBreakdownDetectorV2,
    "BETA_SPIKE_EVENT": BetaSpikeDetectorV2,
}

REGIME_FAMILY_EVENT_TYPES = (
    "VOL_REGIME_SHIFT",
    "VOL_REGIME_SHIFT_EVENT",
    "TREND_TO_CHOP_SHIFT",
    "CHOP_TO_TREND_SHIFT",
)


def get_regime_detectors() -> dict[str, type]:
    detectors = _get_regime_family_detectors()
    detectors.update(REGIME_PROMOTION_DETECTORS)
    return detectors


def ensure_regime_detectors_registered() -> None:
    for event_type, detector_cls in _get_regime_family_detectors().items():
        register_detector(event_type, detector_cls)
    for event_type, detector_cls in REGIME_PROMOTION_DETECTORS.items():
        register_detector(event_type, detector_cls)


# Backward-compatible alias — the promotion-eligible subset previously exported
# as REGIME_DETECTORS contained only the v2 classes.
REGIME_DETECTORS = dict(REGIME_PROMOTION_DETECTORS)

__all__ = [
    "REGIME_DETECTORS",
    "REGIME_FAMILY_EVENT_TYPES",
    "REGIME_PROMOTION_DETECTORS",
    "ensure_regime_detectors_registered",
    "get_regime_detectors",
]