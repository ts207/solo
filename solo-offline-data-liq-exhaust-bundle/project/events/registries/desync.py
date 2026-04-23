from __future__ import annotations

from project.events.detectors.desync_base import (
    BetaSpikeDetectorV2,
    CorrelationBreakdownDetectorV2,
    CrossAssetDesyncDetectorV2,
    CrossVenueDesyncDetectorV2,
    IndexComponentDivergenceDetectorV2,
    LeadLagBreakDetectorV2,
)
from project.events.detectors.registry import register_detector

DESYNC_DETECTORS = {
    "BETA_SPIKE_EVENT": BetaSpikeDetectorV2,
    "CORRELATION_BREAKDOWN_EVENT": CorrelationBreakdownDetectorV2,
    "CROSS_ASSET_DESYNC_EVENT": CrossAssetDesyncDetectorV2,
    "CROSS_VENUE_DESYNC": CrossVenueDesyncDetectorV2,
    "INDEX_COMPONENT_DIVERGENCE": IndexComponentDivergenceDetectorV2,
    "LEAD_LAG_BREAK": LeadLagBreakDetectorV2,
}


def _get_legacy_detectors() -> dict[str, type]:
    from project.events.families.desync import (
        CrossAssetDesyncDetector,
        IndexComponentDivergenceDetector,
        LeadLagBreakDetector,
    )

    return {
        "INDEX_COMPONENT_DIVERGENCE": IndexComponentDivergenceDetector,
        "LEAD_LAG_BREAK": LeadLagBreakDetector,
        "CROSS_ASSET_DESYNC_EVENT": CrossAssetDesyncDetector,
    }


DESYNC_LEGACY_DETECTORS: dict[str, type] = {}


def get_desync_detectors() -> dict[str, type]:
    legacy = _get_legacy_detectors()
    merged = dict(DESYNC_DETECTORS)
    merged.update(legacy)
    return merged


def ensure_desync_detectors_registered() -> None:
    for event_type, detector_cls in DESYNC_DETECTORS.items():
        register_detector(event_type, detector_cls)


# Backward-compat: lazily populated on first access
def _lazy_legacy() -> dict[str, type]:
    global DESYNC_LEGACY_DETECTORS
    if not DESYNC_LEGACY_DETECTORS:
        DESYNC_LEGACY_DETECTORS = _get_legacy_detectors()
    return DESYNC_LEGACY_DETECTORS


__all__ = [
    "DESYNC_DETECTORS",
    "ensure_desync_detectors_registered",
    "get_desync_detectors",
]
