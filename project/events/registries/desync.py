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


def ensure_desync_detectors_registered() -> None:
    for event_type, detector_cls in DESYNC_DETECTORS.items():
        register_detector(event_type, detector_cls)


__all__ = [
    "DESYNC_DETECTORS",
    "ensure_desync_detectors_registered",
]
