from __future__ import annotations

from project.events.detectors.desync_base import CrossVenueDesyncDetectorV2
from project.events.detectors.dislocation_base import (
    BasisDislocationDetectorV2,
    FndDislocDetectorV2,
    SpotPerpBasisShockDetectorV2,
)
from project.events.detectors.registry import register_detector

BASIS_DETECTORS = {
    "BASIS_DISLOC": BasisDislocationDetectorV2,
    "CROSS_VENUE_DESYNC": CrossVenueDesyncDetectorV2,
    "FND_DISLOC": FndDislocDetectorV2,
    "SPOT_PERP_BASIS_SHOCK": SpotPerpBasisShockDetectorV2,
}


def ensure_basis_detectors_registered() -> None:
    for event_type, detector_cls in BASIS_DETECTORS.items():
        register_detector(event_type, detector_cls)


__all__ = [
    "BASIS_DETECTORS",
    "ensure_basis_detectors_registered",
]
