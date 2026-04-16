from __future__ import annotations

from project.events.adapters.funding_analysis import (
    analyze_funding_family,
    detect_funding_family,
)
from project.events.detectors.funding import (
    FUNDING_EVENT_TYPES,
    FundingDetector,
    FundingFlipDetector,
)
from project.events.registries.funding import (
    FUNDING_DETECTORS,
    ensure_funding_detectors_registered,
)


ensure_funding_detectors_registered()

_DETECTORS = FUNDING_DETECTORS

__all__ = [
    "FUNDING_EVENT_TYPES",
    "FundingDetector",
    "FundingFlipDetector",
    "detect_funding_family",
    "analyze_funding_family",
]
