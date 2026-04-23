from __future__ import annotations

from project.events.detectors.registry import register_detector

TEMPORAL_EVENT_TYPES = (
    "SESSION_OPEN_EVENT",
    "SESSION_CLOSE_EVENT",
    "FUNDING_TIMESTAMP_EVENT",
    "SCHEDULED_NEWS_WINDOW_EVENT",
    "SPREAD_REGIME_WIDENING_EVENT",
    "SLIPPAGE_SPIKE_EVENT",
    "FEE_REGIME_CHANGE_EVENT",
    "COPULA_PAIRS_TRADING",
)


def get_temporal_detectors() -> dict[str, type]:
    from project.events.families.temporal import (
        CopulaPairsTradingDetector,
        FeeRegimeChangeDetector,
        FundingTimestampDetector,
        ScheduledNewsDetector,
        SessionCloseDetector,
        SessionOpenDetector,
        SlippageSpikeDetector,
        SpreadRegimeWideningDetector,
    )

    return {
        "SESSION_OPEN_EVENT": SessionOpenDetector,
        "SESSION_CLOSE_EVENT": SessionCloseDetector,
        "FUNDING_TIMESTAMP_EVENT": FundingTimestampDetector,
        "SCHEDULED_NEWS_WINDOW_EVENT": ScheduledNewsDetector,
        "SPREAD_REGIME_WIDENING_EVENT": SpreadRegimeWideningDetector,
        "SLIPPAGE_SPIKE_EVENT": SlippageSpikeDetector,
        "FEE_REGIME_CHANGE_EVENT": FeeRegimeChangeDetector,
        "COPULA_PAIRS_TRADING": CopulaPairsTradingDetector,
    }


def ensure_temporal_detectors_registered() -> None:
    for event_type, detector_cls in get_temporal_detectors().items():
        register_detector(event_type, detector_cls)


__all__ = [
    "TEMPORAL_EVENT_TYPES",
    "ensure_temporal_detectors_registered",
    "get_temporal_detectors",
]