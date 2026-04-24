from __future__ import annotations

from project.events.detectors.registry import register_detector

CANONICAL_PROXY_EVENT_TYPES = (
    "PRICE_VOL_IMBALANCE_PROXY",
    "WICK_REVERSAL_PROXY",
    "ABSORPTION_PROXY",
    "DEPTH_STRESS_PROXY",
    "ORDERFLOW_IMBALANCE_SHOCK",
    "SWEEP_STOPRUN",
    "DEPTH_COLLAPSE",
)


def get_canonical_proxy_detectors() -> dict[str, type]:
    from project.events.families.canonical_proxy import (
        AbsorptionProxyDetector,
        DepthCollapseDetector,
        DepthStressProxyDetector,
        OrderflowImbalanceShockDetector,
        PriceVolImbalanceProxyDetector,
        SweepStopRunDetector,
        WickReversalProxyDetector,
    )

    return {
        "PRICE_VOL_IMBALANCE_PROXY": PriceVolImbalanceProxyDetector,
        "WICK_REVERSAL_PROXY": WickReversalProxyDetector,
        "ABSORPTION_PROXY": AbsorptionProxyDetector,
        "DEPTH_STRESS_PROXY": DepthStressProxyDetector,
        "ORDERFLOW_IMBALANCE_SHOCK": OrderflowImbalanceShockDetector,
        "SWEEP_STOPRUN": SweepStopRunDetector,
        "DEPTH_COLLAPSE": DepthCollapseDetector,
    }


def ensure_canonical_proxy_detectors_registered() -> None:
    for event_type, detector_cls in get_canonical_proxy_detectors().items():
        register_detector(event_type, detector_cls)


__all__ = [
    "CANONICAL_PROXY_EVENT_TYPES",
    "ensure_canonical_proxy_detectors_registered",
    "get_canonical_proxy_detectors",
]
