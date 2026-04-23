from __future__ import annotations

from project.events.detectors.liquidity import (
    LIQUIDITY_FAMILY_DETECTORS,
    AbsorptionDetector,
    OrderflowImbalanceDetector,
    SpreadBlowoutDetector,
)
from project.events.detectors.liquidity_base import (
    DepthCollapseDetectorV2,
    DirectLiquidityStressDetectorV2,
    LiquidityGapDetectorV2,
    LiquidityShockDetectorV2,
    LiquidityVacuumDetectorV2,
    ProxyLiquidityStressDetectorV2,
)
from project.events.detectors.registry import register_detector


LIQUIDITY_REGISTERED_DETECTORS = {
    "LIQUIDITY_SHOCK": LiquidityShockDetectorV2,
    "LIQUIDITY_STRESS_DIRECT": DirectLiquidityStressDetectorV2,
    "LIQUIDITY_STRESS_PROXY": ProxyLiquidityStressDetectorV2,
    "LIQUIDITY_VACUUM": LiquidityVacuumDetectorV2,
    "LIQUIDITY_GAP_PRINT": LiquidityGapDetectorV2,
    "SPREAD_BLOWOUT": SpreadBlowoutDetector,
    "DEPTH_COLLAPSE": DepthCollapseDetectorV2,
    "ABSORPTION_EVENT": AbsorptionDetector,
    "ORDERFLOW_IMBALANCE_SHOCK": OrderflowImbalanceDetector,
}


def ensure_liquidity_detectors_registered() -> None:
    for event_type, detector_cls in LIQUIDITY_REGISTERED_DETECTORS.items():
        register_detector(event_type, detector_cls)


__all__ = [
    "LIQUIDITY_FAMILY_DETECTORS",
    "LIQUIDITY_REGISTERED_DETECTORS",
    "ensure_liquidity_detectors_registered",
]
