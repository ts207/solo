from __future__ import annotations

from project.events.adapters.liquidity_analysis import (
    analyze_liquidity_family,
    detect_liquidity_family,
)
from project.events.detectors.liquidity import (
    LIQUIDITY_FAMILY_DETECTORS,
    AbsorptionDetector,
    BaseLiquidityStressDetector,
    LiquidityStressDetector,
    OrderflowImbalanceDetector,
    SpreadBlowoutDetector,
)
from project.events.detectors.liquidity_base import (
    DepthCollapseDetectorV2,
    DirectLiquidityStressDetectorV2,
    LiquidityGapDetectorV2,
    LiquidityVacuumDetectorV2,
    ProxyLiquidityStressDetectorV2,
)
from project.events.registries.liquidity import ensure_liquidity_detectors_registered

ensure_liquidity_detectors_registered()

_LIQUIDITY_DETECTORS = LIQUIDITY_FAMILY_DETECTORS

__all__ = [
    "AbsorptionDetector",
    "BaseLiquidityStressDetector",
    "DepthCollapseDetectorV2",
    "DirectLiquidityStressDetectorV2",
    "LiquidityGapDetectorV2",
    "LiquidityStressDetector",
    "LiquidityVacuumDetectorV2",
    "OrderflowImbalanceDetector",
    "ProxyLiquidityStressDetectorV2",
    "SpreadBlowoutDetector",
    "analyze_liquidity_family",
    "detect_liquidity_family",
]
