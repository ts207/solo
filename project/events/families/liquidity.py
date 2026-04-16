from __future__ import annotations

from project.events.adapters.liquidity_analysis import (
    analyze_liquidity_family,
    detect_liquidity_family,
)
from project.events.detectors.liquidity import (
    AbsorptionDetector,
    BaseLiquidityStressDetector,
    DepthCollapseDetector,
    DirectLiquidityStressDetector,
    LIQUIDITY_FAMILY_DETECTORS,
    LiquidityGapDetector,
    LiquidityStressDetector,
    LiquidityVacuumDetector,
    OrderflowImbalanceDetector,
    ProxyLiquidityStressDetector,
    SpreadBlowoutDetector,
)
from project.events.registries.liquidity import ensure_liquidity_detectors_registered


ensure_liquidity_detectors_registered()

_LIQUIDITY_DETECTORS = LIQUIDITY_FAMILY_DETECTORS

__all__ = [
    "AbsorptionDetector",
    "BaseLiquidityStressDetector",
    "DepthCollapseDetector",
    "DirectLiquidityStressDetector",
    "LiquidityGapDetector",
    "LiquidityStressDetector",
    "LiquidityVacuumDetector",
    "OrderflowImbalanceDetector",
    "ProxyLiquidityStressDetector",
    "SpreadBlowoutDetector",
    "detect_liquidity_family",
    "analyze_liquidity_family",
]
