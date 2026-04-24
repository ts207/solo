from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

class RegimeName(str, Enum):
    HIGH_VOL = "HIGH_VOL"
    LOW_VOL = "LOW_VOL"
    BULL_TREND = "BULL_TREND"
    BEAR_TREND = "BEAR_TREND"
    CHOP = "CHOP"
    UNKNOWN = "UNKNOWN"

class ClassificationMode(str, Enum):
    RESEARCH_EXACT = "research_exact"
    RUNTIME_APPROX = "runtime_approx"

@dataclass(frozen=True)
class RegimeClassification:
    regime: RegimeName
    mode: ClassificationMode
    confidence: float
    metadata: Dict[str, Any] = field(default_factory=dict)

def classify_regime(
    *,
    # Research Exact Inputs
    rv_pct: Optional[float] = None,
    ms_trend_state: Optional[float] = None,
    # Runtime Approx Inputs
    move_bps: Optional[float] = None,
    # Context
    fallback_regime: RegimeName = RegimeName.LOW_VOL
) -> RegimeClassification:
    """
    Unified API for regime classification.
    Prioritizes exact research semantics if available, falls back to runtime approximations.
    """

    # 1. Try Research Exact Path
    if rv_pct is not None and ms_trend_state is not None:
        regime = RegimeName.UNKNOWN
        if rv_pct >= 80.0:
            regime = RegimeName.HIGH_VOL
        elif rv_pct < 20.0:
            regime = RegimeName.LOW_VOL
        elif ms_trend_state == 1.0:
            regime = RegimeName.BULL_TREND
        elif ms_trend_state == 2.0:
            regime = RegimeName.BEAR_TREND
        elif ms_trend_state == 0.0:
            regime = RegimeName.CHOP

        if regime != RegimeName.UNKNOWN:
            return RegimeClassification(
                regime=regime,
                mode=ClassificationMode.RESEARCH_EXACT,
                confidence=1.0,
                metadata={
                    "move_bps": move_bps,
                    "rv_pct": rv_pct,
                    "ms_trend_state": ms_trend_state,
                }
            )

    # 2. Try Runtime Approx Path
    if move_bps is not None:
        abs_move = abs(move_bps)
        regime = RegimeName.UNKNOWN
        if abs_move >= 80.0:
            regime = RegimeName.HIGH_VOL
        elif abs_move < 20.0:
            regime = RegimeName.LOW_VOL
        else:
            regime = RegimeName.BULL_TREND if move_bps >= 0.0 else RegimeName.BEAR_TREND

        missing_inputs = []
        if rv_pct is None: missing_inputs.append("rv_pct")
        if ms_trend_state is None: missing_inputs.append("ms_trend_state")

        return RegimeClassification(
            regime=regime,
            mode=ClassificationMode.RUNTIME_APPROX,
            confidence=0.6, # heuristic confidence for approximation
            metadata={
                "move_bps": move_bps,
                "missing_inputs": missing_inputs,
                "approximation_type": "bps_threshold"
            }
        )

    # 3. Fallback
    return RegimeClassification(
        regime=fallback_regime,
        mode=ClassificationMode.RUNTIME_APPROX,
        confidence=0.0,
        metadata={"reason": "missing_all_inputs"}
    )
