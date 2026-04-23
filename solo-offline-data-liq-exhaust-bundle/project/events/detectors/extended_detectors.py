from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.exhaustion import PostDeleveragingReboundDetector
from project.events.detectors.funding import BaseFundingDetector
from project.events.detectors.liquidity import DirectLiquidityStressDetector
from project.events.detectors.registry import register_detector
from project.events.detectors.sequence import EventSequenceDetector
from project.events.detectors.volatility import BreakoutTriggerDetector
from project.events.families.basis import BasisDislocationDetector, CrossVenueDesyncDetector
from project.events.families.oi import BaseOIShockDetector


def _recent_true(mask: pd.Series, window: int) -> pd.Series:
    return (
        mask.fillna(False)
        .astype(bool)
        .rolling(window=max(int(window), 1), min_periods=1)
        .max()
        .shift(1)
        .fillna(0)
        .astype(bool)
    )


class LiquidationExhaustionReversalDetector(PostDeleveragingReboundDetector):
    event_type = "LIQUIDATION_EXHAUSTION_REVERSAL"


register_detector("LIQUIDATION_EXHAUSTION_REVERSAL", LiquidationExhaustionReversalDetector)

# Register Sequence Event Detectors
register_detector("SEQ_FND_EXTREME_THEN_BREAKOUT", EventSequenceDetector)
register_detector("SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY", EventSequenceDetector)
register_detector("SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE", EventSequenceDetector)
register_detector("SEQ_VOL_COMP_THEN_BREAKOUT", EventSequenceDetector)
