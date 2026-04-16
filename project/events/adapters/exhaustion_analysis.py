from __future__ import annotations

from typing import Any

import pandas as pd

from project.events.detectors.exhaustion import FlowExhaustionDetector
from project.events.registries.exhaustion import (
    EXHAUSTION_DETECTORS,
    ensure_exhaustion_detectors_registered,
)
from project.research.analyzers import run_analyzer_suite


def detect_exhaustion_family(
    df: pd.DataFrame,
    symbol: str,
    event_type: str = "FLOW_EXHAUSTION_PROXY",
    **params: Any,
) -> pd.DataFrame:
    ensure_exhaustion_detectors_registered()
    detector_cls = EXHAUSTION_DETECTORS.get(event_type, FlowExhaustionDetector)
    return detector_cls().detect(df, symbol=symbol, **params)


def analyze_exhaustion_family(
    df: pd.DataFrame,
    symbol: str,
    event_type: str = "FLOW_EXHAUSTION_PROXY",
    **params: Any,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    events = detect_exhaustion_family(df, symbol, event_type=event_type, **params)
    market = df[["timestamp", "close"]].copy() if not df.empty and "close" in df.columns else None
    analyzer_results = run_analyzer_suite(events, market=market) if not events.empty else {}
    return events, analyzer_results


__all__ = [
    "analyze_exhaustion_family",
    "detect_exhaustion_family",
]
