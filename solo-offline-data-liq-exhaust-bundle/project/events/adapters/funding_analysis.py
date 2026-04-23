from __future__ import annotations

from typing import Any

import pandas as pd

from project.events.detectors.funding import FundingDetector
from project.events.registries.funding import FUNDING_DETECTORS, ensure_funding_detectors_registered
from project.research.analyzers import run_analyzer_suite


def detect_funding_family(
    df: pd.DataFrame,
    symbol: str,
    event_type: str = "FUNDING_EXTREME_ONSET",
    **params: Any,
) -> pd.DataFrame:
    ensure_funding_detectors_registered()
    detector_cls = FUNDING_DETECTORS.get(event_type, FundingDetector)
    return detector_cls().detect(df, symbol=symbol, **params)


def analyze_funding_family(
    df: pd.DataFrame,
    symbol: str,
    event_type: str = "FUNDING_EXTREME_ONSET",
    **params: Any,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    events = detect_funding_family(df, symbol, event_type=event_type, **params)
    market = None
    if not df.empty and "close" in df.columns:
        market = df[["timestamp", "close"]].copy()
    analyzer_results = run_analyzer_suite(events, market=market) if not events.empty else {}
    return events, analyzer_results


__all__ = [
    "analyze_funding_family",
    "detect_funding_family",
]
