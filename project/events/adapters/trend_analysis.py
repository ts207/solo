from __future__ import annotations

from typing import Any

import pandas as pd

from project.events.registries.trend import TREND_DETECTORS, ensure_trend_detectors_registered
from project.research.analyzers import run_analyzer_suite


def detect_trend_family(
    df: pd.DataFrame, symbol: str, event_type: str, **params: Any
) -> pd.DataFrame:
    ensure_trend_detectors_registered()
    detector_cls = TREND_DETECTORS.get(event_type)
    if detector_cls is None:
        raise ValueError(f"Unknown trend event type: {event_type}")
    return detector_cls().detect(df, symbol=symbol, **params)


def analyze_trend_family(
    df: pd.DataFrame, symbol: str, event_type: str, **params: Any
) -> tuple[pd.DataFrame, dict[str, Any]]:
    events = detect_trend_family(df, symbol, event_type, **params)
    market = df[["timestamp", "close"]].copy() if not df.empty and "close" in df.columns else None
    results = run_analyzer_suite(events, market=market) if not events.empty else {}
    return events, results


__all__ = [
    "analyze_trend_family",
    "detect_trend_family",
]
