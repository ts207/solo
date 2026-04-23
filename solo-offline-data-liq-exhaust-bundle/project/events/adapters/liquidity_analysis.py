from __future__ import annotations

from typing import Any

import pandas as pd

from project.events.detectors.liquidity import LIQUIDITY_FAMILY_DETECTORS
from project.events.registries.liquidity import ensure_liquidity_detectors_registered
from project.research.analyzers import run_analyzer_suite


def detect_liquidity_family(
    df: pd.DataFrame,
    symbol: str,
    event_type: str = "LIQUIDITY_SHOCK",
    **params: Any,
) -> pd.DataFrame:
    ensure_liquidity_detectors_registered()
    detector_cls = LIQUIDITY_FAMILY_DETECTORS.get(event_type)
    if detector_cls is None:
        raise ValueError(f"Unknown liquidity event type: {event_type}")
    return detector_cls().detect(df, symbol=symbol, **params)


def analyze_liquidity_family(
    df: pd.DataFrame,
    symbol: str,
    event_type: str = "LIQUIDITY_SHOCK",
    **params: Any,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    events = detect_liquidity_family(df, symbol, event_type=event_type, **params)
    market = df[["timestamp", "close"]].copy() if not df.empty and "close" in df.columns else None
    analyzer_results = run_analyzer_suite(events, market=market) if not events.empty else {}
    return events, analyzer_results


__all__ = [
    "analyze_liquidity_family",
    "detect_liquidity_family",
]
