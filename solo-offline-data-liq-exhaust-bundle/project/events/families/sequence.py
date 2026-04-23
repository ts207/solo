from __future__ import annotations

from typing import Any

import pandas as pd

from project.events.detectors.registry import get_detector
from project.events.detectors.sequence import EventSequenceDetector
from project.events.registries.sequence import (
    ensure_sequence_detectors_registered,
    get_sequence_detectors,
)
from project.events.shared import EVENT_COLUMNS
from project.research.analyzers import run_analyzer_suite


class SeqFndExtremeThenBreakoutDetector(EventSequenceDetector):
    event_type = "SEQ_FND_EXTREME_THEN_BREAKOUT"


class SeqLiqVacuumThenDepthRecoveryDetector(EventSequenceDetector):
    event_type = "SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY"


class SeqOiSpikeposThenVolSpikeDetector(EventSequenceDetector):
    event_type = "SEQ_OI_SPIKEPOS_THEN_VOL_SPIKE"


class SeqVolCompThenBreakoutDetector(EventSequenceDetector):
    event_type = "SEQ_VOL_COMP_THEN_BREAKOUT"

ensure_sequence_detectors_registered()

_DETECTORS = get_sequence_detectors()


def detect_sequence_family(
    df: pd.DataFrame, symbol: str, event_type: str, **params: Any
) -> pd.DataFrame:
    detector = get_detector(event_type)
    if detector is None:
        raise ValueError(f"Unknown sequence event type: {event_type}")
    return detector.detect(df, symbol=symbol, **params)


def analyze_sequence_family(
    df: pd.DataFrame, symbol: str, event_type: str, **params: Any
) -> tuple[pd.DataFrame, dict[str, Any]]:
    events = detect_sequence_family(df, symbol, event_type, **params)
    market = df[["timestamp", "close"]].copy() if not df.empty and "close" in df.columns else None
    analyzer_results = run_analyzer_suite(events, market=market) if not events.empty else {}
    return events, analyzer_results
