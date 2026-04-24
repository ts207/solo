from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class MatchResult:
    matched: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)


class Matcher(ABC):
    @abstractmethod
    def match(self, events: pd.DataFrame, ground_truth: dict) -> MatchResult:
        raise NotImplementedError


class TriggerMatcher(Matcher):
    """Assert that specified event types were detected."""

    def __init__(self, event_types: list[str], tolerance: int = 5):
        self.event_types = [et.upper() for et in event_types]
        self.tolerance = tolerance

    def match(self, events: pd.DataFrame, ground_truth: dict) -> MatchResult:
        if events.empty:
            detected = set()
        else:
            detected = set(events["event_type"].astype(str).str.upper().unique())

        missing = [et for et in self.event_types if et not in detected]

        if missing:
            return MatchResult(
                matched=False,
                message=f"Missed triggers: {missing}",
                details={"expected": self.event_types, "detected": list(detected), "missing": missing},
            )

        return MatchResult(
            matched=True,
            message=f"All expected triggers detected: {self.event_types}",
            details={"expected": self.event_types, "detected": list(detected)},
        )


class NoTriggerMatcher(Matcher):
    """Assert that specified event types were NOT detected."""

    def __init__(self, event_types: list[str]):
        self.event_types = [et.upper() for et in event_types]

    def match(self, events: pd.DataFrame, ground_truth: dict) -> MatchResult:
        if events.empty:
            return MatchResult(
                matched=True,
                message="No events detected (expected)",
                details={"checked": self.event_types},
            )

        detected = set(events["event_type"].astype(str).str.upper().unique())
        false_positives = [et for et in self.event_types if et in detected]

        if false_positives:
            return MatchResult(
                matched=False,
                message=f"False positives detected: {false_positives}",
                details={"excluded": self.event_types, "detected": list(detected), "false_positives": false_positives},
            )

        return MatchResult(
            matched=True,
            message=f"No false positives: {self.event_types} not detected",
            details={"excluded": self.event_types, "detected": list(detected)},
        )


class TimingMatcher(Matcher):
    """Assert events fired within expected time window."""

    def __init__(self, event_type: str, expected_bar: int, tolerance: int = 5):
        self.event_type = event_type.upper()
        self.expected_bar = expected_bar
        self.tolerance = tolerance

    def match(self, events: pd.DataFrame, ground_truth: dict) -> MatchResult:
        if events.empty:
            return MatchResult(
                matched=False,
                message=f"No events detected for timing check: {self.event_type}",
                details={"expected_bar": self.expected_bar},
            )

        target_events = events[events["event_type"].astype(str).str.upper() == self.event_type]
        if target_events.empty:
            return MatchResult(
                matched=False,
                message=f"No {self.event_type} events detected",
                details={"expected_bar": self.expected_bar},
            )

        event_bars = []
        for idx, row in target_events.iterrows():
            meta = row.get("features_payload", {})
            if isinstance(meta, dict):
                event_bar = meta.get("event_idx", -1)
            else:
                event_bar = -1
            event_bars.append(event_bar)

        detected_bar = min(event_bars)
        diff = abs(detected_bar - self.expected_bar)

        if diff <= self.tolerance:
            return MatchResult(
                matched=True,
                message=f"Timing OK: {self.event_type} at bar {detected_bar} (expected ~{self.expected_bar})",
                details={"expected_bar": self.expected_bar, "detected_bar": detected_bar, "diff": diff},
            )

        return MatchResult(
            matched=False,
            message=f"Timing error: {self.event_type} at bar {detected_bar} (expected ~{self.expected_bar})",
            details={"expected_bar": self.expected_bar, "detected_bar": detected_bar, "diff": diff},
        )


class SeverityMatcher(Matcher):
    """Assert events have expected severity."""

    def __init__(self, event_type: str, expected_severities: list[str]):
        self.event_type = event_type.upper()
        self.expected_severities = [s.lower() for s in expected_severities]

    def match(self, events: pd.DataFrame, ground_truth: dict) -> MatchResult:
        if events.empty:
            return MatchResult(
                matched=False,
                message=f"No events to check severity for: {self.event_type}",
                details={"expected_severities": self.expected_severities},
            )

        target_events = events[events["event_type"].astype(str).str.upper() == self.event_type]
        if target_events.empty:
            return MatchResult(
                matched=True,
                message=f"No {self.event_type} events - severity check skipped",
                details={"expected_severities": self.expected_severities},
            )

        detected_severities = target_events["severity_bucket"].astype(str).str.lower().unique()

        mismatches = [s for s in detected_severities if s not in self.expected_severities]
        if mismatches:
            return MatchResult(
                matched=False,
                message=f"Severity mismatch: detected {mismatches}, expected {self.expected_severities}",
                details={"expected": self.expected_severities, "detected": list(detected_severities)},
            )

        return MatchResult(
            matched=True,
            message=f"Severity OK: {detected_severities} in {self.expected_severities}",
            details={"expected": self.expected_severities, "detected": list(detected_severities)},
        )


class DirectionMatcher(Matcher):
    """Assert events have expected direction."""

    def __init__(self, event_type: str, expected_direction: str):
        self.event_type = event_type.upper()
        self.expected_direction = expected_direction.lower()

    def match(self, events: pd.DataFrame, ground_truth: dict) -> MatchResult:
        if events.empty:
            return MatchResult(
                matched=False,
                message=f"No events to check direction for: {self.event_type}",
                details={"expected_direction": self.expected_direction},
            )

        target_events = events[events["event_type"].astype(str).str.upper() == self.event_type]
        if target_events.empty:
            return MatchResult(
                matched=True,
                message=f"No {self.event_type} events - direction check skipped",
                details={"expected_direction": self.expected_direction},
            )

        detected_direction = target_events["direction"].astype(str).str.lower().iloc[0]

        if detected_direction == self.expected_direction:
            return MatchResult(
                matched=True,
                message=f"Direction OK: {detected_direction}",
                details={"expected": self.expected_direction, "detected": detected_direction},
            )

        return MatchResult(
            matched=False,
            message=f"Direction mismatch: detected {detected_direction}, expected {self.expected_direction}",
            details={"expected": self.expected_direction, "detected": detected_direction},
        )
