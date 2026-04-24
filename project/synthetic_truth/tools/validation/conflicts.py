from __future__ import annotations

from dataclasses import dataclass, field
from typing import FrozenSet

import pandas as pd

DEFAULT_MUTUALLY_EXCLUSIVE_PAIRS: set[FrozenSet[str]] = {
    frozenset({"TREND_ACCELERATION", "TREND_DECELERATION"}),
    frozenset({"CHOP_TO_TREND_SHIFT", "TREND_TO_CHOP_SHIFT"}),
    frozenset({"VOL_RELAXATION_START", "VOL_SPIKE"}),
    frozenset({"BREAKOUT_TRIGGER", "FALSE_BREAKOUT"}),
    frozenset({"FUNDING_FLIP", "FUNDING_NORMALIZATION_TRIGGER"}),
    frozenset({"RANGE_BREAKOUT", "RANGE_COMPRESSION_END"}),
}

MUTUALLY_EXCLUSIVE_PAIRS: set[FrozenSet[str]] = DEFAULT_MUTUALLY_EXCLUSIVE_PAIRS.copy()


@dataclass
class ConflictReport:
    event_type_a: str
    event_type_b: str
    conflict_count: int
    conflict_rate: float
    severity: str

    @property
    def is_conflict(self) -> bool:
        return self.conflict_count > 0

    def to_dict(self) -> dict:
        return {
            "event_a": self.event_type_a,
            "event_b": self.event_type_b,
            "conflict_count": self.conflict_count,
            "conflict_rate": self.conflict_rate,
            "severity": self.severity,
        }


@dataclass
class ConflictAnalysis:
    events: pd.DataFrame
    _conflict_reports: list[ConflictReport] = field(default_factory=list)

    def __post_init__(self):
        self._analyze()

    def _analyze(self) -> None:
        self._conflict_reports = []

        for pair in MUTUALLY_EXCLUSIVE_PAIRS:
            events_list = list(pair)
            if len(events_list) != 2:
                continue

            event_a, event_b = events_list[0], events_list[1]

            subset_a = self.events[self.events["event_type"] == event_a]
            subset_b = self.events[self.events["event_type"] == event_b]

            if subset_a.empty or subset_b.empty:
                continue

            timestamps_a = set(subset_a["eval_bar_ts"].dropna())
            timestamps_b = set(subset_b["eval_bar_ts"].dropna())

            conflicts = len(timestamps_a & timestamps_b)

            if conflicts > 0:
                conflict_rate = conflicts / min(len(subset_a), len(subset_b))
                severity = self._rate_severity(conflict_rate)

                self._conflict_reports.append(ConflictReport(
                    event_type_a=event_a,
                    event_type_b=event_b,
                    conflict_count=conflicts,
                    conflict_rate=conflict_rate,
                    severity=severity,
                ))

    def _rate_severity(self, conflict_rate: float) -> str:
        if conflict_rate >= 0.5:
            return "critical"
        elif conflict_rate >= 0.2:
            return "high"
        elif conflict_rate >= 0.1:
            return "medium"
        else:
            return "low"

    @property
    def conflicts(self) -> list[ConflictReport]:
        return self._conflict_reports

    def has_conflicts(self) -> bool:
        return len(self._conflict_reports) > 0

    def critical_conflicts(self) -> list[ConflictReport]:
        return [c for c in self._conflict_reports if c.severity == "critical"]

    def high_conflicts(self) -> list[ConflictReport]:
        return [c for c in self._conflict_reports if c.severity in ("critical", "high")]

    def summary(self) -> dict:
        return {
            "total_conflicts": len(self._conflict_reports),
            "critical": len(self.critical_conflicts()),
            "high": len(self.high_conflicts()),
            "conflicts": [c.to_dict() for c in self._conflict_reports],
        }


def register_mutually_exclusive(event_a: str, event_b: str) -> None:
    MUTUALLY_EXCLUSIVE_PAIRS.add(frozenset({event_a.upper(), event_b.upper()}))


def unregister_mutually_exclusive(event_a: str, event_b: str) -> None:
    pair = frozenset({event_a.upper(), event_b.upper()})
    MUTUALLY_EXCLUSIVE_PAIRS.discard(pair)
