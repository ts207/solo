from __future__ import annotations

import pytest
import pandas as pd

from project.synthetic_truth.tools.validation.conflicts import (
    ConflictAnalysis,
    ConflictReport,
    register_mutually_exclusive,
    unregister_mutually_exclusive,
    MUTUALLY_EXCLUSIVE_PAIRS,
)


class TestConflictReport:
    def test_is_conflict(self):
        report = ConflictReport(
            event_type_a="EVENT_A",
            event_type_b="EVENT_B",
            conflict_count=5,
            conflict_rate=0.5,
            severity="high",
        )
        assert report.is_conflict is True

    def test_no_conflict(self):
        report = ConflictReport(
            event_type_a="EVENT_A",
            event_type_b="EVENT_B",
            conflict_count=0,
            conflict_rate=0.0,
            severity="low",
        )
        assert report.is_conflict is False


class TestConflictAnalysis:
    def test_detects_mutually_exclusive_conflict(self):
        ts = pd.date_range("2024-01-01", periods=5, freq="1h", tz="UTC")
        events = pd.DataFrame({
            "event_type": ["TREND_ACCELERATION"] * 5 + ["TREND_DECELERATION"] * 5,
            "eval_bar_ts": list(ts) + list(ts),
        })

        analysis = ConflictAnalysis(events)
        
        assert analysis.has_conflicts()

    def test_no_conflict_when_no_overlap(self):
        ts_a = pd.date_range("2024-01-01", periods=5, freq="1h", tz="UTC")
        ts_b = pd.date_range("2024-01-06", periods=5, freq="1h", tz="UTC")
        events = pd.DataFrame({
            "event_type": ["EVENT_A"] * 5 + ["EVENT_B"] * 5,
            "eval_bar_ts": list(ts_a) + list(ts_b),
        })

        analysis = ConflictAnalysis(events)
        
        assert not analysis.has_conflicts()

    def test_register_mutually_exclusive(self):
        initial_count = len(MUTUALLY_EXCLUSIVE_PAIRS)
        register_mutually_exclusive("NEW_EVENT_A", "NEW_EVENT_B")
        assert len(MUTUALLY_EXCLUSIVE_PAIRS) == initial_count + 1
        
        unregister_mutually_exclusive("NEW_EVENT_A", "NEW_EVENT_B")
        assert len(MUTUALLY_EXCLUSIVE_PAIRS) == initial_count

    def test_severity_rating(self):
        ts = pd.date_range("2024-01-01", periods=5, freq="1h", tz="UTC")
        events = pd.DataFrame({
            "event_type": ["VOL_SPIKE"] * 5 + ["VOL_RELAXATION_START"] * 5,
            "eval_bar_ts": list(ts) + list(ts),
        })

        analysis = ConflictAnalysis(events)
        conflicts = analysis.conflicts
        
        assert len(conflicts) > 0
        for conflict in conflicts:
            assert conflict.severity in ("low", "medium", "high", "critical")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
