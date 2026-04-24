from __future__ import annotations

import pandas as pd
import pytest

from project.synthetic_truth.tools.metrics.signal_quality import (
    SignalQualityAnalyzer,
    SignalQualityMetrics,
)


class TestSignalQualityMetrics:
    def test_trigger_frequency(self):
        metrics = SignalQualityMetrics(
            event_type="TEST_EVENT",
            total_bars=100,
            trigger_count=5,
        )
        assert metrics.trigger_frequency == 0.05

    def test_co_trigger_rate(self):
        metrics = SignalQualityMetrics(
            event_type="TEST_EVENT",
            total_bars=100,
            trigger_count=10,
            co_trigger_count={"OTHER_EVENT": 3},
        )
        assert metrics.co_trigger_rate == 0.3

    def test_inactivity_rate(self):
        metrics = SignalQualityMetrics(
            event_type="TEST_EVENT",
            total_bars=100,
            trigger_count=10,
            inactivity_rate=0.9,
        )
        assert metrics.inactivity_rate == 0.9


class TestSignalQualityAnalyzer:
    def test_compute_metrics(self):
        events = pd.DataFrame({
            "event_type": ["TEST_EVENT", "TEST_EVENT", "OTHER_EVENT"],
            "eval_bar_ts": pd.date_range("2024-01-01", periods=3, tz="UTC"),
            "event_score": [1.0, 2.0, 1.5],
        })

        analyzer = SignalQualityAnalyzer(events)
        metrics = analyzer.compute_metrics("TEST_EVENT", window_bars=640)

        assert metrics.event_type == "TEST_EVENT"
        assert metrics.trigger_count == 2
        assert metrics.mean_intensity == 1.5

    def test_is_overfiring(self):
        events = pd.DataFrame({
            "event_type": ["TEST_EVENT"] * 50,
            "eval_bar_ts": pd.date_range("2024-01-01", periods=50, tz="UTC"),
        })

        analyzer = SignalQualityAnalyzer(events)
        assert analyzer.is_overfiring("TEST_EVENT", threshold=0.05)
        assert not analyzer.is_overfiring("OTHER_EVENT", threshold=0.05)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
