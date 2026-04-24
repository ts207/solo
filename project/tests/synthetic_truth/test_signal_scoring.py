from __future__ import annotations

import pandas as pd
import pytest

from project.synthetic_truth.tools.scoring.aggregate import (
    SignalAggregator,
    rank_events,
)
from project.synthetic_truth.tools.scoring.normalize import (
    NormalizationBounds,
    NormalizedSignal,
    SignalNormalizer,
)


class TestNormalizationBounds:
    def test_range_calculation(self):
        bounds = NormalizationBounds(min_value=0.0, max_value=10.0)
        assert bounds.range == 10.0

    def test_is_valid(self):
        valid = NormalizationBounds(min_value=0.0, max_value=10.0)
        invalid = NormalizationBounds(min_value=5.0, max_value=5.0)

        assert valid.is_valid() is True
        assert invalid.is_valid() is False


class TestSignalNormalizer:
    def test_normalize_strength_within_bounds(self):
        normalizer = SignalNormalizer()
        normalizer.set_bounds("TEST", NormalizationBounds(min_value=0.0, max_value=10.0))

        result = normalizer.normalize_strength(5.0, "TEST")
        assert 0.4 <= result <= 0.6

    def test_normalize_strength_clipped(self):
        normalizer = SignalNormalizer()
        normalizer.set_bounds("TEST", NormalizationBounds(min_value=0.0, max_value=10.0))

        result = normalizer.normalize_strength(15.0, "TEST")
        assert result == 1.0

    def test_normalize_confidence(self):
        normalizer = SignalNormalizer()

        assert normalizer.normalize_confidence(0.5) == 0.5
        assert normalizer.normalize_confidence(-0.5) == 0.0
        assert normalizer.normalize_confidence(1.5) == 1.0

    def test_learn_bounds(self):
        events = pd.DataFrame({
            "event_type": ["A", "A", "B", "B"],
            "event_score": [1.0, 3.0, 2.0, 4.0],
        })

        normalizer = SignalNormalizer()
        normalizer.learn_bounds(events)

        assert "A" in normalizer.bounds
        assert normalizer.bounds["A"].min_value == 1.0
        assert normalizer.bounds["A"].max_value == 3.0


class TestSignalAggregator:
    def test_weighted_sum(self):
        aggregator = SignalAggregator(weights={"A": 1.0, "B": 2.0})

        signals = [
            NormalizedSignal("A", True, 0.5, 0.8, 0.5, 0.8),
            NormalizedSignal("B", True, 0.8, 0.9, 0.8, 0.9),
        ]

        result = aggregator.aggregate_signals(signals, method="weighted_sum")

        assert result.aggregate_score > 0
        assert result.dominant_event in ("A", "B")

    def test_max_signal(self):
        aggregator = SignalAggregator()

        signals = [
            NormalizedSignal("A", True, 0.3, 0.5, 0.3, 0.5),
            NormalizedSignal("B", True, 0.9, 0.9, 0.9, 0.9),
        ]

        result = aggregator.aggregate_signals(signals, method="max")

        assert result.aggregate_score == 0.9
        assert result.dominant_event == "B"

    def test_empty_signals(self):
        aggregator = SignalAggregator()

        result = aggregator.aggregate_signals([], method="weighted_sum")

        assert result.aggregate_score == 0.0
        assert result.event_count == 0


class TestRankEvents:
    def test_rank_events_by_strength(self):
        events = pd.DataFrame({
            "event_type": ["A", "B", "C"],
            "event_score": [0.5, 0.9, 0.3],
            "evt_signal_intensity": [0.5, 0.9, 0.5],
        })

        result = rank_events(events)

        assert list(result["rank"]) == [1, 2, 3]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
