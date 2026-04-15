from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from project.synthetic_truth.tools.scoring.normalize import SignalNormalizer, NormalizedSignal


@dataclass
class DetectorWeight:
    event_type: str
    weight: float
    min_confidence: float = 0.0

    def is_valid(self) -> bool:
        return 0.0 <= self.weight <= 1.0


@dataclass
class AggregatedSignal:
    timestamp: any
    aggregate_score: float
    dominant_event: Optional[str] = None
    event_count: int = 0
    details: dict = field(default_factory=dict)


class SignalAggregator:
    def __init__(
        self,
        weights: Optional[dict[str, float]] = None,
        normalizer: Optional[SignalNormalizer] = None,
    ):
        self.weights = weights or {}
        self.normalizer = normalizer or SignalNormalizer()
        self._detector_weights: list[DetectorWeight] = []

    def set_weight(self, event_type: str, weight: float, min_confidence: float = 0.0) -> None:
        self._detector_weights.append(DetectorWeight(
            event_type=event_type,
            weight=weight,
            min_confidence=min_confidence,
        ))

    def get_weight(self, event_type: str) -> float:
        return self.weights.get(event_type, 1.0)

    def aggregate_signals(
        self,
        signals: list[NormalizedSignal],
        method: str = "weighted_sum",
    ) -> AggregatedSignal:
        if not signals:
            return AggregatedSignal(
                timestamp=None,
                aggregate_score=0.0,
            )

        if method == "weighted_sum":
            return self._weighted_sum(signals)
        elif method == "max":
            return self._max_signal(signals)
        elif method == "mean":
            return self._mean_signal(signals)
        else:
            return self._weighted_sum(signals)

    def _weighted_sum(self, signals: list[NormalizedSignal]) -> AggregatedSignal:
        total_score = 0.0
        total_weight = 0.0
        event_counts: dict[str, int] = {}

        for signal in signals:
            weight = self.get_weight(signal.event_type)
            
            if signal.normalized_confidence < 0.1:
                continue
            
            weighted_strength = signal.normalized_strength * weight
            total_score += weighted_strength
            total_weight += weight
            
            event_counts[signal.event_type] = event_counts.get(signal.event_type, 0) + 1

        aggregate_score = total_score / total_weight if total_weight > 0 else 0.0
        
        dominant_event = max(event_counts, key=event_counts.get) if event_counts else None
        
        return AggregatedSignal(
            timestamp=None,
            aggregate_score=float(aggregate_score),
            dominant_event=dominant_event,
            event_count=len(signals),
            details={"event_counts": event_counts},
        )

    def _max_signal(self, signals: list[NormalizedSignal]) -> AggregatedSignal:
        if not signals:
            return AggregatedSignal(timestamp=None, aggregate_score=0.0)
        
        best = max(signals, key=lambda s: s.normalized_strength)
        
        return AggregatedSignal(
            timestamp=None,
            aggregate_score=best.normalized_strength,
            dominant_event=best.event_type,
            event_count=len(signals),
        )

    def _mean_signal(self, signals: list[NormalizedSignal]) -> AggregatedSignal:
        if not signals:
            return AggregatedSignal(timestamp=None, aggregate_score=0.0)
        
        mean_score = np.mean([s.normalized_strength for s in signals])
        
        return AggregatedSignal(
            timestamp=None,
            aggregate_score=float(mean_score),
            event_count=len(signals),
        )

    def aggregate_events(
        self,
        events: pd.DataFrame,
        method: str = "weighted_sum",
    ) -> list[AggregatedSignal]:
        if events.empty:
            return []
        
        signals = self.normalizer.normalize_batch(events)
        return [self.aggregate_signals(signals, method=method)]


def rank_events(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return events
    
    normalizer = SignalNormalizer()
    signals = normalizer.normalize_batch(events)
    
    result = events.copy()
    result["rank"] = [s.rank for s in signals]
    result["normalized_strength"] = [s.normalized_strength for s in signals]
    
    return result.sort_values("rank")
