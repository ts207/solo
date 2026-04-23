from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class NormalizationBounds:
    min_value: float
    max_value: float
    center: float = 0.0

    @property
    def range(self) -> float:
        return self.max_value - self.min_value

    def is_valid(self) -> bool:
        return self.range > 0


@dataclass
class NormalizedSignal:
    event_type: str
    trigger: bool
    strength: float
    confidence: float
    normalized_strength: float
    normalized_confidence: float
    rank: Optional[int] = None


class SignalNormalizer:
    def __init__(self, bounds: Optional[dict[str, NormalizationBounds]] = None):
        self.bounds = bounds or {}
        self._default_bounds = NormalizationBounds(
            min_value=0.0,
            max_value=1.0,
            center=0.0,
        )

    def set_bounds(self, event_type: str, bounds: NormalizationBounds) -> None:
        self.bounds[event_type] = bounds

    def learn_bounds(self, events: pd.DataFrame) -> None:
        if events.empty:
            return
        
        for event_type in events["event_type"].unique():
            subset = events[events["event_type"] == event_type]
            
            if "event_score" in subset.columns:
                self.bounds[str(event_type)] = NormalizationBounds(
                    min_value=subset["event_score"].min(),
                    max_value=subset["event_score"].max(),
                    center=subset["event_score"].mean(),
                )

    def normalize_strength(self, value: float, event_type: str) -> float:
        bounds = self.bounds.get(event_type, self._default_bounds)
        
        if not bounds.is_valid():
            return 0.5
        
        normalized = (value - bounds.min_value) / bounds.range
        
        return float(np.clip(normalized, 0.0, 1.0))

    def normalize_confidence(self, value: float) -> float:
        return float(np.clip(value, 0.0, 1.0))

    def normalize_signal(
        self,
        event_type: str,
        trigger: bool,
        strength: float,
        confidence: float,
    ) -> NormalizedSignal:
        normalized_strength = self.normalize_strength(strength, event_type)
        normalized_confidence = self.normalize_confidence(confidence)
        
        return NormalizedSignal(
            event_type=event_type,
            trigger=trigger,
            strength=strength,
            confidence=confidence,
            normalized_strength=normalized_strength,
            normalized_confidence=normalized_confidence,
        )

    def normalize_batch(self, events: pd.DataFrame) -> list[NormalizedSignal]:
        if events.empty:
            return []
        
        self.learn_bounds(events)
        
        signals = []
        for _, row in events.iterrows():
            signal = self.normalize_signal(
                event_type=str(row.get("event_type", "")),
                trigger=True,
                strength=float(row.get("event_score", 0.0)),
                confidence=float(row.get("evt_signal_intensity", 0.5)),
            )
            signals.append(signal)
        
        signals.sort(key=lambda s: s.normalized_strength, reverse=True)
        for i, signal in enumerate(signals, 1):
            signal.rank = i
        
        return signals
