from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd


@dataclass
class SignalQualityMetrics:
    event_type: str
    total_bars: int
    trigger_count: int = 0
    co_trigger_count: dict[str, int] = field(default_factory=dict)
    inactivity_rate: float = 0.0
    mean_intensity: float = 0.0
    std_intensity: float = 0.0

    @property
    def trigger_frequency(self) -> float:
        if self.total_bars == 0:
            return 0.0
        return self.trigger_count / self.total_bars

    @property
    def co_trigger_rate(self) -> float:
        if self.trigger_count == 0:
            return 0.0
        return sum(self.co_trigger_count.values()) / self.trigger_count

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "total_bars": self.total_bars,
            "trigger_count": self.trigger_count,
            "trigger_frequency": self.trigger_frequency,
            "co_trigger_rate": self.co_trigger_rate,
            "inactivity_rate": self.inactivity_rate,
            "mean_intensity": self.mean_intensity,
            "std_intensity": self.std_intensity,
            "co_trigger_counts": dict(self.co_trigger_count),
        }


class SignalQualityAnalyzer:
    def __init__(self, events: pd.DataFrame):
        self.events = events

    def compute_metrics(self, event_type: str, window_bars: int = 640) -> SignalQualityMetrics:
        target_events = self.events[self.events["event_type"] == event_type]
        
        metrics = SignalQualityMetrics(
            event_type=event_type,
            total_bars=window_bars,
            trigger_count=len(target_events),
        )

        if len(target_events) > 0:
            metrics.mean_intensity = target_events["event_score"].mean() if "event_score" in target_events else 0.0
            metrics.std_intensity = target_events["event_score"].std() if "event_score" in target_events else 0.0

        all_types = set(self.events["event_type"].unique())
        all_types.discard(event_type)
        
        for other_type in all_types:
            other_events = self.events[self.events["event_type"] == other_type]
            if len(other_events) == 0:
                continue
            
            co_triggers = self._count_co_triggers(target_events, other_events)
            if co_triggers > 0:
                metrics.co_trigger_count[other_type] = co_triggers

        metrics.inactivity_rate = self._compute_inactivity_rate(target_events, window_bars)
        
        return metrics

    def _count_co_triggers(self, events_a: pd.DataFrame, events_b: pd.DataFrame) -> int:
        if events_a.empty or events_b.empty:
            return 0
        
        timestamps_a = set(events_a["eval_bar_ts"].dropna())
        timestamps_b = set(events_b["eval_bar_ts"].dropna())
        
        return len(timestamps_a & timestamps_b)

    def _compute_inactivity_rate(self, events: pd.DataFrame, window_bars: int) -> float:
        if events.empty or window_bars == 0:
            return 1.0
        
        return 1.0 - (len(events) / window_bars)

    def compute_all_metrics(self, window_bars: int = 640) -> dict[str, SignalQualityMetrics]:
        event_types = self.events["event_type"].unique()
        return {
            str(et): self.compute_metrics(str(et), window_bars)
            for et in event_types
        }

    def is_overfiring(self, event_type: str, threshold: float = 0.05) -> bool:
        metrics = self.compute_metrics(event_type)
        return metrics.trigger_frequency > threshold

    def has_high_co_trigger(self, event_type: str, threshold: float = 0.5) -> bool:
        metrics = self.compute_metrics(event_type)
        return metrics.co_trigger_rate > threshold
