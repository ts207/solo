from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Protocol

import numpy as np
import pandas as pd


@dataclass
class GeneratorConfig:
    n_bars: int = 640
    seed: int = 42
    injection_point: int = 320
    injection_duration: int = 20
    base_price: float = 100.0
    timeframe: str = "5min"
    frequency: str = "5min"


class GeneratorProtocol(Protocol):
    def generate_base(self, config: GeneratorConfig) -> pd.DataFrame:
        """Generate normal/quiet market data."""

    def inject_signal(self, df: pd.DataFrame, config: GeneratorConfig) -> pd.DataFrame:
        """Modify df to create scenario signal."""

    def required_columns(self) -> tuple[str, ...]:
        """Columns this generator produces."""


class BaseGenerator(ABC):
    @abstractmethod
    def generate_base(self, config: GeneratorConfig) -> pd.DataFrame:
        """Generate normal/quiet market data."""
        raise NotImplementedError

    @abstractmethod
    def inject_signal(self, df: pd.DataFrame, config: GeneratorConfig) -> pd.DataFrame:
        """Modify df to create scenario signal."""
        raise NotImplementedError

    def required_columns(self) -> tuple[str, ...]:
        """Columns this generator produces."""
        return ("timestamp",)

    def generate(self, config: GeneratorConfig) -> pd.DataFrame:
        """Full pipeline: generate base, inject signal."""
        df = self.generate_base(config)
        df = self.inject_signal(df, config)
        return df

    def _ensure_timestamp(self, df: pd.DataFrame, config: GeneratorConfig) -> pd.DataFrame:
        """Ensure DataFrame has timestamp column."""
        if "timestamp" not in df.columns:
            df = df.copy()
            df.insert(
                0,
                "timestamp",
                pd.date_range("2024-01-01", periods=len(df), freq=config.frequency, tz="UTC"),
            )
        return df

    def _smooth_transition(
        self,
        arr: np.ndarray,
        injection_point: int,
        duration: int,
        target_value: float,
        ramp_bars: int = 5,
    ) -> np.ndarray:
        """Apply smooth ramp-in and ramp-out around injection point."""
        result = arr.copy()
        pre_value = arr[max(0, injection_point - 1)]
        ramp_start = max(0, injection_point - ramp_bars)
        ramp_end = min(len(arr), injection_point + duration + ramp_bars)
        for i in range(ramp_start, injection_point):
            alpha = (i - ramp_start) / max(1, injection_point - ramp_start)
            result[i] = pre_value + alpha * (target_value - pre_value)
        for i in range(injection_point, min(len(arr), injection_point + duration)):
            result[i] = target_value
        for i in range(injection_point + duration, ramp_end):
            alpha = (ramp_end - i) / max(1, ramp_end - injection_point - duration)
            result[i] = target_value + alpha * (pre_value - target_value)
        return result
