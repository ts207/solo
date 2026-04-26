from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.base import BaseEventDetector
from project.events.sparsify import sparsify_mask


class ThresholdDetector(BaseEventDetector):
    signal_column: str = "signal"
    threshold: float = 0.0
    threshold_quantile: float | None = None
    threshold_window: int = 2880
    threshold_floor: float | None = None
    direction: str = "ge"
    min_spacing: int = 1

    def compute_signal(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> pd.Series:
        if self.signal_column in features:
            return pd.to_numeric(features[self.signal_column], errors="coerce")
        if self.signal_column in df.columns:
            return pd.to_numeric(df[self.signal_column], errors="coerce")
        return pd.Series(0.0, index=df.index)

    def compute_threshold(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> pd.Series:
        from project.events.thresholding import dynamic_quantile_floor

        q = params.get("threshold_quantile", self.threshold_quantile)
        if q is not None:
            signal = self.compute_signal(df, features=features, **params)
            window = int(params.get("threshold_window", self.threshold_window))
            floor = params.get("threshold_floor", params.get("threshold", self.threshold))
            return dynamic_quantile_floor(
                signal.abs(), window=window, quantile=float(q), floor=float(floor)
            )

        value = params.get("threshold", self.threshold)
        return pd.Series(float(value), index=df.index, dtype=float)

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> pd.Series:
        signal = self.compute_signal(df, features=features, **params)
        threshold = self.compute_threshold(df, features=features, **params)
        direction = str(params.get("direction", self.direction)).lower()
        if direction == "ge":
            return (signal >= threshold).fillna(False)
        if direction == "gt":
            return (signal > threshold).fillna(False)
        if direction == "le":
            return (signal <= threshold).fillna(False)
        if direction == "lt":
            return (signal < threshold).fillna(False)
        raise ValueError(f"Unsupported threshold direction: {direction}")

    def compute_intensity(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> pd.Series:
        signal = self.compute_signal(df, features=features, **params)
        threshold = self.compute_threshold(df, features=features, **params).replace(0.0, np.nan)
        return (signal / threshold).astype(float)

    def event_indices(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> list[int]:
        mask = self.compute_raw_mask(df, features=features, **params)
        spacing = int(
            params.get("min_spacing", params.get("cooldown_bars", self.min_spacing))
        )
        return sparsify_mask(mask, min_spacing=spacing)
