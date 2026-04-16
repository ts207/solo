from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from project.events.detectors.threshold import ThresholdDetector


class TransitionDetector(ThresholdDetector):
    transition_mode: str = "rising"

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> pd.Series:
        base = super().compute_raw_mask(df, features=features, **params)
        mode = str(params.get("transition_mode", self.transition_mode)).lower()
        if mode == "rising":
            return base & ~base.shift(1).fillna(False)
        if mode == "falling":
            return ~base & base.shift(1).fillna(False)
        if mode == "flip":
            return base.ne(base.shift(1)).fillna(False)
        raise ValueError(f"Unsupported transition mode: {mode}")
