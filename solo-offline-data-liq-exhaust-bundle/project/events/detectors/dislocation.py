from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from project.events.detectors.threshold import ThresholdDetector


class DislocationDetector(ThresholdDetector):
    use_absolute_signal: bool = True

    def compute_signal(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> pd.Series:
        signal = super().compute_signal(df, features=features, **params)
        if bool(params.get("use_absolute_signal", self.use_absolute_signal)):
            return signal.abs()
        return signal
