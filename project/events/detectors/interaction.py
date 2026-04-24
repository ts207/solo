from __future__ import annotations

from typing import Any

import pandas as pd

from project.events.detectors.base import BaseEventDetector


class EventInteractionDetector(BaseEventDetector):
    """
    A unified detector for interactions between two mechanisms (events/states).
    Delegates to interaction_analyzer module.
    """

    def __init__(self, interaction_name: str, left_id: str, right_id: str, op: str, lag: int):
        super().__init__()
        self.interaction_name = interaction_name
        self.left_id = left_id
        self.right_id = right_id
        self.op = op
        self.lag = lag
        self.event_type = interaction_name

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        return {}

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return pd.Series(False, index=df.index)

    def detect(self, df: pd.DataFrame, symbol: str, **params: Any) -> pd.DataFrame:
        if df.empty or "event_type" not in df.columns:
            return pd.DataFrame()

        from project.events.interaction_analyzer import detect_interactions

        return detect_interactions(
            df=df,
            left_id=params.get("left_id", self.left_id),
            right_id=params.get("right_id", self.right_id),
            op=params.get("op", self.op),
            lag=params.get("lag", self.lag),
            interaction_name=self.interaction_name,
        )
