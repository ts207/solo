from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from project.events.detectors.base import BaseEventDetector
from project.events.sparsify import sparsify_mask


class CompositeDetector(BaseEventDetector):
    component_columns: tuple[str, ...] = ()
    combination: str = "all"
    min_spacing: int = 1

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> pd.Series:
        columns = tuple(params.get("component_columns", self.component_columns))
        if not columns:
            raise ValueError("CompositeDetector requires component_columns")
        parts = []
        for column in columns:
            if column in features:
                parts.append(pd.Series(features[column], index=df.index).fillna(False).astype(bool))
            else:
                parts.append(pd.Series(df[column], index=df.index).fillna(False).astype(bool))
        combo = str(params.get("combination", self.combination)).lower()
        if combo == "all":
            out = parts[0]
            for part in parts[1:]:
                out = out & part
            return out
        if combo == "any":
            out = parts[0]
            for part in parts[1:]:
                out = out | part
            return out
        raise ValueError(f"Unsupported combination: {combo}")

    def event_indices(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> list[int]:
        mask = self.compute_raw_mask(df, features=features, **params)
        return sparsify_mask(mask, min_spacing=int(params.get("min_spacing", self.min_spacing)))
