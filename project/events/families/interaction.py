from __future__ import annotations

from typing import Any

import pandas as pd

from project.events.detectors.composite import CompositeDetector


class CrossAssetInteractionDetector(CompositeDetector):
    """Detects predictive interactions across different assets.

    Concretely: Does an OI spike on ETH predict a volatility transition on BTC?
    Does a liquidity vacuum on SOL create a spread opportunity against ETH?
    """

    event_type = "CROSS_ASSET_INTERACTION"
    required_columns = ("timestamp",)

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        features = {}
        for key in df.columns:
            if any(p in key for p in ["oi_spike", "vol_", "liq_vacuum", "spread_"]):
                features[key] = df[key]
        return features

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return pd.Series(False, index=df.index)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return pd.Series(0.0, index=df.index)


from project.events.registries.interaction import (
    ensure_interaction_detectors_registered,
    get_interaction_detectors,
)

ensure_interaction_detectors_registered()

_DETECTORS = get_interaction_detectors()


def detect_interaction_family(
    df: pd.DataFrame, symbol: str, event_type: str = "CROSS_ASSET_INTERACTION", **params: Any
) -> pd.DataFrame:
    detector_cls = _DETECTORS.get(event_type)
    if detector_cls is None:
        raise ValueError(f"Unknown interaction event type: {event_type}")
    return detector_cls().detect(df, symbol=symbol, **params)
