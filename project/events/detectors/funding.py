from __future__ import annotations

from typing import Any, ClassVar

import numpy as np
import pandas as pd

from project.events.detectors.funding_support import (
    prepare_funding_normalization_features,
    prepare_funding_persistence_features,
    run_length,
)
from project.events.detectors.threshold import ThresholdDetector
from project.events.sparsify import sparsify_mask
from project.events.thresholding import rolling_percentile_rank

FUNDING_EVENT_TYPES = (
    "FUNDING_EXTREME_ONSET",
    "FUNDING_PERSISTENCE_TRIGGER",
    "FUNDING_NORMALIZATION_TRIGGER",
)


from project.events.detectors.base import MarketEventDetector


class BaseFundingDetector(ThresholdDetector, MarketEventDetector):
    """Base logic for funding-related detectors."""

    required_columns = ("timestamp", "funding_abs_pct", "funding_abs")
    default_severity_major_threshold = 0.95

    defaults: ClassVar[dict[str, Any]] = {
        "extreme_pct": 95.0,
        "accel_pct": 90.0,
        "persistence_pct": 85.0,
        "normalization_pct": 50.0,
        "normalization_lookback": 288,
        "min_prior_extreme_abs": 0.0004,
    }

    def _signed_funding(self, df: pd.DataFrame) -> pd.Series:
        if "funding_rate_scaled" in df.columns:
            return pd.to_numeric(df["funding_rate_scaled"], errors="coerce").astype(float)
        return pd.Series(0.0, index=df.index, dtype=float)

    def compute_direction(
        self,
        idx: int,
        features: dict[str, pd.Series],
        **params: Any,
    ) -> str:
        del params
        signed = float(
            np.nan_to_num(features.get("funding_signed", pd.Series(0.0)).iloc[idx], nan=0.0)
        )
        if signed > 0.0:
            return "up"
        if signed < 0.0:
            return "down"
        return "non_directional"

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return features["funding_abs_pct"] / 100.0

    def compute_severity(
        self,
        idx: int,
        intensity: float,
        features: dict[str, pd.Series],
        **params: Any,
    ) -> str:
        del idx, features
        threshold = float(params.get("severity_major_threshold", self.default_severity_major_threshold))
        return "major" if intensity >= threshold else "moderate"

    def compute_metadata(
        self, idx: int, features: dict[str, pd.Series], **params: Any
    ) -> dict[str, Any]:
        del params
        f_pct = float(np.nan_to_num(features["funding_abs_pct"].iloc[idx], nan=0.0))
        signed = float(
            np.nan_to_num(features.get("funding_signed", pd.Series(0.0)).iloc[idx], nan=0.0)
        )
        return {
            "funding_abs_pct": f_pct,
            "funding_abs": float(np.nan_to_num(features["funding_abs"].iloc[idx], nan=0.0)),
            "fr_magnitude": f_pct if f_pct > 1.0 else f_pct * 10000.0,
            "funding_signed": signed,
            "fr_sign": 1.0 if signed > 0.0 else -1.0 if signed < 0.0 else 0.0,
        }


class FundingExtremeOnsetDetector(BaseFundingDetector):
    """Detects the initial onset of extreme funding rates."""

    event_type = "FUNDING_EXTREME_ONSET"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        f_pct = pd.to_numeric(df["funding_abs_pct"], errors="coerce").astype(float)
        f_abs = pd.to_numeric(df["funding_abs"], errors="coerce").astype(float)
        funding_signed = self._signed_funding(df)

        extreme_pct = float(params.get("extreme_pct", self.defaults["extreme_pct"]))
        accel_pct = float(params.get("accel_pct", self.defaults["accel_pct"]))
        accel_lookback = int(params.get("accel_lookback", 12))
        persistence_bars = int(params.get("persistence_bars", 1))
        threshold_window = int(params.get("threshold_window", 2880))

        accel = (f_abs - f_abs.shift(accel_lookback)).clip(lower=0.0)
        accel_rank = rolling_percentile_rank(
            accel,
            window=threshold_window,
            min_periods=max(1, min(threshold_window, max(24, accel_lookback))),
            shift=0,
            scale=100.0,
        ).fillna(0.0)
        extreme_flag = (f_pct >= extreme_pct).fillna(False)
        accel_flag = (accel_rank >= accel_pct).fillna(False)
        persistence_run = run_length(extreme_flag)
        persist_flag = (persistence_run >= persistence_bars).fillna(False)
        qualified = (extreme_flag & accel_flag & persist_flag).fillna(False)
        onset = (qualified & ~qualified.shift(1, fill_value=False)).fillna(False)

        return {
            "funding_abs_pct": f_pct,
            "funding_abs": f_abs,
            "funding_signed": funding_signed,
            "accel_rank": accel_rank,
            "persistence_run": persistence_run.astype(float),
            "accel_flag": accel_flag.astype(float),
            "persist_flag": persist_flag.astype(float),
            "mask": onset,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        mask = features["mask"]
        cooldown = int(params.get("cooldown_bars", 0))
        if cooldown > 0:
            indices = sparsify_mask(mask, min_spacing=cooldown)
            out = pd.Series(False, index=mask.index)
            out.iloc[indices] = True
            return out
        return mask

    def compute_metadata(
        self, idx: int, features: dict[str, pd.Series], **params: Any
    ) -> dict[str, Any]:
        base = super().compute_metadata(idx, features, **params)
        base.update(
            {
                "funding_accel_rank": float(np.nan_to_num(features["accel_rank"].iloc[idx], nan=0.0)),
                "funding_persistence_bars": float(np.nan_to_num(features["persistence_run"].iloc[idx], nan=0.0)),
            }
        )
        return base


class FundingPersistenceDetector(BaseFundingDetector):
    """Detects sustained elevated funding or rapid acceleration."""

    event_type = "FUNDING_PERSISTENCE_TRIGGER"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        return prepare_funding_persistence_features(
            df, self._signed_funding(df), self.defaults, params
        )

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return features["mask"]

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return features["signal_intensity"].fillna(0.0)

    def compute_metadata(
        self, idx: int, features: dict[str, pd.Series], **params: Any
    ) -> dict[str, Any]:
        base = super().compute_metadata(idx, features, **params)
        base.update(
            {
                "funding_subtype": str(features["subtype"].iloc[idx]),
                "funding_run_length": float(np.nan_to_num(features["run_len"].iloc[idx], nan=0.0)),
                "funding_accel_rank": float(
                    np.nan_to_num(features["accel_rank"].iloc[idx], nan=0.0)
                ),
            }
        )
        return base


class FundingNormalizationDetector(BaseFundingDetector):
    """Detects funding returning to baseline after extreme conditions."""

    event_type = "FUNDING_NORMALIZATION_TRIGGER"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        return prepare_funding_normalization_features(
            df, self._signed_funding(df), self.defaults, params
        )

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return features["mask"]

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return features["signal_intensity"].fillna(0.0)

    def compute_metadata(
        self, idx: int, features: dict[str, pd.Series], **params: Any
    ) -> dict[str, Any]:
        base = super().compute_metadata(idx, features, **params)
        base.update(
            {
                "funding_subtype": "normalization",
                "prior_extreme_pct": float(
                    np.nan_to_num(features["prior_extreme_pct"].iloc[idx], nan=0.0)
                ),
                "prior_extreme_abs": float(
                    np.nan_to_num(features["prior_extreme_abs"].iloc[idx], nan=0.0)
                ),
            }
        )
        return base


class FundingDetector(BaseFundingDetector):
    """Legacy polymorphic funding detector for backward compatibility."""

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        onset = FundingExtremeOnsetDetector().prepare_features(df, **params)
        persistence = FundingPersistenceDetector().prepare_features(df, **params)
        normalization = FundingNormalizationDetector().prepare_features(df, **params)

        return {
            "funding_abs_pct": onset["funding_abs_pct"],
            "funding_abs": onset["funding_abs"],
            "funding_signed": onset["funding_signed"],
            "FUNDING_EXTREME_ONSET": onset["mask"],
            "FUNDING_PERSISTENCE_TRIGGER": persistence["mask"],
            "FUNDING_NORMALIZATION_TRIGGER": normalization["mask"],
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return (
            features["FUNDING_EXTREME_ONSET"]
            | features["FUNDING_PERSISTENCE_TRIGGER"]
            | features["FUNDING_NORMALIZATION_TRIGGER"]
        ).fillna(False)

    def compute_event_type(self, idx: int, features: dict[str, pd.Series]) -> str:
        if features["FUNDING_EXTREME_ONSET"].iloc[idx]:
            return "FUNDING_EXTREME_ONSET"
        if features["FUNDING_PERSISTENCE_TRIGGER"].iloc[idx]:
            return "FUNDING_PERSISTENCE_TRIGGER"
        return "FUNDING_NORMALIZATION_TRIGGER"


class FundingFlipDetector(ThresholdDetector):
    event_type = "FUNDING_FLIP"
    required_columns = ("timestamp", "funding_rate_scaled")
    causal = False
    min_spacing = 24
    min_magnitude_quantile: float = 0.75

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        funding = pd.to_numeric(df["funding_rate_scaled"], errors="coerce").astype(float)
        funding_abs = funding.abs()
        funding_prev_abs = funding_abs.shift(1)

        # Adaptive thresholds
        window = int(params.get("threshold_window", 2880))
        q_mag = float(params.get("min_magnitude_quantile", self.min_magnitude_quantile))
        min_flip_abs = float(params.get("min_flip_abs", 2.5e-4))

        funding_q_mag = funding_abs.rolling(window, min_periods=288).quantile(q_mag).shift(1)
        required_abs = funding_q_mag.fillna(0.0).clip(lower=min_flip_abs)

        return {
            "funding": funding,
            "funding_abs": funding_abs,
            "funding_prev_abs": funding_prev_abs,
            "required_abs": required_abs,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        funding = features["funding"]
        funding_abs = features["funding_abs"]
        funding_prev_abs = features["funding_prev_abs"]
        required_abs = features["required_abs"]
        persistence_bars = int(params.get("persistence_bars", 2))

        sign_now = np.sign(funding)
        sign_prev = np.sign(funding.shift(1))
        flip = ((sign_now != sign_prev) & (sign_now != 0) & (sign_prev != 0)).fillna(False)
        significant = ((funding_abs >= required_abs) & (funding_prev_abs >= required_abs)).fillna(
            False
        )
        if persistence_bars > 1:
            future_same_sign = pd.Series(True, index=funding.index, dtype=bool)
            for step in range(1, persistence_bars):
                future_same_sign &= (np.sign(funding.shift(-step)) == sign_now).fillna(False)
        else:
            future_same_sign = pd.Series(True, index=funding.index, dtype=bool)

        return (flip & significant & future_same_sign).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return features["funding_abs"].fillna(0.0)


__all__ = [
    "FUNDING_EVENT_TYPES",
    "BaseFundingDetector",
    "FundingDetector",
    "FundingExtremeOnsetDetector",
    "FundingFlipDetector",
    "FundingNormalizationDetector",
    "FundingPersistenceDetector",
]
