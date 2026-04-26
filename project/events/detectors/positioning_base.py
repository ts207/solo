from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.base_v2 import BaseDetectorV2
from project.events.detectors.funding_support import (
    prepare_funding_normalization_features,
    prepare_funding_persistence_features,
    run_length,
)
from project.events.thresholding import percentile_rank_historical
from project.features.context_guards import state_at_least, state_at_most


class PositioningDetectorV2Base(BaseDetectorV2):
    def compute_severity(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        if intensity >= 1.8:
            return 1.0
        if intensity >= 1.15:
            return 0.75
        return 0.45

    def compute_confidence(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> float:
        intensity = float(np.nan_to_num(features.get("signal_intensity", pd.Series(0.0, index=next(iter(features.values())).index)).iloc[idx], nan=0.0)) if features else 0.0
        return float(max(0.0, min(1.0, 0.5 + min(0.35, intensity * 0.25))))

    def compute_data_quality(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        primary = next(iter(features.values()))
        if pd.isna(primary.iloc[idx]):
            return "degraded"
        return "ok"


class BaseFundingDetectorV2(PositioningDetectorV2Base):
    required_columns = ("timestamp", "funding_abs_pct", "funding_abs")

    def _signed_funding(self, df: pd.DataFrame) -> pd.Series:
        if "funding_rate_scaled" in df.columns:
            return pd.to_numeric(df["funding_rate_scaled"], errors="coerce").astype(float)
        return pd.Series(0.0, index=df.index, dtype=float)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        f_pct = float(np.nan_to_num(features["funding_abs_pct"].iloc[idx], nan=0.0))
        return {
            "cluster_id": "positioning_extremes",
            "funding_abs_pct": f_pct,
            "funding_abs": float(np.nan_to_num(features["funding_abs"].iloc[idx], nan=0.0)),
            "funding_signed": float(np.nan_to_num(features.get("funding_signed", pd.Series(0.0, index=features["funding_abs_pct"].index)).iloc[idx], nan=0.0)),
            "fr_sign": 1.0 if float(np.nan_to_num(features.get("funding_signed", pd.Series(0.0, index=features["funding_abs_pct"].index)).iloc[idx], nan=0.0)) > 0.0 else -1.0 if float(np.nan_to_num(features.get("funding_signed", pd.Series(0.0, index=features["funding_abs_pct"].index)).iloc[idx], nan=0.0)) < 0.0 else 0.0,
            "fr_magnitude": f_pct if f_pct > 1.0 else f_pct * 10000.0,
        }

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        if "signal_intensity" in features:
            return pd.to_numeric(features["signal_intensity"], errors="coerce")
        return pd.to_numeric(features["funding_abs_pct"], errors="coerce") / 100.0


class FundingExtremeOnsetDetectorV2(BaseFundingDetectorV2):
    event_name = "FUNDING_EXTREME_ONSET"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        f_pct = pd.to_numeric(df["funding_abs_pct"], errors="coerce").astype(float)
        f_abs = pd.to_numeric(df["funding_abs"], errors="coerce").astype(float)
        funding_signed = self._signed_funding(df)
        extreme_pct = float(params.get("extreme_pct", 95.0))
        accel_pct = float(params.get("accel_pct", 90.0))
        accel_lookback = int(params.get("accel_lookback", 12))
        threshold_window = int(params.get("threshold_window", 2880))
        accel = (f_abs - f_abs.shift(accel_lookback)).clip(lower=0.0)
        accel_rank = percentile_rank_historical(accel, window=threshold_window, min_periods=max(24, accel_lookback)).fillna(0.0)
        extreme_flag = (f_pct >= extreme_pct).fillna(False)
        persistence_run = run_length(extreme_flag)
        persist_flag = persistence_run >= int(params.get("persistence_bars", 1))
        qualified = (extreme_flag & (accel_rank >= accel_pct).fillna(False) & persist_flag.fillna(False)).fillna(False)
        onset = (qualified & ~qualified.shift(1, fill_value=False)).fillna(False)
        return {
            "funding_abs_pct": f_pct,
            "funding_abs": f_abs,
            "funding_signed": funding_signed,
            "accel_rank": accel_rank,
            "signal_intensity": ((f_pct / 100.0) + (accel_rank / 100.0)).clip(lower=0.0),
            "mask": onset,
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)


class FundingPersistenceDetectorV2(BaseFundingDetectorV2):
    event_name = "FUNDING_PERSISTENCE_TRIGGER"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        return prepare_funding_persistence_features(
            df,
            self._signed_funding(df),
            {
                "accel_pct": 90.0,
                "persistence_pct": 85.0,
                "normalization_pct": 50.0,
                "normalization_lookback": 288,
                "min_prior_extreme_abs": 0.0004,
            },
            params,
        )

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)


class FundingNormalizationDetectorV2(BaseFundingDetectorV2):
    event_name = "FUNDING_NORMALIZATION_TRIGGER"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        return prepare_funding_normalization_features(
            df,
            self._signed_funding(df),
            {
                "extreme_pct": 95.0,
                "accel_pct": 90.0,
                "persistence_pct": 85.0,
                "normalization_pct": 50.0,
                "normalization_lookback": 288,
                "min_prior_extreme_abs": 0.0004,
            },
            params,
        )

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)


class FundingFlipDetectorV2(BaseFundingDetectorV2):
    event_name = "FUNDING_FLIP"
    required_columns = ("timestamp", "funding_rate_scaled")

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        funding = pd.to_numeric(df["funding_rate_scaled"], errors="coerce").astype(float)
        funding_abs = funding.abs()
        required_abs = funding_abs.rolling(int(params.get("threshold_window", 2880)), min_periods=288).quantile(float(params.get("min_magnitude_quantile", 0.75))).shift(1).fillna(float(params.get("min_flip_abs", 2.5e-4)))
        sign_now = np.sign(funding)
        sign_prev = np.sign(funding.shift(1))
        flip = ((sign_now != sign_prev) & (sign_now != 0) & (sign_prev != 0)).fillna(False)
        significant = ((funding_abs >= required_abs) & (funding_abs.shift(1) >= required_abs)).fillna(False)
        persistence_bars = int(params.get("persistence_bars", 2))
        future_same_sign = pd.Series(True, index=df.index, dtype=bool)
        for step in range(1, persistence_bars):
            future_same_sign &= (np.sign(funding.shift(-step)) == sign_now).fillna(False)
        mask = (flip & significant & future_same_sign).fillna(False)
        return {
            "funding_abs_pct": (funding_abs * 10000.0).fillna(0.0),
            "funding_abs": funding_abs,
            "funding_signed": funding,
            "required_abs": required_abs,
            "signal_intensity": (funding_abs / required_abs.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan),
            "mask": mask,
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)


class BaseOIDetectorV2(PositioningDetectorV2Base):
    required_columns = ("timestamp", "oi_notional", "close", "ms_oi_state", "ms_oi_confidence", "ms_oi_entropy")

    def _compute_oi(self, df: pd.DataFrame, **params: Any) -> tuple[pd.Series, pd.Series, pd.Series]:
        window = int(params.get("oi_window", 96))
        min_periods = int(params.get("min_periods", max(24, window // 4)))
        oi = pd.to_numeric(df["oi_notional"], errors="coerce").replace(0.0, np.nan).astype(float)
        oi_log_delta = np.log(oi).diff()
        baseline = oi_log_delta.shift(1)
        mean = baseline.rolling(window=window, min_periods=min_periods).mean()
        std = baseline.rolling(window=window, min_periods=min_periods).std()
        oi_z = (oi_log_delta - mean) / std.where(std > 0.0, 1e-12)
        close_ret = pd.to_numeric(df["close"], errors="coerce").astype(float).pct_change(periods=1)
        return oi_z, close_ret, oi.pct_change(periods=1)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        return {
            "cluster_id": "positioning_oi",
            "oi_z": float(np.nan_to_num(features["oi_z"].iloc[idx], nan=0.0)),
            "oi_pct_change": float(np.nan_to_num(features["oi_pct_change"].iloc[idx], nan=0.0)),
            "close_ret": float(np.nan_to_num(features["close_ret"].iloc[idx], nan=0.0)),
        }

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return pd.to_numeric(features.get("signal_intensity", features["oi_z"].abs()), errors="coerce")


class OISpikePositiveDetectorV2(BaseOIDetectorV2):
    event_name = "OI_SPIKE_POSITIVE"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        oi_z, close_ret, oi_pct_change = self._compute_oi(df, **params)
        mask = (oi_z >= float(params.get("spike_z_th", 2.0))) & (close_ret > 0)
        context = state_at_least(df, "ms_oi_state", 2.0, default_if_absent=True, min_confidence=float(params.get("context_min_confidence", 0.55)), max_entropy=float(params.get("context_max_entropy", 0.90)))
        return {
            "oi_z": oi_z,
            "close_ret": close_ret,
            "oi_pct_change": oi_pct_change,
            "signal_intensity": oi_z.abs(),
            "mask": (mask & context.fillna(False)).fillna(False),
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)


class OISpikeNegativeDetectorV2(BaseOIDetectorV2):
    event_name = "OI_SPIKE_NEGATIVE"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        oi_z, close_ret, oi_pct_change = self._compute_oi(df, **params)
        mask = (oi_z >= float(params.get("spike_z_th", 2.5))) & (close_ret < 0)
        context = state_at_least(df, "ms_oi_state", 2.0, default_if_absent=True, min_confidence=float(params.get("context_min_confidence", 0.55)), max_entropy=float(params.get("context_max_entropy", 0.90)))
        return {
            "oi_z": oi_z,
            "close_ret": close_ret,
            "oi_pct_change": oi_pct_change,
            "signal_intensity": oi_z.abs(),
            "mask": (mask & context.fillna(False)).fillna(False),
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)


class OIFlushDetectorV2(BaseOIDetectorV2):
    event_name = "OI_FLUSH"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        oi_z, close_ret, oi_pct_change = self._compute_oi(df, **params)
        flush_pct_th = float(params.get("flush_pct_th", -0.005))
        mask = oi_pct_change <= flush_pct_th
        context = state_at_most(df, "ms_oi_state", 0.0, default_if_absent=True, min_confidence=float(params.get("context_min_confidence", 0.55)), max_entropy=float(params.get("context_max_entropy", 0.90)))
        return {
            "oi_z": oi_z,
            "close_ret": close_ret,
            "oi_pct_change": oi_pct_change,
            "signal_intensity": oi_pct_change.abs() * 100.0,
            "mask": (mask & context.fillna(False)).fillna(False),
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)
