from __future__ import annotations

from typing import Any, Mapping

import numpy as np
import pandas as pd

from project.events.detectors.base_v2 import BaseDetectorV2
from project.events.thresholding import dynamic_quantile_floor, rolling_robust_zscore, rolling_vol_regime_factor
from project.features.context_guards import state_at_least


class BasisDetectorV2Base(BaseDetectorV2):
    required_columns = ("timestamp", "close_perp", "close_spot")

    def _base_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close_perp = pd.to_numeric(df["close_perp"], errors="coerce")
        close_spot = pd.to_numeric(df["close_spot"], errors="coerce").replace(0.0, np.nan)
        basis_bps = (close_perp - close_spot) / close_spot * 10000.0
        lookback = int(params.get("lookback_window", params.get("basis_lookback", 2880)))
        min_periods = int(params.get("min_periods", min(lookback, max(1, max(24, lookback // 10)))))
        basis_z = rolling_robust_zscore(basis_bps, window=lookback, min_periods=min_periods, shift=1)
        logret = np.log(close_perp / close_perp.shift(1).replace(0.0, np.nan))
        rv_proxy = logret.rolling(96, min_periods=24).std()
        vol_factor = rolling_vol_regime_factor(rv_proxy.fillna(0.0), window=lookback)
        floor = float(params.get("threshold_floor", params.get("z_threshold", params.get("threshold", 3.0))))
        dynamic_threshold = dynamic_quantile_floor(
            basis_z.abs(),
            window=lookback,
            quantile=float(params.get("threshold_quantile", 0.985)),
            floor=floor * vol_factor.clip(0.8, 1.6) if bool(params.get("vol_scaled_threshold", True)) else floor,
        )
        max_dynamic_threshold = float(params.get("max_dynamic_threshold", max(floor * 4.0, 10.0)))
        dynamic_threshold = dynamic_threshold.clip(lower=floor, upper=max_dynamic_threshold)
        return {
            "close_perp": close_perp,
            "close_spot": close_spot,
            "basis_bps": basis_bps,
            "basis_zscore": basis_z,
            "dynamic_threshold": dynamic_threshold,
            "rv_proxy": rv_proxy,
            "vol_factor": vol_factor,
        }

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (features["basis_zscore"].abs() / features["dynamic_threshold"].replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)

    def compute_severity(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        if intensity >= 2.5:
            return 1.0
        if intensity >= 1.6:
            return 0.75
        return 0.45

    def compute_confidence(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> float:
        ratio = float(np.nan_to_num((features["basis_zscore"].abs() / features["dynamic_threshold"].replace(0.0, np.nan)).iloc[idx], nan=0.0))
        conf = 0.55 + min(0.25, max(0.0, ratio - 1.0) * 0.2)
        if abs(float(np.nan_to_num(features["basis_bps"].iloc[idx], nan=0.0))) >= float(params.get("min_basis_bps", 5.0)) * 2.0:
            conf += 0.10
        return float(max(0.0, min(1.0, conf)))

    def compute_data_quality(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        if pd.isna(features["basis_zscore"].iloc[idx]) or pd.isna(features["basis_bps"].iloc[idx]):
            return "degraded"
        return "ok"

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        return {
            "cluster_id": "basis_funding_dislocation",
            "basis_bps": float(np.nan_to_num(features["basis_bps"].iloc[idx], nan=0.0)),
            "basis_zscore": float(np.nan_to_num(features["basis_zscore"].iloc[idx], nan=0.0)),
            "dynamic_threshold": float(np.nan_to_num(features["dynamic_threshold"].iloc[idx], nan=0.0)),
        }


class BasisDislocationDetectorV2(BasisDetectorV2Base):
    event_name = "BASIS_DISLOC"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        return self._base_features(df, **params)

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (
            features["basis_zscore"].abs().ge(features["dynamic_threshold"]).fillna(False)
            & features["basis_bps"].abs().ge(float(params.get("min_basis_bps", 5.0))).fillna(False)
        ).fillna(False)


class FndDislocDetectorV2(BasisDetectorV2Base):
    event_name = "FND_DISLOC"
    required_columns = ("timestamp", "close_perp", "close_spot", "funding_rate_scaled")

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        features = self._base_features(df, **params)
        funding = pd.to_numeric(df["funding_rate_scaled"], errors="coerce")
        lookback = int(params.get("lookback_window", 2880))
        funding_abs = funding.abs() * 10000.0
        funding_threshold = dynamic_quantile_floor(
            funding_abs, window=lookback, quantile=float(params.get("funding_quantile", 0.90)), floor=float(params.get("funding_floor_bps", 2.0))
        )
        canonical_funding_extreme = state_at_least(
            df,
            "ms_funding_state",
            2.0,
            default_if_absent=True,
            min_confidence=float(params.get("context_min_confidence", 0.55)),
            max_entropy=float(params.get("context_max_entropy", 0.90)),
        )
        features.update({
            "funding_rate_scaled": funding,
            "funding_abs_bps": funding_abs,
            "funding_threshold": funding_threshold,
            "canonical_funding_extreme": canonical_funding_extreme,
        })
        return features

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        base = BasisDislocationDetectorV2.compute_raw_mask(self, df, features=features, **params)
        funding_ok = features["funding_abs_bps"].ge(features["funding_threshold"]).fillna(False)
        sign_align = (np.sign(features["basis_bps"].fillna(0.0)) == np.sign(features["funding_rate_scaled"].fillna(0.0))).fillna(False)
        return (base & funding_ok & sign_align & features["canonical_funding_extreme"].fillna(False)).fillna(False)

    def compute_confidence(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> float:
        conf = super().compute_confidence(idx, features, **params)
        if float(np.nan_to_num(features["funding_abs_bps"].iloc[idx], nan=0.0)) >= float(np.nan_to_num(features["funding_threshold"].iloc[idx], nan=0.0)) * 1.2:
            conf += 0.1
        return float(max(0.0, min(1.0, conf)))

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        meta = dict(super().compute_metadata(idx, features, **params))
        meta.update({
            "funding_bps": float(np.nan_to_num(features["funding_abs_bps"].iloc[idx], nan=0.0)),
            "funding_threshold": float(np.nan_to_num(features["funding_threshold"].iloc[idx], nan=0.0)),
        })
        return meta


class SpotPerpBasisShockDetectorV2(BasisDetectorV2Base):
    event_name = "SPOT_PERP_BASIS_SHOCK"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        features = self._base_features(df, **params)
        lookback = int(params.get("lookback_window", 2880))
        shock_change = features["basis_zscore"].diff().abs()
        shock_threshold = dynamic_quantile_floor(
            shock_change,
            window=lookback,
            quantile=float(params.get("shock_change_quantile", 0.90)),
            floor=float(params.get("shock_change_floor", 0.75)),
        )
        features.update({"shock_change": shock_change, "shock_threshold": shock_threshold})
        return features

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        base = BasisDislocationDetectorV2.compute_raw_mask(self, df, features=features, **params)
        return (base & features["shock_change"].ge(features["shock_threshold"]).fillna(False)).fillna(False)

    def compute_confidence(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> float:
        conf = super().compute_confidence(idx, features, **params)
        if float(np.nan_to_num(features["shock_change"].iloc[idx], nan=0.0)) >= float(np.nan_to_num(features["shock_threshold"].iloc[idx], nan=0.0)) * 1.2:
            conf += 0.1
        return float(max(0.0, min(1.0, conf)))

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        meta = dict(super().compute_metadata(idx, features, **params))
        meta.update({
            "shock_change": float(np.nan_to_num(features["shock_change"].iloc[idx], nan=0.0)),
            "shock_threshold": float(np.nan_to_num(features["shock_threshold"].iloc[idx], nan=0.0)),
        })
        return meta
