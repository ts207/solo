from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.base_v2 import BaseDetectorV2
from project.events.detectors.dislocation_base import BasisDetectorV2Base
from project.events.thresholding import dynamic_quantile_floor, rolling_robust_zscore


class PairedSeriesDetectorV2(BaseDetectorV2):
    required_columns = ("timestamp", "close")

    def _pair_close(self, df: pd.DataFrame) -> pd.Series:
        for col in ("pair_close", "close_pair", "component_close", "reference_close"):
            if col in df.columns:
                return pd.to_numeric(df[col], errors="coerce").astype(float)
        return pd.Series(np.nan, index=df.index, dtype=float)

    def _paired_returns(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = pd.to_numeric(df["close"], errors="coerce").astype(float)
        pair_close = self._pair_close(df)
        valid = close.notna() & pair_close.notna() & close.gt(0.0) & pair_close.gt(0.0)
        ret = np.log(close / close.shift(1).replace(0.0, np.nan))
        pair_ret = np.log(pair_close / pair_close.shift(1).replace(0.0, np.nan))
        basis = (ret - pair_ret).where(valid)
        lookback = int(params.get("lookback_window", params.get("lead_lag_window", params.get("regime_window", 2880))))
        min_periods = int(params.get("min_periods", max(24, min(lookback, max(24, lookback // 10)))))
        basis_z = rolling_robust_zscore(basis, window=lookback, min_periods=min_periods, shift=1)
        return {
            "close": close,
            "pair_close": pair_close,
            "pair_valid": valid,
            "ret": ret,
            "pair_ret": pair_ret,
            "basis": basis,
            "basis_z": basis_z,
        }

    def compute_data_quality(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        valid = bool(pd.Series(features.get("pair_valid", pd.Series(False, index=next(iter(features.values())).index))).iloc[idx]) if features else False
        if not valid:
            return "invalid"
        for key in ("basis_z", "basis", "ret", "pair_ret"):
            series = features.get(key)
            if hasattr(series, "iloc") and pd.isna(series.iloc[idx]):
                return "degraded"
        return "ok"

    def compute_confidence(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> float:
        ratio = 0.0
        if "signal_intensity" in features:
            ratio = float(np.nan_to_num(features["signal_intensity"].iloc[idx], nan=0.0))
        elif "basis_z_abs" in features:
            ratio = float(np.nan_to_num(features["basis_z_abs"].iloc[idx], nan=0.0))
        conf = 0.52 + min(0.33, max(0.0, ratio - 1.0) * 0.18)
        if self.compute_data_quality(idx, features, **params) == "degraded":
            conf -= 0.18
        if self.compute_data_quality(idx, features, **params) == "invalid":
            conf = 0.0
        return float(max(0.0, min(1.0, conf)))

    def compute_severity(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        ratio = float(np.nan_to_num(intensity, nan=0.0))
        if ratio >= 2.5:
            return 1.0
        if ratio >= 1.4:
            return 0.75
        return 0.45


class CrossVenueDesyncDetectorV2(BasisDetectorV2Base):
    event_name = "CROSS_VENUE_DESYNC"
    event_type = event_name
    required_columns = ("timestamp", "close_perp", "close_spot")

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        work = df.copy()
        if "close_spot" not in work.columns and "close" in work.columns:
            work["close_spot"] = pd.to_numeric(work["close"], errors="coerce")
        if "close_perp" not in work.columns and "perp_close" in work.columns:
            work["close_perp"] = pd.to_numeric(work["perp_close"], errors="coerce")
        params = dict(params)
        params.setdefault("max_dynamic_threshold", max(float(params.get("threshold", params.get("z_threshold", 3.0))) * 2.5, 6.0))
        features = self._base_features(work, **params)
        persistence_bars = int(params.get("persistence_bars", 2))
        features["persistent_shock"] = features["basis_zscore"].abs().rolling(
            persistence_bars, min_periods=1
        ).max()
        features["signal_intensity"] = (
            features["persistent_shock"] / features["dynamic_threshold"].replace(0.0, np.nan)
        ).replace([np.inf, -np.inf], np.nan)
        return features

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        min_bps = float(params.get("min_basis_bps", 5.0))
        active = (
            features["persistent_shock"].ge(features["dynamic_threshold"]).fillna(False)
            & features["basis_bps"].abs().ge(min_bps).fillna(False)
        ).fillna(False)
        return (active & ~active.shift(1, fill_value=False)).fillna(False)

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return pd.to_numeric(features["signal_intensity"], errors="coerce")

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        return {
            "cluster_id": "information_desync_cross_venue",
            "basis_bps": float(np.nan_to_num(features["basis_bps"].iloc[idx], nan=0.0)),
            "basis_zscore": float(np.nan_to_num(features["basis_zscore"].iloc[idx], nan=0.0)),
            "persistent_shock": float(np.nan_to_num(features["persistent_shock"].iloc[idx], nan=0.0)),
            "dynamic_threshold": float(np.nan_to_num(features["dynamic_threshold"].iloc[idx], nan=0.0)),
        }


class CrossAssetDesyncDetectorV2(PairedSeriesDetectorV2):
    event_name = "CROSS_ASSET_DESYNC_EVENT"
    event_type = event_name

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        features = self._paired_returns(df, **params)
        lookback = int(params.get("lookback_window", 2880))
        basis_z_abs = features["basis_z"].abs()
        dynamic_threshold = dynamic_quantile_floor(
            basis_z_abs,
            window=lookback,
            quantile=float(params.get("threshold_quantile", 0.985)),
            floor=float(params.get("threshold_z", 3.0)),
        )
        active = (basis_z_abs >= dynamic_threshold).fillna(False) & features["pair_valid"].fillna(False)
        onset = (active & ~active.shift(1, fill_value=False)).fillna(False)
        features.update({
            "basis_z_abs": basis_z_abs,
            "dynamic_threshold": dynamic_threshold,
            "signal_intensity": (basis_z_abs / dynamic_threshold.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan),
            "mask": onset,
        })
        return features

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        return {
            "cluster_id": "information_desync_cross_asset",
            "basis": float(np.nan_to_num(features["basis"].iloc[idx], nan=0.0)),
            "basis_z": float(np.nan_to_num(features["basis_z"].iloc[idx], nan=0.0)),
            "dynamic_threshold": float(np.nan_to_num(features["dynamic_threshold"].iloc[idx], nan=0.0)),
        }


class IndexComponentDivergenceDetectorV2(PairedSeriesDetectorV2):
    event_name = "INDEX_COMPONENT_DIVERGENCE"
    event_type = event_name

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        features = self._paired_returns(df, **params)
        lookback = int(params.get("threshold_window", params.get("lookback_window", 2880)))
        basis_z_abs = features["basis_z"].abs()
        ret_abs = features["ret"].abs()
        pair_ret_abs = features["pair_ret"].abs()
        basis_threshold = dynamic_quantile_floor(
            basis_z_abs, window=lookback, quantile=float(params.get("basis_quantile", 0.93)), floor=float(params.get("desync_z_threshold", 2.5))
        )
        ret_threshold = dynamic_quantile_floor(
            (ret_abs + pair_ret_abs).fillna(0.0), window=lookback, quantile=float(params.get("ret_quantile", 0.80)), floor=float(params.get("ret_floor", 0.002))
        )
        mask = (
            basis_z_abs.ge(basis_threshold).fillna(False)
            & (ret_abs + pair_ret_abs).ge(ret_threshold).fillna(False)
            & features["pair_valid"].fillna(False)
        ).fillna(False)
        features.update({
            "basis_z_abs": basis_z_abs,
            "basis_threshold": basis_threshold,
            "ret_combo": (ret_abs + pair_ret_abs),
            "ret_threshold": ret_threshold,
            "signal_intensity": (basis_z_abs / basis_threshold.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan),
            "mask": mask,
        })
        return features

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        return {
            "cluster_id": "information_desync_index_components",
            "basis_z": float(np.nan_to_num(features["basis_z"].iloc[idx], nan=0.0)),
            "basis_threshold": float(np.nan_to_num(features["basis_threshold"].iloc[idx], nan=0.0)),
            "ret_combo": float(np.nan_to_num(features["ret_combo"].iloc[idx], nan=0.0)),
        }


class LeadLagBreakDetectorV2(PairedSeriesDetectorV2):
    event_name = "LEAD_LAG_BREAK"
    event_type = event_name

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        features = self._paired_returns(df, **params)
        lookback = int(params.get("threshold_window", params.get("lookback_window", 2880)))
        basis_step = features["basis"].diff().abs()
        threshold = dynamic_quantile_floor(
            basis_step,
            window=lookback,
            quantile=float(params.get("basis_diff_quantile", 0.99)),
            floor=float(params.get("step_floor", 0.001)),
        )
        mask = basis_step.ge(threshold).fillna(False) & features["pair_valid"].fillna(False)
        features.update({
            "basis_step": basis_step,
            "step_threshold": threshold,
            "signal_intensity": (basis_step / threshold.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan),
            "mask": mask,
        })
        return features

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        return {
            "cluster_id": "information_desync_lead_lag",
            "basis_step": float(np.nan_to_num(features["basis_step"].iloc[idx], nan=0.0)),
            "step_threshold": float(np.nan_to_num(features["step_threshold"].iloc[idx], nan=0.0)),
        }


class CorrelationBreakdownDetectorV2(PairedSeriesDetectorV2):
    event_name = "CORRELATION_BREAKDOWN_EVENT"
    event_type = event_name

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        features = self._paired_returns(df, **params)
        window = int(params.get("regime_window", 288))
        min_periods = int(params.get("min_periods", max(24, window // 4)))
        corr = features["ret"].rolling(window, min_periods=min_periods).corr(features["pair_ret"])
        corr_delta = (corr.shift(1) - corr).clip(lower=0.0)
        delta_z = rolling_robust_zscore(corr_delta, window=window, min_periods=min_periods, shift=1)
        mask = (
            corr.shift(1).ge(float(params.get("min_prior_corr", 0.5))).fillna(False)
            & corr.le(float(params.get("corr_floor", 0.25))).fillna(False)
            & delta_z.abs().ge(float(params.get("transition_z_threshold", 2.5))).fillna(False)
            & features["pair_valid"].fillna(False)
        ).fillna(False)
        features.update({
            "corr": corr,
            "corr_delta": corr_delta,
            "delta_z": delta_z,
            "signal_intensity": delta_z.abs(),
            "mask": mask,
        })
        return features

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        return {
            "cluster_id": "regime_transition_correlation",
            "corr": float(np.nan_to_num(features["corr"].iloc[idx], nan=0.0)),
            "corr_delta": float(np.nan_to_num(features["corr_delta"].iloc[idx], nan=0.0)),
            "delta_z": float(np.nan_to_num(features["delta_z"].iloc[idx], nan=0.0)),
        }


class BetaSpikeDetectorV2(PairedSeriesDetectorV2):
    event_name = "BETA_SPIKE_EVENT"
    event_type = event_name

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        features = self._paired_returns(df, **params)
        window = int(params.get("regime_window", 288))
        min_periods = int(params.get("min_periods", max(24, window // 4)))
        pair_var = features["pair_ret"].rolling(window, min_periods=min_periods).var().replace(0.0, np.nan)
        cov = features["ret"].rolling(window, min_periods=min_periods).cov(features["pair_ret"])
        beta = cov / pair_var
        beta_z = rolling_robust_zscore(beta, window=window, min_periods=min_periods, shift=1)
        rv_96 = pd.to_numeric(df.get("rv_96", pd.Series(0.0, index=df.index)), errors="coerce").fillna(0.0)
        rv_threshold = dynamic_quantile_floor(rv_96, window=max(window, 96), quantile=float(params.get("rv_quantile", 0.70)), floor=float(params.get("rv_floor", 0.0)))
        mask = (
            beta_z.abs().ge(float(params.get("transition_z_threshold", 2.5))).fillna(False)
            & rv_96.ge(rv_threshold).fillna(False)
            & features["pair_valid"].fillna(False)
        ).fillna(False)
        features.update({
            "beta": beta,
            "beta_z": beta_z,
            "rv_96": rv_96,
            "rv_threshold": rv_threshold,
            "signal_intensity": beta_z.abs(),
            "mask": mask,
        })
        return features

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        return {
            "cluster_id": "regime_transition_beta",
            "beta": float(np.nan_to_num(features["beta"].iloc[idx], nan=0.0)),
            "beta_z": float(np.nan_to_num(features["beta_z"].iloc[idx], nan=0.0)),
            "rv_96": float(np.nan_to_num(features["rv_96"].iloc[idx], nan=0.0)),
        }
