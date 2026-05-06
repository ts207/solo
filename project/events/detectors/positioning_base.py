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
from project.events.thresholding import rolling_percentile_rank
from project.features.context_guards import state_at_least, state_at_most
from project.events.polarity import PolaritySemantics


class PositioningDetectorV2Base(BaseDetectorV2):
    promotion_eligible = False
    planning_default = False
    runtime_default = False

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

    def compute_polarity_semantics(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        return PolaritySemantics.FUNDING_CROWDING_SIDE.value

    def compute_event_side(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> str:
        del intensity, params
        signed = float(np.nan_to_num(features.get("funding_signed", pd.Series(0.0, index=features["funding_abs_pct"].index)).iloc[idx], nan=0.0))
        return "bullish" if signed > 0.0 else "bearish" if signed < 0.0 else "neutral"

    def compute_polarity_source(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> str:
        del idx, intensity, features, params
        return "funding_signed"

    def compute_magnitude(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        del intensity, params
        return abs(float(np.nan_to_num(features["funding_abs"].iloc[idx], nan=0.0)))

    def compute_magnitude_source(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> str:
        del idx, intensity, features, params
        return "funding_abs"

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
        accel_rank = rolling_percentile_rank(
            accel,
            window=threshold_window,
            min_periods=max(24, accel_lookback),
            shift=0,
            scale=100.0,
        ).fillna(0.0)
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

    @staticmethod
    def _causal_flip_confirmation(
        funding: pd.Series,
        required_abs: pd.Series,
        *,
        persistence_bars: int,
    ) -> tuple[pd.Series, pd.Series]:
        """Return a point-in-time funding-flip mask and origin index.

        Persistence is confirmed only after the required bars have elapsed.
        The event is emitted on the confirmation bar, not the original flip
        bar, so the detector does not need to read future rows.
        """
        persistence_bars = max(1, int(persistence_bars))
        funding_abs = funding.abs()
        sign_now = pd.Series(np.sign(funding), index=funding.index)
        sign_prev = pd.Series(np.sign(funding.shift(1)), index=funding.index)
        flip = ((sign_now != sign_prev) & (sign_now != 0) & (sign_prev != 0)).fillna(False)
        significant = ((funding_abs >= required_abs) & (funding_abs.shift(1) >= required_abs)).fillna(False)
        flip_candidate = (flip & significant).fillna(False)

        lag = persistence_bars - 1
        if lag <= 0:
            origin_idx = pd.Series(np.where(flip_candidate, np.arange(len(funding)), np.nan), index=funding.index)
            return flip_candidate, origin_idx

        origin_candidate = flip_candidate.shift(lag, fill_value=False).astype(bool)
        origin_sign = sign_now.shift(lag)
        persisted = origin_candidate.copy()
        for offset in range(0, lag + 1):
            persisted &= (sign_now.shift(offset) == origin_sign).fillna(False)
        mask = persisted.fillna(False)
        origin_idx = pd.Series(np.nan, index=funding.index, dtype=float)
        origin_idx.loc[mask] = np.arange(len(funding))[mask.to_numpy()] - lag
        return mask, origin_idx

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        funding = pd.to_numeric(df["funding_rate_scaled"], errors="coerce").astype(float)
        funding_abs = funding.abs()
        threshold_window = int(params.get("threshold_window", 2880))
        min_periods = min(threshold_window, int(params.get("min_periods", 288)))
        required_abs = funding_abs.rolling(threshold_window, min_periods=min_periods).quantile(float(params.get("min_magnitude_quantile", 0.75))).shift(1).fillna(float(params.get("min_flip_abs", 2.5e-4)))
        persistence_bars = max(1, int(params.get("persistence_bars", 2)))
        mask, origin_idx = self._causal_flip_confirmation(
            funding,
            required_abs,
            persistence_bars=persistence_bars,
        )
        confirmation_lag = pd.Series(0.0, index=df.index, dtype=float)
        confirmation_lag.loc[mask] = float(persistence_bars - 1)
        return {
            "funding_abs_pct": (funding_abs * 10000.0).fillna(0.0),
            "funding_abs": funding_abs,
            "funding_signed": funding,
            "required_abs": required_abs,
            "signal_intensity": (funding_abs / required_abs.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan),
            "confirmation_lag_bars": confirmation_lag,
            "flip_origin_idx": origin_idx,
            "mask": mask,
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        meta = dict(super().compute_metadata(idx, features, **params))
        meta["cluster_id"] = "funding_flip"
        meta["event_semantics"] = "causal_persistence_confirmed_flip"
        lag_series = features.get("confirmation_lag_bars", pd.Series(0.0, index=features["funding_abs_pct"].index))
        meta["confirmation_lag_bars"] = int(float(np.nan_to_num(lag_series.iloc[idx], nan=0.0)))
        origin = features.get("flip_origin_idx")
        if origin is not None and pd.notna(origin.iloc[idx]):
            meta["flip_origin_idx"] = int(origin.iloc[idx])
        return meta


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
        std_floor = float(params.get("oi_std_floor", 1e-6))
        std_safe = std.where(std.notna(), np.nan).clip(lower=std_floor)
        oi_z = ((oi_log_delta - mean) / std_safe).replace([np.inf, -np.inf], np.nan).clip(-20.0, 20.0)
        close_ret = pd.to_numeric(df["close"], errors="coerce").astype(float).pct_change(periods=1)
        return oi_z, close_ret, oi.pct_change(periods=1)

    def compute_polarity_semantics(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        return PolaritySemantics.PRICE_OI_QUADRANT.value

    def _price_oi_quadrant(self, idx: int, features: Mapping[str, pd.Series]) -> str:
        price = float(np.nan_to_num(features["close_ret"].iloc[idx], nan=0.0))
        oi_delta = float(np.nan_to_num(features["oi_pct_change"].iloc[idx], nan=0.0))
        if price > 0.0 and oi_delta > 0.0:
            return "price_up_oi_up"
        if price > 0.0 and oi_delta < 0.0:
            return "price_up_oi_down"
        if price < 0.0 and oi_delta > 0.0:
            return "price_down_oi_up"
        if price < 0.0 and oi_delta < 0.0:
            return "price_down_oi_down"
        return "price_oi_flat"

    def compute_event_side(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> str:
        del intensity, params
        quadrant = self._price_oi_quadrant(idx, features)
        if quadrant.startswith("price_up"):
            return "bullish"
        if quadrant.startswith("price_down"):
            return "bearish"
        return "neutral"

    def compute_polarity_source(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> str:
        del idx, intensity, features, params
        return "price_oi_quadrant"

    def compute_magnitude(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        del intensity, params
        oi_z = abs(float(np.nan_to_num(features["oi_z"].iloc[idx], nan=0.0)))
        oi_pct = abs(float(np.nan_to_num(features["oi_pct_change"].iloc[idx], nan=0.0))) * 100.0
        return max(oi_z, oi_pct)

    def compute_magnitude_source(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> str:
        del idx, intensity, features, params
        return "max(abs(oi_z),abs(oi_pct_change)*100)"

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        return {
            "cluster_id": "positioning_oi",
            "oi_z": float(np.nan_to_num(features["oi_z"].iloc[idx], nan=0.0)),
            "oi_pct_change": float(np.nan_to_num(features["oi_pct_change"].iloc[idx], nan=0.0)),
            "close_ret": float(np.nan_to_num(features["close_ret"].iloc[idx], nan=0.0)),
            "price_oi_quadrant": self._price_oi_quadrant(idx, features),
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
    promotion_eligible = False
    planning_default = True
    runtime_default = True
    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        oi_z, close_ret, oi_pct_change = self._compute_oi(df, **params)
        mask = (oi_z >= float(params.get("spike_z_th", 2.5))) & (close_ret < 0)
        context = state_at_least(df, "ms_oi_state", 2.0, default_if_absent=True, min_confidence=float(params.get("context_min_confidence", 0.55)), max_entropy=float(params.get("context_max_entropy", 0.90)))
        funding = pd.to_numeric(df.get("funding_rate_scaled", pd.Series(0.0, index=df.index)), errors="coerce").fillna(0.0)
        liquidity_vacuum = pd.Series(df.get("liquidity_vacuum", False), index=df.index).fillna(False).astype(bool)
        failed_continuation = pd.Series(df.get("failed_continuation", False), index=df.index).fillna(False).astype(bool)
        vol_shock = pd.Series(df.get("vol_shock", False), index=df.index).fillna(False).astype(bool)
        paired = (funding.abs() > float(params.get("funding_context_abs_min", 0.0001))) | failed_continuation | vol_shock
        return {"oi_z": oi_z, "close_ret": close_ret, "oi_pct_change": oi_pct_change, "funding_signed": funding, "liquidity_vacuum": liquidity_vacuum, "failed_continuation": failed_continuation, "vol_shock": vol_shock, "paired_context_present": paired.fillna(False), "signal_intensity": oi_z.abs(), "mask": (mask & context.fillna(False)).fillna(False)}
    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)
    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        base = dict(super().compute_metadata(idx, features, **params))
        funding = float(np.nan_to_num(features.get("funding_signed", pd.Series(0.0, index=features["oi_z"].index)).iloc[idx], nan=0.0))
        liquidity_vacuum = bool(features.get("liquidity_vacuum", pd.Series(False, index=features["oi_z"].index)).iloc[idx])
        failed_continuation = bool(features.get("failed_continuation", pd.Series(False, index=features["oi_z"].index)).iloc[idx])
        vol_shock = bool(features.get("vol_shock", pd.Series(False, index=features["oi_z"].index)).iloc[idx])
        paired = bool(features.get("paired_context_present", pd.Series(False, index=features["oi_z"].index)).iloc[idx])
        if liquidity_vacuum: subtype = "avoid_size_down"
        elif failed_continuation: subtype = "squeeze_risk"
        elif funding > 0.0: subtype = "long_crowding_stress"
        elif funding < 0.0: subtype = "short_build_continuation"
        elif vol_shock: subtype = "ambiguous_positioning_stress"
        else: subtype = "ambiguous_positioning_stress"
        base.update({"cluster_id":"positioning_oi","positioning_subtype":subtype,"standalone_trade_eligible":False,"paired_context_present":paired,"liquidity_vacuum_active":liquidity_vacuum,"failed_continuation_active":failed_continuation,"vol_shock_active":vol_shock,"funding_signed":funding,"trade_eligible":False})
        return base
    def compute_anchor_role(self, idx: int, features: Mapping[str, pd.Series], semantics: str, **params: Any) -> str:
        return "confirmation_context"

class OIExpansionStressDetectorV2(BaseOIDetectorV2):
    event_name = "OI_EXPANSION_STRESS"
    promotion_eligible = False
    planning_default = True
    runtime_default = True
    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        oi_z, close_ret, oi_pct_change = self._compute_oi(df, **params)
        min_price_move = float(params.get("price_move_threshold_bps", 25.0)) / 10000.0
        mask = (oi_z >= float(params.get("spike_z_th", 2.0))) & (close_ret.abs() >= min_price_move)
        context = state_at_least(df, "ms_oi_state", 2.0, default_if_absent=True, min_confidence=float(params.get("context_min_confidence", 0.55)), max_entropy=float(params.get("context_max_entropy", 0.90)))
        funding = pd.to_numeric(df.get("funding_rate_scaled", pd.Series(0.0, index=df.index)), errors="coerce").fillna(0.0)
        liquidity_vacuum = pd.Series(df.get("liquidity_vacuum", False), index=df.index).fillna(False).astype(bool)
        failed_continuation = pd.Series(df.get("failed_continuation", False), index=df.index).fillna(False).astype(bool)
        vol_shock = pd.Series(df.get("vol_shock", False), index=df.index).fillna(False).astype(bool)
        paired = (funding.abs() > float(params.get("funding_context_abs_min", 0.0001))) | failed_continuation | vol_shock
        return {"oi_z":oi_z,"close_ret":close_ret,"oi_pct_change":oi_pct_change,"funding_signed":funding,"liquidity_vacuum":liquidity_vacuum,"failed_continuation":failed_continuation,"vol_shock":vol_shock,"paired_context_present":paired.fillna(False),"signal_intensity":oi_z.abs()+(close_ret.abs()*100.0),"mask":(mask & context.fillna(False)).fillna(False)}
    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)
    def compute_event_side(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> str:
        price=float(np.nan_to_num(features["close_ret"].iloc[idx], nan=0.0)); return "bullish" if price > 0.0 else "bearish" if price < 0.0 else "neutral"
    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        base=dict(super().compute_metadata(idx, features, **params)); price=float(np.nan_to_num(features["close_ret"].iloc[idx], nan=0.0)); oi_delta=float(np.nan_to_num(features["oi_pct_change"].iloc[idx], nan=0.0)); funding=float(np.nan_to_num(features.get("funding_signed",pd.Series(0.0,index=features["oi_z"].index)).iloc[idx], nan=0.0)); liquidity_vacuum=bool(features.get("liquidity_vacuum",pd.Series(False,index=features["oi_z"].index)).iloc[idx]); failed_continuation=bool(features.get("failed_continuation",pd.Series(False,index=features["oi_z"].index)).iloc[idx]); vol_shock=bool(features.get("vol_shock",pd.Series(False,index=features["oi_z"].index)).iloc[idx]); paired=bool(features.get("paired_context_present",pd.Series(False,index=features["oi_z"].index)).iloc[idx])
        if liquidity_vacuum: subtype="avoid_size_down"
        elif price < 0.0 and oi_delta > 0.0 and funding < 0.0: subtype="short_build_continuation"
        elif price < 0.0 and oi_delta > 0.0 and funding > 0.0: subtype="long_crowding_stress"
        elif price > 0.0 and oi_delta > 0.0 and funding < 0.0: subtype="short_squeeze_risk"
        elif price > 0.0 and oi_delta > 0.0 and funding > 0.0: subtype="long_build_continuation"
        elif failed_continuation: subtype="squeeze_risk"
        else: subtype="ambiguous_positioning_stress"
        base.update({"cluster_id":"positioning_oi","positioning_subtype":subtype,"standalone_trade_eligible":False,"paired_context_present":paired,"liquidity_vacuum_active":liquidity_vacuum,"failed_continuation_active":failed_continuation,"vol_shock_active":vol_shock,"funding_signed":funding,"trade_eligible":False})
        return base
    def compute_anchor_role(self, idx: int, features: Mapping[str, pd.Series], semantics: str, **params: Any) -> str:
        return "confirmation_context"

class OIFlushDetectorV2(BaseOIDetectorV2):
    event_name = "OI_FLUSH"
    promotion_eligible = False
    planning_default = True
    runtime_default = True
    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        oi_z, close_ret, oi_pct_change = self._compute_oi(df, **params)
        flush_pct_th = -abs(float(params.get("flush_pct_th", 0.005)))
        min_move = float(params.get("price_move_threshold_bps", 25.0)) / 10000.0
        core_flush = (oi_pct_change <= flush_pct_th) & (close_ret.abs() >= min_move)
        context_confirms = state_at_most(df, "ms_oi_state", 0.0, default_if_absent=True, min_confidence=float(params.get("context_min_confidence", 0.55)), max_entropy=float(params.get("context_max_entropy", 0.90)))
        failed_continuation=pd.Series(df.get("failed_continuation",False),index=df.index).fillna(False).astype(bool); liquidity_recovery=pd.Series(df.get("liquidity_vacuum_recovery",False),index=df.index).fillna(False).astype(bool); vol_shock=pd.Series(df.get("vol_shock",False),index=df.index).fillna(False).astype(bool); paired=failed_continuation|liquidity_recovery|vol_shock
        return {"oi_z":oi_z,"close_ret":close_ret,"oi_pct_change":oi_pct_change,"ms_oi_state_confirms":context_confirms.fillna(False),"failed_continuation":failed_continuation,"liquidity_vacuum_recovery":liquidity_recovery,"vol_shock":vol_shock,"paired_context_present":paired.fillna(False),"signal_intensity":oi_pct_change.abs()*100.0+close_ret.abs()*100.0,"mask":core_flush.fillna(False)}
    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["mask"].fillna(False)
    def compute_event_side(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> str:
        price=float(np.nan_to_num(features["close_ret"].iloc[idx], nan=0.0)); return "bullish" if price < 0.0 else "bearish" if price > 0.0 else "neutral"
    def compute_data_quality(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        if not bool(features.get("ms_oi_state_confirms", pd.Series(False, index=features["oi_z"].index)).iloc[idx]):
            return "degraded"
        return "ok"
    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        base=dict(super().compute_metadata(idx, features, **params)); price=float(np.nan_to_num(features["close_ret"].iloc[idx], nan=0.0)); paired=bool(features.get("paired_context_present",pd.Series(False,index=features["oi_z"].index)).iloc[idx]); context_confirms=bool(features.get("ms_oi_state_confirms",pd.Series(False,index=features["oi_z"].index)).iloc[idx]); subtype="long_flush_candidate" if price < 0.0 else "short_flush_candidate" if price > 0.0 else "ambiguous_oi_flush"; base.update({"cluster_id":"positioning_oi_flush","flush_subtype":subtype,"standalone_trade_eligible":False,"paired_context_present":paired,"ms_oi_state_confirms":context_confirms,"trade_eligible":bool(paired)}) ; return base

class FundingPosExtremeOnsetDetectorV2(FundingExtremeOnsetDetectorV2):
    event_name = "FUNDING_POS_EXTREME_ONSET"
    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (super().compute_raw_mask(df, features=features, **params) & (features["funding_signed"] > 0.0)).fillna(False)


class FundingNegExtremeOnsetDetectorV2(FundingExtremeOnsetDetectorV2):
    event_name = "FUNDING_NEG_EXTREME_ONSET"
    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (super().compute_raw_mask(df, features=features, **params) & (features["funding_signed"] < 0.0)).fillna(False)


class FundingPosPersistenceDetectorV2(FundingPersistenceDetectorV2):
    event_name = "FUNDING_POS_PERSISTENCE"
    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (super().compute_raw_mask(df, features=features, **params) & (features["funding_signed"] > 0.0)).fillna(False)


class FundingNegPersistenceDetectorV2(FundingPersistenceDetectorV2):
    event_name = "FUNDING_NEG_PERSISTENCE"
    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (super().compute_raw_mask(df, features=features, **params) & (features["funding_signed"] < 0.0)).fillna(False)


class FundingPosNormalizationDetectorV2(FundingNormalizationDetectorV2):
    event_name = "FUNDING_POS_NORMALIZATION"
    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (super().compute_raw_mask(df, features=features, **params) & (features["funding_signed"] > 0.0)).fillna(False)


class FundingNegNormalizationDetectorV2(FundingNormalizationDetectorV2):
    event_name = "FUNDING_NEG_NORMALIZATION"
    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (super().compute_raw_mask(df, features=features, **params) & (features["funding_signed"] < 0.0)).fillna(False)


class FundingFlipToPositiveDetectorV2(FundingFlipDetectorV2):
    event_name = "FUNDING_FLIP_TO_POSITIVE"
    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (super().compute_raw_mask(df, features=features, **params) & (features["funding_signed"] > 0.0)).fillna(False)


class FundingFlipToNegativeDetectorV2(FundingFlipDetectorV2):
    event_name = "FUNDING_FLIP_TO_NEGATIVE"
    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (super().compute_raw_mask(df, features=features, **params) & (features["funding_signed"] < 0.0)).fillna(False)


class PriceUpOIUpDetectorV2(OISpikePositiveDetectorV2):
    event_name = "PRICE_UP_OI_UP"


class PriceDownOIUpDetectorV2(OISpikeNegativeDetectorV2):
    event_name = "PRICE_DOWN_OI_UP"


class PriceUpOIDownDetectorV2(OIFlushDetectorV2):
    event_name = "PRICE_UP_OI_DOWN"
    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (super().compute_raw_mask(df, features=features, **params) & (features["close_ret"] > 0.0)).fillna(False)


class PriceDownOIDownDetectorV2(OIFlushDetectorV2):
    event_name = "PRICE_DOWN_OI_DOWN"
    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (super().compute_raw_mask(df, features=features, **params) & (features["close_ret"] < 0.0)).fillna(False)
