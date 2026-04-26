from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.threshold import ThresholdDetector
from project.features.context_guards import optional_state


def _onset_mask(mask: pd.Series) -> pd.Series:
    typed = mask.astype("boolean")
    return (typed & ~typed.shift(1, fill_value=False)).astype(bool)


class TrendBase(ThresholdDetector):
    required_columns = ("timestamp", "close")
    min_spacing = 48

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = df["close"]
        return {"close": close}

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        # Standard intensity: (Trend Strength) * (1 + Rebound Strength)
        close = features.get("close", pd.Series(0.0, index=pd.RangeIndex(0)))
        if close.empty:
            return pd.Series(0.0, index=pd.RangeIndex(0))

        trend_window = int(params.get("trend_window", 96))
        rebound_window = int(params.get("rebound_window", 12))

        trend = close.pct_change(trend_window).abs().fillna(0.0)
        retrace = close.pct_change(rebound_window).abs().fillna(0.0)
        return (trend * (1.0 + retrace)).clip(lower=0.0)

    def _canonical_trend_state(self, df: pd.DataFrame) -> pd.Series:
        return optional_state(
            df,
            "ms_trend_state",
            min_confidence=0.55,
            max_entropy=0.90,
        )

    def _canonical_trend_present(self, df: pd.DataFrame) -> pd.Series:
        return optional_state(df, "ms_trend_state").notna()


class TrendAccelerationDetector(TrendBase):
    event_type = "TREND_ACCELERATION"
    min_spacing = 192
    threshold_quantile: float = 0.98
    min_trend_extension_quantile: float = 0.92

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = df["close"]

        trend_window = int(params.get("trend_window", 96))
        accel_window = int(params.get("accel_window", 3))

        trend_raw = close.pct_change(trend_window)
        trend_abs = trend_raw.abs()
        trend_delta = trend_abs.diff(accel_window)
        ret_1 = close.pct_change(1)

        window = int(params.get("threshold_window", 2880))

        # Adaptive thresholds
        q_ext = float(params.get("min_trend_extension_quantile", self.min_trend_extension_quantile))
        trend_q_ext = trend_abs.rolling(window, min_periods=288).quantile(q_ext).shift(1)

        q_accel = float(params.get("threshold_quantile", self.threshold_quantile))
        accel_q_threshold = trend_delta.rolling(window, min_periods=288).quantile(q_accel).shift(1)

        return {
            "close": close,
            "trend_raw": trend_raw,
            "trend_abs": trend_abs,
            "trend_delta": trend_delta,
            "ret_1": ret_1,
            "trend_q_ext": trend_q_ext,
            "accel_q_threshold": accel_q_threshold,
            "canonical_trend_state": self._canonical_trend_state(df),
            "canonical_trend_present": self._canonical_trend_present(df),
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        trend_abs = features["trend_abs"]
        trend_delta = features["trend_delta"]
        ret_1 = features["ret_1"]
        trend_raw = features["trend_raw"]
        trend_q_ext = features["trend_q_ext"]
        accel_q_threshold = features["accel_q_threshold"]

        # Consistency: recent returns moving in same direction as long trend
        direction_consistent = (
            np.sign(ret_1.rolling(window=6, min_periods=3).mean()) == np.sign(trend_raw)
        ).fillna(False)
        canonical_trend_state = features["canonical_trend_state"]
        canonical_trend_present = features.get(
            "canonical_trend_present",
            canonical_trend_state.notna(),
        )
        canonical_trend_active = (
            pd.Series(True, index=canonical_trend_state.index, dtype=bool)
            if not canonical_trend_present.any()
            else canonical_trend_state.isin([1.0, 2.0]).fillna(False)
        )

        return (
            (trend_abs >= trend_q_ext).fillna(False)
            & (trend_delta >= accel_q_threshold).fillna(False)
            & direction_consistent
            & canonical_trend_active
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (features["trend_abs"].fillna(0.0) * 100.0).clip(lower=0.0)


class TrendDecelerationDetector(TrendBase):
    event_type = "TREND_DECELERATION"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = df["close"]
        trend_window = int(params.get("trend_window", 96))
        accel_window = int(params.get("accel_window", 3))

        trend_abs = close.pct_change(trend_window).abs()
        trend_delta = trend_abs.diff(accel_window)
        return {
            "close": close,
            "trend_abs": trend_abs,
            "trend_delta": trend_delta,
            "canonical_trend_state": self._canonical_trend_state(df),
            "canonical_trend_present": self._canonical_trend_present(df),
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        min_trend = float(params.get("min_trend_pct", 0.01))
        min_decel = float(params.get("min_deceleration_pct", 0.001))
        canonical_trend_state = features["canonical_trend_state"]
        canonical_trend_present = features.get(
            "canonical_trend_present",
            canonical_trend_state.notna(),
        )
        canonical_trend_active = (
            pd.Series(True, index=canonical_trend_state.index, dtype=bool)
            if not canonical_trend_present.any()
            else canonical_trend_state.isin([1.0, 2.0]).fillna(False)
        )
        return (
            (features["trend_abs"] > min_trend)
            & (features["trend_delta"] < -min_decel)
            & canonical_trend_active
        ).fillna(False)


class RangeBreakoutDetector(TrendBase):
    event_type = "RANGE_BREAKOUT"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = df["close"]
        window = int(params.get("trend_window", params.get("range_window", 48)))
        rolling_max = close.rolling(window).max().shift(1)
        rolling_min = close.rolling(window).min().shift(1)
        return {"close": close, "rolling_max": rolling_max, "rolling_min": rolling_min}

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        break_up = (features["close"] > features["rolling_max"]).fillna(False)
        break_dn = (features["close"] < features["rolling_min"]).fillna(False)
        return (break_up | break_dn).fillna(False)


class FalseBreakoutDetector(TrendBase):
    event_type = "FALSE_BREAKOUT"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = df["close"]
        window = int(params.get("trend_window", params.get("range_window", 48)))
        rolling_max = close.rolling(window).max().shift(1)
        rolling_min = close.rolling(window).min().shift(1)
        range_ratio = pd.Series(np.nan, index=df.index, dtype=float)
        if "range_96" in df.columns and "range_med_2880" in df.columns:
            range_ratio = (
                pd.to_numeric(df["range_96"], errors="coerce")
                / pd.to_numeric(df["range_med_2880"], errors="coerce").replace(0.0, np.nan)
            ).astype(float)
        return {
            "close": close,
            "rolling_max": rolling_max,
            "rolling_min": rolling_min,
            "range_ratio": range_ratio,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        min_break_dist = float(params.get("min_breakout_distance", 0.0030))
        return_buffer = float(params.get("return_buffer", 0.0020))
        # Detect breakout at t-1 that fails (returns inside range) at t
        breakout_up_prev = (
            (features["close"].shift(1) - features["rolling_max"].shift(1))
            / features["rolling_max"].shift(1).replace(0.0, np.nan)
        ).fillna(0.0)
        breakout_dn_prev = (
            (features["rolling_min"].shift(1) - features["close"].shift(1))
            / features["rolling_min"].shift(1).replace(0.0, np.nan)
        ).fillna(0.0)

        was_break_up = (breakout_up_prev >= min_break_dist).fillna(False)
        was_break_dn = (breakout_dn_prev >= min_break_dist).fillna(False)

        # Require a meaningful return inside the range (not just barely crossing the boundary)
        is_back_in_up = (features["close"] <= features["rolling_max"] * (1 - return_buffer)).fillna(
            False
        )
        is_back_in_dn = (features["close"] >= features["rolling_min"] * (1 + return_buffer)).fillna(
            False
        )

        range_gate = pd.Series(True, index=features["close"].index, dtype=bool)
        max_range_ratio = params.get("max_range_ratio")
        if max_range_ratio is not None:
            range_gate = (
                pd.to_numeric(features.get("range_ratio"), errors="coerce")
                <= float(max_range_ratio)
            ).fillna(False)

        return (
            ((was_break_up & is_back_in_up) | (was_break_dn & is_back_in_dn)) & range_gate
        ).fillna(False)

    def compute_direction(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        del params
        close = features["close"]
        rolling_max = features["rolling_max"]
        rolling_min = features["rolling_min"]

        prev_idx = idx - 1
        if prev_idx < 0:
            return "non_directional"

        prior_close = close.iloc[prev_idx]
        prior_max = rolling_max.iloc[prev_idx]
        prior_min = rolling_min.iloc[prev_idx]

        if pd.notna(prior_close) and pd.notna(prior_max) and bool(prior_close > prior_max):
            return "up"
        if pd.notna(prior_close) and pd.notna(prior_min) and bool(prior_close < prior_min):
            return "down"
        return "non_directional"


class PullbackPivotDetector(TrendBase):
    event_type = "PULLBACK_PIVOT"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = df["close"]
        trend_window = int(params.get("trend_window", 96))
        pullback_window = int(params.get("pullback_window", 12))

        trend = close.pct_change(trend_window)
        retrace = close.pct_change(pullback_window)
        return {"close": close, "trend": trend, "retrace": retrace}

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        trend_th = float(params.get("trend_pct", 0.01))
        pullback_th = float(params.get("pullback_pct", 0.005))

        pivot_up = ((features["trend"] > trend_th) & (features["retrace"] < -pullback_th)).fillna(
            False
        )
        pivot_dn = ((features["trend"] < -trend_th) & (features["retrace"] > pullback_th)).fillna(
            False
        )
        return (pivot_up | pivot_dn).fillna(False)


class SREventDetector(TrendBase):
    event_type = "SUPPORT_RESISTANCE_BREAK"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = df["close"]
        trend_window = int(params.get("trend_window", 288))
        breakout_z_threshold = float(params.get("breakout_z_threshold", 2.5))
        min_periods = max(24, trend_window // 6)
        rolling_high = close.rolling(trend_window, min_periods=min_periods).max().shift(1)
        rolling_low = close.rolling(trend_window, min_periods=min_periods).min().shift(1)
        ret_1 = close.pct_change(1)
        vol = ret_1.rolling(trend_window, min_periods=min_periods).std().shift(1)
        breakout_up = ((close - rolling_high) / close.replace(0.0, np.nan)).clip(lower=0.0)
        breakout_down = ((rolling_low - close) / close.replace(0.0, np.nan)).clip(lower=0.0)
        breakout_mag = pd.concat([breakout_up, breakout_down], axis=1).max(axis=1)
        breakout_z = breakout_mag / vol.replace(0.0, np.nan)
        return {
            "close": close,
            "rolling_high": rolling_high,
            "rolling_low": rolling_low,
            "breakout_up": breakout_up,
            "breakout_down": breakout_down,
            "breakout_z": breakout_z.replace([np.inf, -np.inf], np.nan).fillna(0.0),
            "breakout_z_threshold": pd.Series(breakout_z_threshold, index=df.index, dtype=float),
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        breaks_level = (features["close"] > features["rolling_high"]).fillna(False) | (
            features["close"] < features["rolling_low"]
        ).fillna(False)
        return (
            breaks_level
            & (features["breakout_z"] >= features["breakout_z_threshold"]).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return features["breakout_z"].fillna(0.0)

    def compute_direction(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        del params
        if bool(features["breakout_up"].iloc[idx]):
            return "up"
        if bool(features["breakout_down"].iloc[idx]):
            return "down"
        return "non_directional"


__all__ = [
    "FalseBreakoutDetector",
    "PullbackPivotDetector",
    "RangeBreakoutDetector",
    "SREventDetector",
    "TrendAccelerationDetector",
    "TrendBase",
    "TrendDecelerationDetector",
]
