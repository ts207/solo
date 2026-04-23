from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.threshold import ThresholdDetector
from project.events.thresholding import dynamic_quantile_floor, rolling_vol_regime_factor
from project.features.context_guards import state_at_least
from project.features.rolling_thresholds import lagged_rolling_quantile


def _ewma_z(series: pd.Series, span: int) -> pd.Series:
    # Shift series by 1 to compute EWMA/Var on data strictly prior to t.
    # This prevents the current spike from inflating the volatility baseline
    # and dampening its own z-score.
    baseline = series.shift(1)
    ewma = baseline.ewm(span=span, adjust=False).mean()
    ewmvar = baseline.ewm(span=span, adjust=False).var()
    ewmstd = np.sqrt(ewmvar)
    return (series - ewma) / ewmstd.replace(0, np.nan)


class VolatilityBase(ThresholdDetector):
    required_columns = ("timestamp", "close", "rv_96", "range_96", "range_med_2880")
    timeframe_minutes = 5
    default_severity = "moderate"

    def compute_severity(
        self, idx: int, intensity: float, features: dict[str, pd.Series], **params: Any
    ) -> str:
        del idx, features, params
        if intensity >= 4.0:
            return "extreme"
        if intensity >= 2.5:
            return "major"
        return "moderate"

    def compute_metadata(
        self, idx: int, features: dict[str, pd.Series], **params: Any
    ) -> dict[str, Any]:
        del params
        return {
            "family": "volatility_transition",
            **{
                key: float(value.iloc[idx]) if hasattr(value, "iloc") else value
                for key, value in features.items()
                if key
                not in {
                    "mask",
                    "intensity",
                    "close",
                    "high",
                    "low",
                    "rolling_hi",
                    "rolling_lo",
                    "prior_high_96",
                    "prior_low_96",
                }
            },
        }


class VolSpikeDetector(VolatilityBase):
    event_type = "VOL_SPIKE"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        rv_96 = df["rv_96"].ffill()
        rv_z = _ewma_z(rv_96, 288)
        vol_factor = rolling_vol_regime_factor(df["range_med_2880"], window=2880)
        canonical_high_vol = state_at_least(
            df,
            "ms_vol_state",
            2.0,
            min_confidence=float(params.get("context_min_confidence", 0.55)),
            max_entropy=float(params.get("context_max_entropy", 0.90)),
        )

        quantile = float(params.get("quantile", 0.97))
        expansion_z = float(params.get("expansion_z_threshold", 2.0))

        dynamic_th = dynamic_quantile_floor(
            rv_z,
            window=2880,
            quantile=quantile,
            floor=expansion_z * vol_factor.clip(0.8, 1.5),
        )
        return {
            "rv_z": rv_z,
            "dynamic_threshold": dynamic_th,
            "vol_factor": vol_factor,
            "close": df["close"],
            "canonical_high_vol": canonical_high_vol,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (
            features["canonical_high_vol"].fillna(False)
            & (features["rv_z"] >= features["dynamic_threshold"]).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return features["rv_z"].abs()

    def compute_direction(self, idx: int, features: dict[str, pd.Series], **params: Any) -> str:
        # Look at price move over the lookback used for RV (default 96 bars)
        # Use parameterized window if available, else default to 12 as per original code but now configurable
        # Ideally should match the detection window (e.g. 96).
        window = int(params.get("rv_window", 96))
        close = features.get("close")
        if close is not None:
            ret = close.iloc[idx] / close.iloc[max(0, idx - window)] - 1.0
            return "up" if ret > 0 else "down" if ret < 0 else "non_directional"
        return "non_directional"


class VolRelaxationDetector(VolatilityBase):
    event_type = "VOL_RELAXATION_START"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        rv_96 = df["rv_96"].ffill()
        rv_z = _ewma_z(rv_96, 288)
        q_start = float(params.get("rv_q_start", 0.95))
        q_end = float(params.get("rv_q_end", 0.70))

        rv_q95 = lagged_rolling_quantile(rv_z, window=2880, quantile=q_start, min_periods=288)
        rv_q70 = lagged_rolling_quantile(rv_z, window=2880, quantile=q_end, min_periods=288)
        canonical_from_high_vol = state_at_least(
            df,
            "ms_vol_state",
            2.0,
            lag=1,
            min_confidence=float(params.get("context_min_confidence", 0.55)),
            max_entropy=float(params.get("context_max_entropy", 0.90)),
        )
        return {
            "rv_z": rv_z,
            "rv_q95": rv_q95,
            "rv_q70": rv_q70,
            "canonical_from_high_vol": canonical_from_high_vol,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        rv_z = features["rv_z"]
        rv_q95 = features["rv_q95"]
        rv_q70 = features["rv_q70"]
        return (
            features["canonical_from_high_vol"].fillna(False)
            & (rv_z.shift(1) >= rv_q95).fillna(False)
            & (rv_z < rv_q70).fillna(False)
            & (rv_z.diff() < 0).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return features["rv_z"].abs()


class VolClusterShiftDetector(VolatilityBase):
    event_type = "VOL_CLUSTER_SHIFT"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        rv_96 = df["rv_96"].ffill()
        rv_z = _ewma_z(rv_96, 288)
        rv_diff_abs = rv_z.diff().abs()

        shift_q = float(params.get("shift_quantile", 0.98))
        rv_shift_q98 = lagged_rolling_quantile(
            rv_diff_abs, window=2880, quantile=shift_q, min_periods=288
        )
        return {"rv_diff_abs": rv_diff_abs, "rv_shift_q98": rv_shift_q98, "rv_z": rv_z}

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (features["rv_diff_abs"] >= features["rv_shift_q98"]).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return features["rv_z"].abs()


class RangeCompressionDetector(VolatilityBase):
    event_type = "RANGE_COMPRESSION_END"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        del params
        range_96 = df["range_96"]
        range_med_2880 = df["range_med_2880"].replace(0.0, np.nan)
        comp_ratio = (range_96 / range_med_2880).astype(float)
        return {"comp_ratio": comp_ratio}

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        ratio_max = float(params.get("compression_ratio_max", 0.80))
        ratio_min = float(params.get("compression_ratio_min", 0.95))
        comp_ratio = features["comp_ratio"]
        return (
            (comp_ratio.shift(1) <= ratio_max).fillna(False)
            & (comp_ratio >= ratio_min).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return 1.0 / features["comp_ratio"].clip(lower=0.1)


class BreakoutTriggerDetector(VolatilityBase):
    event_type = "BREAKOUT_TRIGGER"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = df["close"]
        high = pd.to_numeric(df.get("high", close), errors="coerce").astype(float)
        low = pd.to_numeric(df.get("low", close), errors="coerce").astype(float)
        range_96 = df["range_96"]
        range_med_2880 = df["range_med_2880"].replace(0.0, np.nan)
        comp_ratio = (range_96 / range_med_2880).astype(float)

        lookback = int(params.get("vol_lookback_window", 96))
        rolling_hi = high.rolling(lookback, min_periods=max(1, lookback // 4)).max().shift(1)
        rolling_lo = low.rolling(lookback, min_periods=max(1, lookback // 4)).min().shift(1)

        ret_abs = close.pct_change(1).abs()
        breakout_dist_up = ((close - rolling_hi) / close.replace(0.0, np.nan)).clip(lower=0.0)
        breakout_dist_down = ((rolling_lo - close) / close.replace(0.0, np.nan)).clip(lower=0.0)
        breakout_dist = pd.concat([breakout_dist_up, breakout_dist_down], axis=1).max(axis=1)

        window = int(params.get("threshold_window", 2880))
        min_periods = max(window // 10, 1)
        ret_q80 = lagged_rolling_quantile(ret_abs, window=window, quantile=float(params.get("ret_quantile", 0.80)), min_periods=min_periods)

        exp_q = float(params.get("expansion_quantile", 0.85))
        breakout_q85 = lagged_rolling_quantile(
            breakout_dist,
            window=window,
            quantile=exp_q,
            min_periods=288,
        )
        return {
            "comp_ratio": comp_ratio,
            "rolling_hi": rolling_hi,
            "rolling_lo": rolling_lo,
            "close": close,
            "ret_abs": ret_abs,
            "breakout_dist": breakout_dist,
            "ret_q80": ret_q80,
            "breakout_q85": breakout_q85,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        comp_ratio = features["comp_ratio"]
        close = features["close"]
        rolling_hi = features["rolling_hi"]
        rolling_lo = features["rolling_lo"]
        ret_abs = features["ret_abs"]
        breakout_dist = features["breakout_dist"]

        ratio_max = float(params.get("compression_ratio_max", 0.80))
        min_dist = float(params.get("min_breakout_distance", 0.0015))
        comp_window = int(params.get("compression_window", 6))

        breakout = ((close > rolling_hi) | (close < rolling_lo)).fillna(False)
        compressed = (comp_ratio.shift(1) <= ratio_max).fillna(False) & (
            comp_ratio.rolling(comp_window, min_periods=1).max().shift(1) <= ratio_max
        ).fillna(False)
        impulse = (
            (ret_abs >= features["ret_q80"]).fillna(False)
            & (breakout_dist >= breakout_dist.combine(features["breakout_q85"], np.fmax)).fillna(
                False
            )
            & (breakout_dist >= min_dist).fillna(False)
        )
        return (breakout & compressed & impulse).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (
            features["breakout_dist"].fillna(0.0) * (1.0 / features["comp_ratio"].clip(lower=0.1))
        ).clip(lower=0.0)

    def compute_direction(self, idx: int, features: dict[str, pd.Series], **params: Any) -> str:
        del params
        close = pd.to_numeric(features.get("close"), errors="coerce")
        rolling_hi = pd.to_numeric(features.get("rolling_hi"), errors="coerce")
        rolling_lo = pd.to_numeric(features.get("rolling_lo"), errors="coerce")
        if close is None or rolling_hi is None or rolling_lo is None:
            return "non_directional"
        price = float(close.iloc[idx])
        prior_hi = float(rolling_hi.iloc[idx]) if pd.notna(rolling_hi.iloc[idx]) else np.nan
        prior_lo = float(rolling_lo.iloc[idx]) if pd.notna(rolling_lo.iloc[idx]) else np.nan
        if np.isfinite(prior_hi) and price > prior_hi:
            return "up"
        if np.isfinite(prior_lo) and price < prior_lo:
            return "down"
        return "non_directional"


class VolShockRelaxationDetector(VolatilityBase):
    event_type = "VOL_SHOCK"
    required_columns = ("timestamp", "close", "high", "low")

    def detect(self, df: pd.DataFrame, *, symbol: str, **params: Any) -> pd.DataFrame:
        from project.features.vol_shock_relaxation import (
            VolShockRelaxationConfig,
            detect_vol_shock_relaxation_events,
        )

        cfg_dict = {
            key: value
            for key, value in params.items()
            if key in VolShockRelaxationConfig.__dataclass_fields__
        }
        cfg = VolShockRelaxationConfig(**cfg_dict)
        events, _, _ = detect_vol_shock_relaxation_events(df, symbol=symbol, config=cfg)
        return events


__all__ = [
    "BreakoutTriggerDetector",
    "RangeCompressionDetector",
    "VolClusterShiftDetector",
    "VolRelaxationDetector",
    "VolShockRelaxationDetector",
    "VolSpikeDetector",
    "VolatilityBase",
]
