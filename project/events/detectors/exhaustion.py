from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.composite import CompositeDetector
from project.events.detectors.exhaustion_support import (
    compute_post_deleveraging_mask,
    compute_trend_exhaustion_mask,
    onset_mask,
    prepare_flow_exhaustion_features,
    prepare_post_deleveraging_features,
    prepare_trend_exhaustion_features,
)
from project.events.detectors.threshold import ThresholdDetector
from project.features.context_guards import optional_state
from project.features.rolling_thresholds import lagged_rolling_quantile


class FlowExhaustionDetector(CompositeDetector):
    event_type = "FLOW_EXHAUSTION_PROXY"
    required_columns = (
        "timestamp",
        "close",
        "high",
        "low",
        "rv_96",
        "oi_delta_1h",
        "liquidation_notional",
    )
    min_spacing = 24

    defaults = {
        "threshold_window": 2880,
        "oi_drop_quantile": 0.80,
        "liquidation_quantile": 0.85,
        "spread_quantile": 0.70,
        "return_quantile": 0.75,
        "rebound_window": 6,
        "reversal_window": 3,
        "reversal_quantile": 0.65,
        "oi_drop_abs_min": 5.0,
        "liquidation_abs_min": 25.0,
        "liquidation_multiplier": 0.9,
        "return_abs_min": 0.0025,
        "spread_abs_min": 5.0,
        "rv_decay_ratio": 0.99,
    }

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        return prepare_flow_exhaustion_features(df, self.defaults, params)

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (features["forced_flow"] & features["exhaustion"]).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (features["ret_abs"] * 1000.0) + (features["liquidation_notional"] / 100.0)

    def compute_direction(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        del params
        direction = float(
            features["direction"].iloc[idx] if not pd.isna(features["direction"].iloc[idx]) else 0.0
        )
        return "up" if direction < 0 else "down" if direction > 0 else "non_directional"


    def compute_metadata(
        self,
        idx: int,
        features: Mapping[str, pd.Series],
        **params: Any,
    ) -> Mapping[str, Any]:
        del idx, features, params
        return {
            "evidence_tier": "hybrid",
            "source_event_type": self.event_type,
        }

    def event_indices(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> list[int]:
        mask = self.compute_raw_mask(df, features=features, **params)
        onset = onset_mask(mask)
        from project.events.sparsify import sparsify_mask

        spacing = int(params.get("cooldown_bars", params.get("min_spacing", self.min_spacing)))
        return sparsify_mask(onset, min_spacing=spacing)


class PostDeleveragingReboundDetector(CompositeDetector):
    event_type = "POST_DELEVERAGING_REBOUND"
    required_columns = ("timestamp", "close", "rv_96", "oi_delta_1h", "liquidation_notional")
    min_spacing = 12

    defaults = {
        "threshold_window": 2880,
        "oi_drop_quantile": 0.80,
        "liquidation_quantile": 0.85,
        "spread_quantile": 0.70,
        "return_quantile": 0.75,
        "wick_quantile": 0.70,
        "rebound_window": 6,
        "rebound_quantile": 0.70,
        "reversal_window": 3,
        "reversal_quantile": 0.65,
        "oi_drop_abs_min": 5.0,
        "liquidation_abs_min": 25.0,
        "liquidation_multiplier": 0.9,
        "return_abs_min": 0.0025,
        "spread_abs_min": 5.0,
        "cluster_window": 12,
        "rebound_window_bars": 6,
        "post_cluster_lookback": 48,
        "rv_peak_decay_ratio": 0.99,
        "liq_cooldown_ratio": 0.50,
        "liquidation_cooldown_abs_max": 500.0,
        "rebound_return_min": 0.0015,
        "wick_ratio_min": 0.55,
    }

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        return prepare_post_deleveraging_features(df, self.defaults, params)

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return compute_post_deleveraging_mask(features, self.defaults, params)

    def event_indices(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> list[int]:
        mask = self.compute_raw_mask(df, features=features, **params)
        onset = onset_mask(mask)
        spacing = int(params.get("cooldown_bars", params.get("min_spacing", self.min_spacing)))
        from project.events.sparsify import sparsify_mask

        return sparsify_mask(onset, min_spacing=spacing)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        liq_ratio = features["liquidation_notional"] / features["liq_q85"].replace(0.0, np.nan)
        return (
            features["rebound_ret"].abs().fillna(0.0) * 100.0
            + features["reversal_impulse"].fillna(0.0) * 100.0
            + (1.0 - liq_ratio.fillna(0.0).clip(lower=0.0, upper=1.0))
        ).clip(lower=0.0)

    def compute_direction(self, idx: int, features: dict[str, pd.Series], **params: Any) -> str:
        del params
        rebound = float(
            features["rebound_ret"].iloc[idx]
            if not pd.isna(features["rebound_ret"].iloc[idx])
            else 0.0
        )
        return "up" if rebound > 0 else "down" if rebound < 0 else "non_directional"


class TrendExhaustionDetector(CompositeDetector):
    event_type = "TREND_EXHAUSTION_TRIGGER"
    required_columns = ("timestamp", "close", "rv_96")
    min_spacing = 96

    defaults = {
        "context_min_confidence": 0.55,
        "context_max_entropy": 0.90,
        "trend_window": 96,
        "vol_window": 288,
        "slope_fast_window": 12,
        "slope_slow_window": 48,
        "pullback_window": 96,
        "threshold_window": 2880,
        "trend_quantile": 0.95,
        "cooldown_quantile": 0.35,
        "pullback_quantile": 0.70,
        "reversal_window": 3,
        "reversal_quantile": 0.65,
        "trend_peak_multiplier": 1.30,
        "trend_strength_ratio": 3.0,
        "min_trend_duration_bars": 72,
        "cooldown_ratio": 0.90,
        "reversal_alignment_window": 3,
    }

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        return prepare_trend_exhaustion_features(df, self.defaults, params)

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return compute_trend_exhaustion_mask(features, self.defaults, params)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        pullback = pd.concat([features["pullback_up"], features["pullback_down"]], axis=1).max(
            axis=1
        )
        return (features["trend_abs"].fillna(0.0) * (1.0 + pullback.fillna(0.0))).clip(lower=0.0)

    def compute_direction(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        del params
        canonical_state = float(
            features["canonical_trend_state"].iloc[idx]
            if not pd.isna(features["canonical_trend_state"].iloc[idx])
            else np.nan
        )
        if canonical_state == 1.0:
            return "down"
        if canonical_state == 2.0:
            return "up"
        trend = float(
            features["trend"].iloc[idx] if not pd.isna(features["trend"].iloc[idx]) else 0.0
        )
        return "down" if trend > 0 else "up" if trend < 0 else "non_directional"

    def event_indices(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> list[int]:
        mask = self.compute_raw_mask(df, features=features, **params)
        onset = onset_mask(mask)
        from project.events.sparsify import sparsify_mask

        spacing = int(params.get("cooldown_bars", params.get("min_spacing", self.min_spacing)))
        return sparsify_mask(onset, min_spacing=spacing)


class MomentumDivergenceDetector(ThresholdDetector):
    event_type = "MOMENTUM_DIVERGENCE_TRIGGER"
    required_columns = ("timestamp", "close")
    min_spacing = 96
    min_trend_extension_quantile: float = 0.90

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = pd.to_numeric(df["close"], errors="coerce").astype(float)
        raw_canonical_trend_state = optional_state(df, "ms_trend_state")
        canonical_trend_state = optional_state(
            df,
            "ms_trend_state",
            min_confidence=float(params.get("context_min_confidence", 0.55)),
            max_entropy=float(params.get("context_max_entropy", 0.90)),
        )

        fast_window = int(params.get("fast_window", 12))
        slow_window = int(params.get("slow_window", 96))
        mom_fast = close.pct_change(fast_window)
        mom_slow = close.pct_change(slow_window)
        mom_slow_abs = mom_slow.abs()
        accel = mom_fast - mom_slow
        slow_sign = np.sign(mom_slow).fillna(0.0)
        trend_group = slow_sign.ne(slow_sign.shift(1)) | slow_sign.eq(0.0)
        trend_streak = slow_sign.groupby(trend_group.cumsum()).cumcount() + 1
        trend_streak = trend_streak.where(slow_sign != 0.0, 0).astype(float)

        ext_window = int(params.get("extension_window", 96))
        rolling_high = close.rolling(ext_window, min_periods=12).max().shift(1)
        rolling_low = close.rolling(ext_window, min_periods=12).min().shift(1)

        extension_up = ((close - rolling_low) / rolling_low.replace(0.0, np.nan)).clip(lower=0.0)
        extension_down = ((rolling_high - close) / rolling_high.replace(0.0, np.nan)).clip(
            lower=0.0
        )
        extension_max = pd.concat([extension_up, extension_down], axis=1).max(axis=1)

        divergence = (mom_fast * mom_slow < 0).fillna(False)

        reversal_window = int(params.get("reversal_window", 3))
        reversal_impulse = close.pct_change(reversal_window).abs()

        accel_abs = accel.abs()
        threshold_window = int(params.get("threshold_window", 2880))

        min_periods = max(threshold_window // 10, 1)
        accel_q_threshold = lagged_rolling_quantile(
            accel_abs,
            window=threshold_window,
            quantile=float(params.get("accel_quantile", 0.90)),
            min_periods=min_periods,
        )
        ext_q = float(params.get("min_trend_extension_quantile", self.min_trend_extension_quantile))
        extension_q_threshold = lagged_rolling_quantile(
            extension_max, window=threshold_window, quantile=ext_q, min_periods=min_periods
        )
        slow_trend_q = lagged_rolling_quantile(
            mom_slow_abs,
            window=threshold_window,
            quantile=float(params.get("slow_trend_quantile", 0.70)),
            min_periods=min_periods,
        )

        reversal_q70 = lagged_rolling_quantile(
            reversal_impulse,
            window=threshold_window,
            quantile=float(params.get("reversal_quantile", 0.70)),
            min_periods=min_periods,
        )
        divergence_turn = (mom_fast.shift(1) * mom_fast <= 0).fillna(False)

        return {
            "mom_fast": mom_fast,
            "mom_slow": mom_slow,
            "mom_slow_abs": mom_slow_abs,
            "divergence": divergence,
            "reversal_impulse": reversal_impulse,
            "accel_abs": accel_abs,
            "extension_max": extension_max,
            "accel_q_threshold": accel_q_threshold,
            "extension_q_threshold": extension_q_threshold,
            "slow_trend_q": slow_trend_q,
            "trend_streak": trend_streak,
            "reversal_q70": reversal_q70,
            "divergence_turn": divergence_turn,
            "canonical_trend_state": canonical_trend_state,
            "canonical_trend_present": raw_canonical_trend_state.notna(),
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        # Must be extended relative to history to fire divergence
        is_extended = (features["extension_max"] >= features["extension_q_threshold"]).fillna(False)
        max_trend_persistence_bars = int(params.get("max_trend_persistence_bars", 72))
        canonical_trend_state = features["canonical_trend_state"]
        canonical_trend_present = features.get(
            "canonical_trend_present",
            canonical_trend_state.notna(),
        )
        canonical_chop = (
            pd.Series(False, index=canonical_trend_state.index, dtype=bool)
            if not canonical_trend_present.any()
            else (canonical_trend_state == 0.0).fillna(False)
        )
        weak_or_choppy_regime = (
            canonical_chop
            | (features["mom_slow_abs"] <= features["slow_trend_q"]).fillna(False)
            | (features["trend_streak"] <= max_trend_persistence_bars).fillna(False)
        )

        return (
            features["divergence"]
            & features["divergence_turn"]
            & is_extended
            & weak_or_choppy_regime
            & (
                (features["accel_abs"] >= features["accel_q_threshold"]).fillna(False)
                | (features["reversal_impulse"] >= features["reversal_q70"]).fillna(False)
            )
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        # Use simple name for max extension from prepare_features
        extension = features["extension_max"]
        return (features["accel_abs"].fillna(0.0) * (1.0 + extension.fillna(0.0))).clip(lower=0.0)

    def compute_direction(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        del params
        accel = float(features["mom_fast"].iloc[idx] - features["mom_slow"].iloc[idx])
        return "down" if accel < 0 else "up" if accel > 0 else "non_directional"

    def event_indices(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> list[int]:
        mask = self.compute_raw_mask(df, features=features, **params)
        onset = onset_mask(mask)
        from project.events.sparsify import sparsify_mask

        spacing = int(params.get("cooldown_bars", params.get("min_spacing", self.min_spacing)))
        return sparsify_mask(onset, min_spacing=spacing)


class ClimaxVolumeDetector(ThresholdDetector):
    event_type = "CLIMAX_VOLUME_BAR"
    required_columns = ("timestamp", "close", "high", "low", "volume")
    min_spacing = 12

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        vol = pd.to_numeric(df["volume"], errors="coerce").astype(float)
        close = pd.to_numeric(df["close"], errors="coerce").astype(float)
        high = pd.to_numeric(df["high"], errors="coerce").astype(float)
        low = pd.to_numeric(df["low"], errors="coerce").astype(float)
        ret_abs = close.pct_change(1).abs()
        bar_range = (high - low) / close.replace(0.0, np.nan)

        window = int(params.get("threshold_window", 2880))
        min_periods = max(window // 10, 1)
        vol_q97 = lagged_rolling_quantile(
            vol,
            window=window,
            quantile=float(params.get("vol_quantile", 0.97)),
            min_periods=min_periods,
        )
        ret_q97 = lagged_rolling_quantile(
            ret_abs,
            window=window,
            quantile=float(params.get("ret_quantile", 0.97)),
            min_periods=min_periods,
        )
        range_q95 = lagged_rolling_quantile(
            bar_range,
            window=window,
            quantile=float(params.get("range_quantile", 0.95)),
            min_periods=min_periods,
        )
        return {
            "vol": vol,
            "ret_abs": ret_abs,
            "bar_range": bar_range,
            "vol_q97": vol_q97,
            "ret_q97": ret_q97,
            "range_q95": range_q95,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (
            (features["vol"] >= features["vol_q97"]).fillna(False)
            & (features["ret_abs"] >= features["ret_q97"]).fillna(False)
            & (features["bar_range"] >= features["range_q95"]).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (features["vol"] / features["vol_q97"]).fillna(0.0)

    def event_indices(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> list[int]:
        mask = self.compute_raw_mask(df, features=features, **params)
        onset = onset_mask(mask)
        from project.events.sparsify import sparsify_mask

        spacing = int(params.get("cooldown_bars", params.get("min_spacing", self.min_spacing)))
        return sparsify_mask(onset, min_spacing=spacing)


class FailedContinuationDetector(ThresholdDetector):
    event_type = "FAILED_CONTINUATION"
    required_columns = ("timestamp", "close", "high", "low")
    min_spacing = 24

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = pd.to_numeric(df["close"], errors="coerce").astype(float)
        high = pd.to_numeric(df["high"], errors="coerce").astype(float)
        low = pd.to_numeric(df["low"], errors="coerce").astype(float)

        breakout_window = int(params.get("breakout_window", 48))
        reversal_window = int(params.get("reversal_window", 12))

        trend = close.pct_change(breakout_window)
        prior_high = (
            high.rolling(breakout_window, min_periods=max(6, breakout_window // 4)).max().shift(1)
        )
        prior_low = (
            low.rolling(breakout_window, min_periods=max(6, breakout_window // 4)).min().shift(1)
        )

        breakout_up = (close.shift(1) >= prior_high.shift(1)).fillna(False)
        breakout_dn = (close.shift(1) <= prior_low.shift(1)).fillna(False)
        breakout_strength_up = (
            (close.shift(1) - prior_high.shift(1)) / close.shift(1).replace(0.0, np.nan)
        ).clip(lower=0.0)
        breakout_strength_dn = (
            (prior_low.shift(1) - close.shift(1)) / close.shift(1).replace(0.0, np.nan)
        ).clip(lower=0.0)

        recent_window = int(params.get("recent_breakout_window", 6))
        breakout_up_recent = (
            breakout_up.rolling(window=recent_window, min_periods=1).max().astype(bool)
        )
        breakout_dn_recent = (
            breakout_dn.rolling(window=recent_window, min_periods=1).max().astype(bool)
        )
        recent_breakout_strength_up = breakout_strength_up.rolling(
            window=recent_window, min_periods=1
        ).max()
        recent_breakout_strength_dn = breakout_strength_dn.rolling(
            window=recent_window, min_periods=1
        ).max()

        reentry_up = (close < prior_high).fillna(False)
        reentry_dn = (close > prior_low).fillna(False)
        reversal_fast = close.pct_change(max(1, reversal_window // 2))

        breakout_strength_min = float(params.get("breakout_strength_min", 0.0020))
        reentry_min = float(params.get("reentry_min", 0.0030))
        reversal_return_min = float(params.get("reversal_return_min", 0.0010))

        failed_up = (
            breakout_up_recent
            & (recent_breakout_strength_up >= breakout_strength_min).fillna(False)
            & reentry_up
            & (close < prior_high * (1.0 - reentry_min)).fillna(False)
            & (reversal_fast < -reversal_return_min).fillna(False)
        )
        failed_dn = (
            breakout_dn_recent
            & (recent_breakout_strength_dn >= breakout_strength_min).fillna(False)
            & reentry_dn
            & (close > prior_low * (1.0 + reentry_min)).fillna(False)
            & (reversal_fast > reversal_return_min).fillna(False)
        )

        ret_abs = close.pct_change(1).abs()
        threshold_window = int(params.get("threshold_window", 2880))
        reversal_quantile = float(params.get("reversal_quantile", 0.60))
        ret_q60 = lagged_rolling_quantile(
            ret_abs,
            window=threshold_window,
            quantile=reversal_quantile,
            min_periods=max(threshold_window // 10, 1),
        )

        range_width = (prior_high - prior_low).replace(0.0, np.nan)
        reentry_distance = pd.concat(
            [
                ((prior_high - close) / range_width).clip(lower=0.0),
                ((close - prior_low) / range_width).clip(lower=0.0),
            ],
            axis=1,
        ).max(axis=1)

        return {
            "trend": trend,
            "failed_up": failed_up,
            "failed_dn": failed_dn,
            "ret_abs": ret_abs,
            "ret_q60": ret_q60,
            "reentry_distance": reentry_distance.fillna(0.0),
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (
            (features["failed_up"] | features["failed_dn"]).fillna(False)
            & (features["ret_abs"] >= features["ret_q60"]).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (
            features["ret_abs"].fillna(0.0) * (1.0 + features["reentry_distance"].fillna(0.0))
        ).clip(lower=0.0)

    def compute_direction(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        del params
        if bool(features["failed_up"].iloc[idx]):
            return "down"
        if bool(features["failed_dn"].iloc[idx]):
            return "up"
        return "non_directional"

    def event_indices(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> list[int]:
        mask = self.compute_raw_mask(df, features=features, **params)
        onset = onset_mask(mask)
        from project.events.sparsify import sparsify_mask

        spacing = int(params.get("cooldown_bars", params.get("min_spacing", self.min_spacing)))
        return sparsify_mask(onset, min_spacing=spacing)


class ForcedFlowExhaustionDetector(FlowExhaustionDetector):
    """Dedicated detector for FORCED_FLOW_EXHAUSTION.

    Inherits all logic from FlowExhaustionDetector but has its own event_type,
    signal_column, and tighter defaults reflecting that forced deleveraging events
    require higher evidence thresholds than a proxy signal.

    This separation allows FORCED_FLOW_EXHAUSTION and FLOW_EXHAUSTION_PROXY to be
    independently parameterized, calibrated, and disabled.
    """

    event_type = "FORCED_FLOW_EXHAUSTION"
    # Stricter defaults — forced flows carry a stronger prior on liquidation
    defaults = {
        **FlowExhaustionDetector.defaults,
        "oi_drop_quantile": 0.88,
        "liquidation_quantile": 0.92,
        "return_quantile": 0.80,
        "min_spacing": 32,
    }


EXHAUSTION_DETECTORS = {
    "FLOW_EXHAUSTION_PROXY": FlowExhaustionDetector,
    "FORCED_FLOW_EXHAUSTION": ForcedFlowExhaustionDetector,
    "POST_DELEVERAGING_REBOUND": PostDeleveragingReboundDetector,
    "TREND_EXHAUSTION_TRIGGER": TrendExhaustionDetector,
    "MOMENTUM_DIVERGENCE_TRIGGER": MomentumDivergenceDetector,
    "CLIMAX_VOLUME_BAR": ClimaxVolumeDetector,
    "FAILED_CONTINUATION": FailedContinuationDetector,
}


__all__ = [
    "EXHAUSTION_DETECTORS",
    "ClimaxVolumeDetector",
    "FailedContinuationDetector",
    "FlowExhaustionDetector",
    "ForcedFlowExhaustionDetector",
    "MomentumDivergenceDetector",
    "PostDeleveragingReboundDetector",
    "TrendExhaustionDetector",
]
