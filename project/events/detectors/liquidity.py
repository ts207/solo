from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.composite import CompositeDetector
from project.events.detectors.threshold import ThresholdDetector
from project.events.thresholding import rolling_mean_std_zscore
from project.features.context_guards import state_at_least
from project.features.rolling_thresholds import lagged_rolling_quantile


class BaseLiquidityStressDetector(CompositeDetector):
    """Base logic for liquidity stress detection."""

    required_columns = ("timestamp", "close", "high", "low")
    timeframe_minutes = 5
    default_severity = "moderate"
    default_depth_collapse_threshold = 0.5
    default_spread_spike_threshold = 3.0
    default_imbalance_sensitivity = 0.2
    default_imbalance_threshold = 0.5
    default_depth_floor = 0.1
    default_major_intensity_threshold = 6.0
    default_extreme_intensity_threshold = 12.0

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        depth = features["depth"]
        spread = features["spread"]
        depth_med = features["depth_median"]
        spread_med = features["spread_median"]
        imbalance = features["imbalance"]
        canonical_spread_wide = features.get("canonical_spread_wide")

        depth_collapse_th = float(
            params.get("depth_collapse_th", self.default_depth_collapse_threshold)
        )
        spread_spike_th = float(params.get("spread_spike_th", self.default_spread_spike_threshold))
        imbalance_sensitivity = float(
            params.get("imbalance_sensitivity", self.default_imbalance_sensitivity)
        )
        imbalance_th = float(params.get("imbalance_threshold", self.default_imbalance_threshold))

        dynamic_spread_th = spread_spike_th * (
            1.0 - imbalance_sensitivity * (imbalance.abs() > imbalance_th).astype(float)
        )
        mask = (
            depth.notna()
            & spread.notna()
            & depth_med.notna()
            & spread_med.notna()
            & (depth_med > 0.0)
            & (spread_med > 0.0)
            & (depth < depth_med * depth_collapse_th)
            & (spread > spread_med * dynamic_spread_th)
        )
        if canonical_spread_wide is not None:
            mask = mask & canonical_spread_wide.fillna(False)
        return mask.fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        depth = features["depth"]
        spread = features["spread"]
        depth_med = features["depth_median"].replace(0.0, np.nan)
        spread_med = features["spread_median"].replace(0.0, np.nan)
        imbalance = features["imbalance"]

        depth_floor = float(params.get("depth_floor", self.default_depth_floor))

        depth_ratio = depth / depth_med
        spread_ratio = spread / spread_med
        score = spread_ratio * (1.0 / depth_ratio.clip(lower=depth_floor)) * (1.0 + imbalance.abs())
        return score.replace([np.inf, -np.inf], np.nan)

    def compute_severity(
        self,
        idx: int,
        intensity: float,
        features: dict[str, pd.Series],
        **params: Any,
    ) -> str:
        del idx, features
        extreme_threshold = float(
            params.get("severity_extreme_threshold", self.default_extreme_intensity_threshold)
        )
        major_threshold = float(
            params.get("severity_major_threshold", self.default_major_intensity_threshold)
        )
        if intensity >= extreme_threshold:
            return "extreme"
        if intensity >= major_threshold:
            return "major"
        return "moderate"

    def compute_metadata(
        self, idx: int, features: dict[str, pd.Series], **params: Any
    ) -> dict[str, Any]:
        del params
        depth_val = features["depth"].iloc[idx]
        depth_med = features["depth_median"].iloc[idx]
        spread_val = features["spread"].iloc[idx]
        spread_med = features["spread_median"].iloc[idx]

        return {
            "evidence_tier": str(features["evidence_tier"].iloc[idx]),
            "depth_source": str(features["depth_source"].iloc[idx]),
            "spread_source": str(features["spread_source"].iloc[idx]),
            "depth_ratio": float(np.nan_to_num((depth_val / max(depth_med, 1e-12)), nan=0.0)),
            "spread_ratio": float(np.nan_to_num((spread_val / max(spread_med, 1e-12)), nan=0.0)),
            "imbalance": float(np.nan_to_num(features["imbalance"].iloc[idx], nan=0.0)),
        }


class DirectLiquidityStressDetector(BaseLiquidityStressDetector):
    """
    Liquidity stress detector using direct L2/L3 aggregate microstructure metrics.
    Aligned with build_microstructure_rollup.py output.
    """

    event_type = "LIQUIDITY_STRESS_DIRECT"
    required_columns = ("timestamp", "close", "high", "low", "depth_usd", "spread_bps")

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        window = int(params.get("median_window", 288))
        min_periods = int(params.get("min_periods", max(24, window // 12)))

        depth = pd.to_numeric(df["depth_usd"], errors="coerce").astype(float)
        spread = pd.to_numeric(df["spread_bps"], errors="coerce").abs().astype(float)

        imbalance_raw = (
            df["ms_imbalance_24"]
            if "ms_imbalance_24" in df.columns
            else pd.Series(0.0, index=df.index)
        )
        imbalance = pd.to_numeric(imbalance_raw, errors="coerce").fillna(0.0).astype(float)
        if "ms_spread_state" in df.columns:
            canonical_spread_wide = state_at_least(
                df,
                "ms_spread_state",
                1.0,
                default_if_absent=True,
                min_confidence=float(params.get("context_min_confidence", 0.55)),
                max_entropy=float(params.get("context_max_entropy", 0.90)),
            )
        else:
            canonical_spread_wide = pd.Series(True, index=df.index, dtype=bool)

        # micro_spread_stress is already a ratio, but we compute rolling medians for consistency
        # with the Base class which expects raw units to be compared against medians.
        depth_med = depth.shift(1).rolling(window=window, min_periods=min_periods).median()
        spread_med = spread.shift(1).rolling(window=window, min_periods=min_periods).median()

        return {
            "depth": depth,
            "spread": spread,
            "depth_median": depth_med,
            "spread_median": spread_med,
            "imbalance": imbalance,
            "canonical_spread_wide": canonical_spread_wide,
            "evidence_tier": pd.Series("direct", index=df.index),
            "depth_source": pd.Series("depth_usd", index=df.index),
            "spread_source": pd.Series("spread_bps", index=df.index),
        }


class ProxyLiquidityStressDetector(BaseLiquidityStressDetector):
    """Compatibility liquidity-stress detector using hybrid evidence.

    The detector keeps the legacy event surface but upgrades it from pure proxy
    logic to a blended contract: direct order-book stress when available
    (depth_usd/spread_bps) plus bar-range expansion and optional trade-flow
    collapse confirmation from OHLCV inputs.
    """

    event_type = "LIQUIDITY_STRESS_PROXY"
    required_columns = ("timestamp", "close", "high", "low")
    default_proxy_range_spike_threshold = 1.25
    default_proxy_volume_collapse_threshold = 0.80

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        window = int(params.get("median_window", 288))
        min_periods = int(params.get("min_periods", max(24, window // 12)))

        high = pd.to_numeric(df["high"], errors="coerce").astype(float)
        low = pd.to_numeric(df["low"], errors="coerce").astype(float)
        close = pd.to_numeric(df["close"], errors="coerce").replace(0.0, np.nan).astype(float)
        bp_scale = float(params.get("bp_scale", 10000.0))
        bar_range_bps = ((high - low) / close).abs() * bp_scale
        proxy_range_median = bar_range_bps.shift(1).rolling(window=window, min_periods=min_periods).median()

        if "depth_usd" in df.columns:
            depth = pd.to_numeric(df["depth_usd"], errors="coerce").astype(float)
            depth_source = "depth_usd+bar_range_bps"
        elif "quote_volume" in df.columns:
            depth = pd.to_numeric(df["quote_volume"], errors="coerce").astype(float)
            depth_source = "quote_volume+bar_range_bps"
        else:
            depth = bar_range_bps.replace(0.0, np.nan)
            depth_source = "bar_range_bps_only"

        if "spread_bps" in df.columns:
            spread = pd.to_numeric(df["spread_bps"], errors="coerce").abs().astype(float)
            spread_source = "spread_bps+bar_range_bps"
        else:
            spread = bar_range_bps
            spread_source = "bar_range_bps"

        if "ms_imbalance_24" in df.columns:
            imbalance = pd.to_numeric(df["ms_imbalance_24"], errors="coerce").fillna(0.0).astype(float)
        else:
            imbalance = pd.Series(0.0, index=df.index, dtype=float)

        if "quote_volume" in df.columns:
            proxy_volume = pd.to_numeric(df["quote_volume"], errors="coerce").astype(float)
            proxy_volume_median = proxy_volume.shift(1).rolling(window=window, min_periods=min_periods).median()
        else:
            proxy_volume = pd.Series(np.nan, index=df.index, dtype=float)
            proxy_volume_median = pd.Series(np.nan, index=df.index, dtype=float)

        if "ms_spread_state" in df.columns:
            canonical_spread_wide = state_at_least(
                df,
                "ms_spread_state",
                1.0,
                default_if_absent=True,
                min_confidence=float(params.get("context_min_confidence", 0.55)),
                max_entropy=float(params.get("context_max_entropy", 0.90)),
            )
        else:
            canonical_spread_wide = pd.Series(True, index=df.index, dtype=bool)

        depth_med = depth.shift(1).rolling(window=window, min_periods=min_periods).median()
        spread_med = spread.shift(1).rolling(window=window, min_periods=min_periods).median()

        return {
            "depth": depth,
            "spread": spread,
            "depth_median": depth_med,
            "spread_median": spread_med,
            "imbalance": imbalance,
            "canonical_spread_wide": canonical_spread_wide,
            "bar_range_bps": bar_range_bps,
            "proxy_range_median": proxy_range_median,
            "proxy_volume": proxy_volume,
            "proxy_volume_median": proxy_volume_median,
            "evidence_tier": pd.Series("hybrid", index=df.index),
            "depth_source": pd.Series(depth_source, index=df.index),
            "spread_source": pd.Series(spread_source, index=df.index),
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        base_mask = super().compute_raw_mask(df, features=features, **params)
        proxy_range_spike_th = float(
            params.get("proxy_range_spike_th", self.default_proxy_range_spike_threshold)
        )
        range_confirm = (
            features["bar_range_bps"]
            > features["proxy_range_median"] * proxy_range_spike_th
        ).fillna(False)
        if features["proxy_volume"].notna().any():
            proxy_volume_collapse_th = float(
                params.get(
                    "proxy_volume_collapse_th",
                    self.default_proxy_volume_collapse_threshold,
                )
            )
            volume_confirm = (
                features["proxy_volume"]
                < features["proxy_volume_median"] * proxy_volume_collapse_th
            ).fillna(False)
        else:
            volume_confirm = pd.Series(True, index=range_confirm.index, dtype=bool)
        return (base_mask & range_confirm & volume_confirm).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        base_score = super().compute_intensity(df, features=features, **params).fillna(0.0)
        range_ratio = (
            features["bar_range_bps"]
            / features["proxy_range_median"].replace(0.0, np.nan)
        ).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        if features["proxy_volume"].notna().any():
            volume_ratio = (
                features["proxy_volume_median"]
                / features["proxy_volume"].replace(0.0, np.nan)
            ).replace([np.inf, -np.inf], np.nan).fillna(0.0)
            return (base_score + range_ratio + volume_ratio) / 3.0
        return (base_score + range_ratio) / 2.0


class LiquidityStressDetector(BaseLiquidityStressDetector):
    """Legacy polymorphic liquidity stress detector for backward compatibility."""

    event_type = "LIQUIDITY_SHOCK"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        if "depth_usd" in df.columns and "spread_bps" in df.columns:
            return DirectLiquidityStressDetector().prepare_features(df, **params)
        return ProxyLiquidityStressDetector().prepare_features(df, **params)

    def compute_event_type(self, idx: int, features: dict[str, pd.Series]) -> str:
        tier = features["evidence_tier"].iloc[idx]
        if tier == "direct":
            return "LIQUIDITY_STRESS_DIRECT"
        return "LIQUIDITY_STRESS_PROXY"


class DepthCollapseDetector(ThresholdDetector):
    event_type = "DEPTH_COLLAPSE"
    required_columns = ("timestamp", "spread_zscore", "rv_96")

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        spread_z = df["spread_zscore"]
        rv_96 = df["rv_96"].ffill()
        lookback = int(params.get("lookback_window", 288))
        rv_z = rolling_mean_std_zscore(rv_96, window=lookback)

        threshold_window = int(params.get("threshold_window", 2880))
        q_spread = float(params.get("spread_quantile", 0.90))
        q_rv = float(params.get("rv_quantile", 0.70))

        spread_q90 = lagged_rolling_quantile(
            spread_z,
            window=threshold_window,
            quantile=q_spread,
            min_periods=lookback,
        )
        rv_q70 = lagged_rolling_quantile(
            rv_z,
            window=threshold_window,
            quantile=q_rv,
            min_periods=lookback,
        )
        if "ms_spread_state" in df.columns:
            canonical_spread_wide = state_at_least(
                df,
                "ms_spread_state",
                1.0,
                default_if_absent=True,
                min_confidence=float(params.get("context_min_confidence", 0.55)),
                max_entropy=float(params.get("context_max_entropy", 0.90)),
            )
        else:
            canonical_spread_wide = pd.Series(True, index=df.index, dtype=bool)
        return {
            "spread_z": spread_z,
            "rv_z": rv_z,
            "spread_q90": spread_q90,
            "rv_q70": rv_q70,
            "canonical_spread_wide": canonical_spread_wide,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (
            features["canonical_spread_wide"].fillna(False)
            & (features["spread_z"] >= features["spread_q90"]).fillna(False)
            & (features["rv_z"] >= features["rv_q70"]).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return features["spread_z"].abs()


class SpreadBlowoutDetector(ThresholdDetector):
    event_type = "SPREAD_BLOWOUT"
    required_columns = ("timestamp", "spread_zscore")

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        spread_z = df["spread_zscore"]
        lookback = int(params.get("lookback_window", 288))
        threshold_window = int(params.get("threshold_window", 2880))
        q_spread = float(params.get("z_quantile", params.get("spread_quantile", 0.97)))

        spread_q97 = lagged_rolling_quantile(
            spread_z,
            window=threshold_window,
            quantile=q_spread,
            min_periods=lookback,
        )
        if "ms_spread_state" in df.columns:
            canonical_spread_wide = state_at_least(
                df,
                "ms_spread_state",
                1.0,
                default_if_absent=True,
                min_confidence=float(params.get("context_min_confidence", 0.55)),
                max_entropy=float(params.get("context_max_entropy", 0.90)),
            )
        else:
            canonical_spread_wide = pd.Series(True, index=df.index, dtype=bool)
        return {
            "spread_z": spread_z,
            "spread_q97": spread_q97,
            "canonical_spread_wide": canonical_spread_wide,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        spread_z = features["spread_z"]
        spread_q97 = features["spread_q97"]

        z_floor = float(params.get("z_threshold", 2.0))
        threshold = spread_q97.where(spread_q97 >= z_floor, z_floor)
        return (
            features["canonical_spread_wide"].fillna(False) & (spread_z >= threshold).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return features["spread_z"].abs()


class OrderflowImbalanceDetector(CompositeDetector):
    event_type = "ORDERFLOW_IMBALANCE_SHOCK"
    required_columns = ("timestamp", "close", "rv_96")
    DEFAULT_RET_QUANTILE = 0.99
    DEFAULT_RV_QUANTILE = 0.7

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        ret_abs = df["close"].pct_change(1).abs()
        rv_96 = df["rv_96"].ffill()

        lookback = int(params.get("lookback_window", 288))
        rv_z = rolling_mean_std_zscore(rv_96, window=lookback)

        threshold_window = int(params.get("threshold_window", 2880))
        q_ret = float(params.get("ret_quantile", self.DEFAULT_RET_QUANTILE))
        q_rv = float(params.get("rv_quantile", self.DEFAULT_RV_QUANTILE))

        ret_q99 = lagged_rolling_quantile(
            ret_abs,
            window=threshold_window,
            quantile=q_ret,
            min_periods=lookback,
        )
        rv_q70 = lagged_rolling_quantile(
            rv_z,
            window=threshold_window,
            quantile=q_rv,
            min_periods=lookback,
        )
        return {"ret_abs": ret_abs, "rv_z": rv_z, "ret_q99": ret_q99, "rv_q70": rv_q70}

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (
            (features["ret_abs"] >= features["ret_q99"]).fillna(False)
            & (features["rv_z"] >= features["rv_q70"]).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        bp_scale = float(params.get("bp_scale", 10000.0))
        return features["ret_abs"] * bp_scale


class AbsorptionDetector(CompositeDetector):
    event_type = "ABSORPTION_EVENT"
    required_columns = ("timestamp", "close", "spread_zscore", "rv_96")

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        ret_abs = df["close"].pct_change(1).abs()
        spread_z = df["spread_zscore"]
        rv_96 = df["rv_96"].ffill()
        lookback = int(params.get("lookback_window", 288))
        rv_z = rolling_mean_std_zscore(rv_96, window=lookback)

        threshold_window = int(params.get("threshold_window", 2880))
        q_ret = float(params.get("ret_quantile", 0.35))
        q_spread = float(params.get("spread_quantile", 0.80))
        q_rv = float(params.get("rv_quantile", 0.70))

        ret_q35 = lagged_rolling_quantile(
            ret_abs,
            window=threshold_window,
            quantile=q_ret,
            min_periods=lookback,
        )
        spread_q80 = lagged_rolling_quantile(
            spread_z,
            window=threshold_window,
            quantile=q_spread,
            min_periods=lookback,
        )
        rv_q70 = lagged_rolling_quantile(
            rv_z,
            window=threshold_window,
            quantile=q_rv,
            min_periods=lookback,
        )
        if "ms_spread_state" in df.columns:
            canonical_spread_wide = state_at_least(
                df,
                "ms_spread_state",
                1.0,
                default_if_absent=True,
                min_confidence=float(params.get("context_min_confidence", 0.55)),
                max_entropy=float(params.get("context_max_entropy", 0.90)),
            )
        else:
            canonical_spread_wide = pd.Series(True, index=df.index, dtype=bool)
        return {
            "ret_abs": ret_abs,
            "spread_z": spread_z,
            "rv_z": rv_z,
            "ret_q35": ret_q35,
            "spread_q80": spread_q80,
            "rv_q70": rv_q70,
            "canonical_spread_wide": canonical_spread_wide,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (
            features["canonical_spread_wide"].fillna(False)
            & (features["ret_abs"] <= features["ret_q35"]).fillna(False)
            & (features["spread_z"] >= features["spread_q80"]).fillna(False)
            & (features["rv_z"] >= features["rv_q70"]).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return features["spread_z"].abs() + features["rv_z"].abs()


class LiquidityGapDetector(ThresholdDetector):
    event_type = "LIQUIDITY_GAP_PRINT"
    required_columns = ("timestamp", "close")

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        ret_abs = df["close"].pct_change(1).abs()

        lookback = int(params.get("lookback_window", 288))
        threshold_window = int(params.get("threshold_window", 2880))
        q_ret = float(params.get("ret_quantile", 0.995))

        ret_q995 = lagged_rolling_quantile(
            ret_abs,
            window=threshold_window,
            quantile=q_ret,
            min_periods=lookback,
        )
        return {"ret_abs": ret_abs, "ret_q995": ret_q995}

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (features["ret_abs"] >= features["ret_q995"]).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        bp_scale = float(params.get("bp_scale", 10000.0))
        return features["ret_abs"] * bp_scale


class LiquidityVacuumDetector(ThresholdDetector):
    event_type = "LIQUIDITY_VACUUM"
    required_columns = ("timestamp", "close", "high", "low", "volume")

    def detect(self, df: pd.DataFrame, *, symbol: str, **params: Any) -> pd.DataFrame:
        from project.features.liquidity_vacuum import (
            LiquidityVacuumConfig,
            detect_liquidity_vacuum_events,
        )

        cfg_dict = {
            key: value
            for key, value in params.items()
            if key in LiquidityVacuumConfig.__dataclass_fields__
        }
        cfg = LiquidityVacuumConfig(**cfg_dict)
        t_shock = params.get("t_shock")
        return detect_liquidity_vacuum_events(df, symbol=symbol, cfg=cfg, t_shock=t_shock)


LIQUIDITY_FAMILY_DETECTORS = {
    "LIQUIDITY_SHOCK": LiquidityStressDetector,
    "LIQUIDITY_STRESS_DIRECT": DirectLiquidityStressDetector,
    "LIQUIDITY_STRESS_PROXY": ProxyLiquidityStressDetector,
    "DEPTH_COLLAPSE": DepthCollapseDetector,
    "SPREAD_BLOWOUT": SpreadBlowoutDetector,
    "ORDERFLOW_IMBALANCE_SHOCK": OrderflowImbalanceDetector,
    "ABSORPTION_EVENT": AbsorptionDetector,
    "LIQUIDITY_GAP_PRINT": LiquidityGapDetector,
    "LIQUIDITY_VACUUM": LiquidityVacuumDetector,
}


__all__ = [
    "AbsorptionDetector",
    "BaseLiquidityStressDetector",
    "DepthCollapseDetector",
    "DirectLiquidityStressDetector",
    "LIQUIDITY_FAMILY_DETECTORS",
    "LiquidityGapDetector",
    "LiquidityStressDetector",
    "LiquidityVacuumDetector",
    "OrderflowImbalanceDetector",
    "ProxyLiquidityStressDetector",
    "SpreadBlowoutDetector",
]
