from typing import Any, Mapping

import numpy as np
import pandas as pd

from project.events.detectors.base_v2 import FamilyBaseDetector

class BaseLiquidityStressDetectorV2(FamilyBaseDetector):
    """Base logic for liquidity stress detection using v2 schema."""
    
    required_columns = ("timestamp", "close", "high", "low")
    
    default_depth_collapse_threshold = 0.5
    default_spread_spike_threshold = 3.0
    default_imbalance_sensitivity = 0.2
    default_imbalance_threshold = 0.5
    default_depth_floor = 0.1
    default_major_intensity_threshold = 6.0
    default_extreme_intensity_threshold = 12.0

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        depth = features["depth"]
        spread = features["spread"]
        depth_med = features["depth_median"]
        spread_med = features["spread_median"]
        imbalance = features["imbalance"]
        canonical_spread_wide = features.get("canonical_spread_wide")

        depth_collapse_th = float(params.get("depth_collapse_th", self.default_depth_collapse_threshold))
        spread_spike_th = float(params.get("spread_spike_th", self.default_spread_spike_threshold))
        imbalance_sensitivity = float(params.get("imbalance_sensitivity", self.default_imbalance_sensitivity))
        imbalance_th = float(params.get("imbalance_threshold", self.default_imbalance_threshold))

        dynamic_spread_th = spread_spike_th * (1.0 - imbalance_sensitivity * (imbalance.abs() > imbalance_th).astype(float))
        
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

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
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

    def compute_severity(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        extreme_threshold = float(params.get("severity_extreme_threshold", self.default_extreme_intensity_threshold))
        major_threshold = float(params.get("severity_major_threshold", self.default_major_intensity_threshold))
        
        # severity from spread/depth dislocation magnitude
        if intensity >= extreme_threshold:
            return 1.0 # extreme
        if intensity >= major_threshold:
            return 0.7 # major
        return 0.4 # moderate

    def compute_confidence(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> float:
        # Check corroborating dimensions present
        conf = 0.5
        if features.get("canonical_spread_wide") is not None and features["canonical_spread_wide"].iloc[idx]:
            conf += 0.2
        if abs(features.get("imbalance", pd.Series([0.0])).iloc[idx]) > self.default_imbalance_threshold:
            conf += 0.2
            
        evidence_tier = features.get("evidence_tier", pd.Series(["unknown"])).iloc[idx]
        if evidence_tier == "direct":
            conf += 0.1
        elif evidence_tier == "proxy":
            conf -= 0.2
            
        return min(max(conf, 0.0), 1.0)
        
    def compute_data_quality(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        # Simple implementation, can be extended to check data staleness or missing depth_usd
        if pd.isna(features["depth"].iloc[idx]) or pd.isna(features["spread"].iloc[idx]):
            return "degraded"
        return "ok"

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
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

from project.features.context_guards import state_at_least

class DirectLiquidityStressDetectorV2(BaseLiquidityStressDetectorV2):
    """
    Liquidity stress detector using direct L2/L3 aggregate microstructure metrics.
    """

    event_name = "LIQUIDITY_STRESS_DIRECT"
    required_columns = ("timestamp", "close", "high", "low", "depth_usd", "spread_bps")

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
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

class ProxyLiquidityStressDetectorV2(BaseLiquidityStressDetectorV2):
    event_name = "LIQUIDITY_STRESS_PROXY"
    required_columns = ("timestamp", "close", "high", "low")

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
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
            depth_med = depth.shift(1).rolling(window=window, min_periods=min_periods).median()
            depth_source = pd.Series("depth_usd", index=df.index)
        else:
            if "volume" in df.columns:
                vol = pd.to_numeric(df["volume"], errors="coerce").astype(float)
                vol_med = vol.shift(1).rolling(window=window, min_periods=min_periods).median().replace(0.0, np.nan)
                depth = vol / vol_med
                depth_med = pd.Series(1.0, index=df.index)
                depth_source = pd.Series("volume_proxy", index=df.index)
            else:
                depth = pd.Series(1.0, index=df.index)
                depth_med = pd.Series(1.0, index=df.index)
                depth_source = pd.Series("no_proxy", index=df.index)

        return {
            "depth": depth,
            "spread": bar_range_bps,
            "depth_median": depth_med,
            "spread_median": proxy_range_median,
            "imbalance": pd.Series(0.0, index=df.index),
            "canonical_spread_wide": pd.Series(True, index=df.index, dtype=bool),
            "evidence_tier": pd.Series("proxy", index=df.index),
            "depth_source": depth_source,
            "spread_source": pd.Series("range_proxy", index=df.index),
        }

class LiquidityShockDetectorV2(BaseLiquidityStressDetectorV2):
    """
    Polymorphic liquidity stress detector using direct or proxy features.
    """
    event_name = "LIQUIDITY_SHOCK"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        if "depth_usd" in df.columns and "spread_bps" in df.columns:
            return DirectLiquidityStressDetectorV2().prepare_features(df, **params)
        return ProxyLiquidityStressDetectorV2().prepare_features(df, **params)
