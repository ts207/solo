from typing import Any, Mapping

import numpy as np
import pandas as pd

from project.events.detectors.base_v2 import FamilyBaseDetector
from project.events.thresholding import dynamic_quantile_floor, rolling_vol_regime_factor
from project.features.context_guards import state_at_least
from project.features.rolling_thresholds import lagged_rolling_quantile

def _ewma_z(series: pd.Series, span: int) -> pd.Series:
    baseline = series.shift(1)
    ewma = baseline.ewm(span=span, adjust=False).mean()
    ewmvar = baseline.ewm(span=span, adjust=False).var()
    ewmstd = np.sqrt(ewmvar)
    return (series - ewma) / ewmstd.replace(0, np.nan)

class VolatilityBaseDetectorV2(FamilyBaseDetector):
    required_columns = ("timestamp", "close", "rv_96", "range_96", "range_med_2880")
    
    def compute_severity(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        if intensity >= 4.0:
            return 1.0 # extreme
        if intensity >= 2.5:
            return 0.7 # major
        return 0.4 # moderate

    def compute_confidence(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> float:
        conf = 0.5
        if features.get("canonical_high_vol") is not None and features["canonical_high_vol"].iloc[idx]:
            conf += 0.3
        return min(conf, 1.0)
        
    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        return {
            "family": "volatility_transition",
            **{
                key: float(value.iloc[idx]) if hasattr(value, "iloc") else value
                for key, value in features.items()
                if key not in {
                    "mask", "intensity", "close", "high", "low", 
                    "rolling_hi", "rolling_lo", "prior_high_96", "prior_low_96"
                }
            },
        }

class VolSpikeDetectorV2(VolatilityBaseDetectorV2):
    event_name = "VOL_SPIKE"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
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

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (
            features["canonical_high_vol"].fillna(False)
            & (features["rv_z"] >= features["dynamic_threshold"]).fillna(False)
        ).fillna(False)

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features["rv_z"].abs()
