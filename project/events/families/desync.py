from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.threshold import ThresholdDetector
from project.events.detectors.composite import CompositeDetector
from project.features.rolling_thresholds import lagged_rolling_quantile
from project.events.shared import EVENT_COLUMNS, emit_event, format_event_id
from project.events.thresholding import rolling_mean_std_zscore
from project.research.analyzers import run_analyzer_suite
from project.events.detectors.desync_base import (
    CrossAssetDesyncDetectorV2,
    IndexComponentDivergenceDetectorV2,
    LeadLagBreakDetectorV2,
)
from project.events.registries.desync import (
    DESYNC_DETECTORS,
    ensure_desync_detectors_registered,
    get_desync_detectors,
)
from project.events.detectors.registry import get_detector


class IndexComponentDivergenceDetector(CompositeDetector):
    event_type = "INDEX_COMPONENT_DIVERGENCE"
    required_columns = ("timestamp", "close")

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = df["close"]
        ret_abs = close.pct_change(1).abs()
        basis_z = df.get(
            "basis_zscore", df.get("cross_exchange_spread_z", pd.Series(0.0, index=df.index))
        )
        basis_abs = basis_z.abs()
        window = int(params.get("threshold_window", 2880))
        min_periods = max(window // 10, 1)
        basis_q93 = lagged_rolling_quantile(
            basis_abs, window=window, quantile=float(params.get("basis_quantile", 0.93)), min_periods=min_periods
        )
        ret_q75 = lagged_rolling_quantile(
            ret_abs, window=window, quantile=float(params.get("ret_quantile", 0.75)), min_periods=min_periods
        )
        return {
            "basis_abs": basis_abs,
            "ret_abs": ret_abs,
            "basis_q93": basis_q93,
            "ret_q75": ret_q75,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return (
            (features["basis_abs"] >= features["basis_q93"]).fillna(False)
            & (features["ret_abs"] >= features["ret_q75"]).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return features["basis_abs"]


class LeadLagBreakDetector(ThresholdDetector):
    event_type = "LEAD_LAG_BREAK"
    required_columns = ("timestamp",)

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        basis_z = df.get(
            "basis_zscore", df.get("cross_exchange_spread_z", pd.Series(0.0, index=df.index))
        )
        basis_diff_abs = basis_z.diff().abs()
        window = int(params.get("threshold_window", 2880))
        min_periods = max(window // 10, 1)
        basis_diff_q99 = lagged_rolling_quantile(
            basis_diff_abs, window=window, quantile=float(params.get("basis_diff_quantile", 0.99)), min_periods=min_periods
        )
        return {"basis_diff_abs": basis_diff_abs, "basis_diff_q99": basis_diff_q99}

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return (features["basis_diff_abs"] >= features["basis_diff_q99"]).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return features["basis_diff_abs"]


class CrossAssetDesyncDetector(ThresholdDetector):
    DEFAULT_LOOKBACK_WINDOW = 2880
    DEFAULT_THRESHOLD_Z = 3.0
    DEFAULT_MIN_PAIR_OBSERVATIONS = 96

    """Detects price desynchronization between correlated asset pairs.
    
    Triggered when the spread between two historically correlated assets
    (e.g., BTC and ETH, or SOL and ETH) deviates significantly from its 
    rolling mean, suggesting a lead-lag opportunity or relative value dislocation.
    """
    event_type = "CROSS_ASSET_DESYNC_EVENT"
    required_columns = ("timestamp", "close")

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = pd.to_numeric(df["close"], errors="coerce").astype(float)
        paired_close = pd.to_numeric(
            df.get("pair_close", df.get("close_pair", pd.Series(np.nan, index=df.index))),
            errors="coerce"
        ).astype(float)
        basis_valid = close.notna() & paired_close.notna()
        min_pair_observations = int(params.get("min_pair_observations", self.DEFAULT_MIN_PAIR_OBSERVATIONS))
        lookback_window = int(params.get("lookback_window", self.DEFAULT_LOOKBACK_WINDOW))
        threshold_z = float(params.get("threshold_z", self.DEFAULT_THRESHOLD_Z))

        if int(basis_valid.sum()) < min_pair_observations:
            empty = pd.Series(False, index=df.index, dtype=bool)
            return {
                "basis": pd.Series(np.nan, index=df.index, dtype=float),
                "basis_valid": basis_valid,
                "desync_z": pd.Series(0.0, index=df.index, dtype=float),
                "threshold": pd.Series(threshold_z, index=df.index, dtype=float),
                "onset": empty,
            }

        # Calculate log returns and basis (spread)
        ret = np.log(close / close.shift(1)).fillna(0.0)
        paired_ret = np.log(paired_close / paired_close.shift(1)).fillna(0.0)
        basis = ret - paired_ret
        
        min_periods = max(lookback_window // 10, 1)

        basis_mean = basis.rolling(lookback_window, min_periods=min_periods).mean()
        basis_std = basis.rolling(lookback_window, min_periods=min_periods).std().replace(0.0, np.nan)
        desync_z = (basis - basis_mean) / basis_std
        active = (desync_z.abs() >= threshold_z).fillna(False) & basis_valid.fillna(False)
        onset = (active & ~active.shift(1, fill_value=False)).fillna(False)

        return {
            "desync_z": desync_z.abs().fillna(0.0),
            "threshold": pd.Series(threshold_z, index=df.index, dtype=float),
            "basis": basis,
            "basis_valid": basis_valid,
            "onset": onset,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return features.get("onset", pd.Series(False, index=df.index, dtype=bool)).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return features["desync_z"]

    def compute_direction(self, idx: int, features: dict[str, pd.Series], **params: Any) -> str:
        basis = float(features["basis"].iloc[idx]) if "basis" in features else 0.0
        return "down" if basis > 0 else "up" if basis < 0 else "non_directional"


ensure_desync_detectors_registered()

_DETECTORS = get_desync_detectors()

_LEGACY_DETECTORS = {
    "INDEX_COMPONENT_DIVERGENCE": IndexComponentDivergenceDetector,
    "LEAD_LAG_BREAK": LeadLagBreakDetector,
    "CROSS_ASSET_DESYNC_EVENT": CrossAssetDesyncDetector,
}

_PAIR_COLUMNS = ("pair_close", "close_pair", "component_close", "reference_close")


def _has_pair_inputs(df: pd.DataFrame) -> bool:
    return any(col in df.columns for col in _PAIR_COLUMNS)


def detect_desync_family(
    df: pd.DataFrame, symbol: str, event_type: str = "INDEX_COMPONENT_DIVERGENCE", **params: Any
) -> pd.DataFrame:
    detector = get_detector(event_type)
    if detector is not None:
        if event_type not in _LEGACY_DETECTORS or _has_pair_inputs(df):
            return detector.detect(df, symbol=symbol, **params)
        legacy_detector_cls = _LEGACY_DETECTORS[event_type]
        return legacy_detector_cls().detect(df, symbol=symbol, **params)
    if detector is None:
        # Fallback to BasisDislocationDetector if it's BASIS related
        from project.events.families.basis import BasisDislocationDetector

        if event_type in {"BASIS_DISLOC", "SPOT_PERP_BASIS_SHOCK"}:
            # Map columns for BasisDislocationDetector
            work = df.copy()
            if "close" in work.columns and "close_perp" not in work.columns:
                work["close_perp"] = work["close"]
            if "spot_close" in work.columns and "close_spot" not in work.columns:
                work["close_spot"] = work["spot_close"]
            if "close_spot" not in work.columns:
                # Last resort fallback
                work["close_spot"] = work[
                    "close"
                ]  # Should not happen if canonical feature building is correct
            return BasisDislocationDetector().detect(work, symbol=symbol, **params)
        raise ValueError(f"Unknown desync event type: {event_type}")


def analyze_desync_family(
    df: pd.DataFrame, symbol: str, event_type: str = "INDEX_COMPONENT_DIVERGENCE", **params: Any
) -> tuple[pd.DataFrame, dict[str, Any]]:
    events = detect_desync_family(df, symbol, event_type=event_type, **params)
    market = df[["timestamp", "close"]].copy() if not df.empty and "close" in df.columns else None
    analyzer_results = run_analyzer_suite(events, market=market) if not events.empty else {}
    return events, analyzer_results


IndexComponentDivergenceDetector = IndexComponentDivergenceDetectorV2
LeadLagBreakDetector = LeadLagBreakDetectorV2
CrossAssetDesyncDetector = CrossAssetDesyncDetectorV2
