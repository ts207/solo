from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from project.domain.compiled_registry import get_domain_registry
from project.events.detectors.registry import get_detector
from project.events.detectors.threshold import ThresholdDetector
from project.events.registries.statistical import (
    ensure_statistical_detectors_registered,
    get_statistical_detectors,
)
from project.events.thresholding import rolling_mean_std_zscore
from project.research.analyzers import run_analyzer_suite
from project.spec_registry import load_event_spec


def _band_params(params: dict[str, Any] | None = None) -> tuple[int, int, float]:
    params = params or {}
    try:
        payload = get_domain_registry().event_row("BAND_BREAK")
    except Exception:
        payload = {}
    spec_params = payload.get("parameters", {}) if isinstance(payload, dict) else {}
    lookback = int(params.get("lookback_window", spec_params.get("lookback_window", 96)))
    min_periods = int(
        params.get("min_periods", spec_params.get("min_periods", max(24, lookback // 4)))
    )
    mult = float(
        params.get(
            "band_std_mult",
            params.get(
                "band_z_threshold",
                spec_params.get("band_std_mult", spec_params.get("band_z_threshold", 2.0)),
            ),
        )
    )
    return lookback, min_periods, mult


def _event_params(event_type: str) -> dict[str, Any]:
    spec = load_event_spec(event_type)
    params = spec.get("parameters", {}) if isinstance(spec, dict) else {}
    return dict(params) if isinstance(params, dict) else {}


def _stat_windows(
    event_type: str, params: dict[str, Any]
) -> tuple[dict[str, Any], int, int, int]:
    spec_params = _event_params(event_type)
    lookback_window = int(params.get("lookback_window", spec_params.get("lookback_window", 288)))
    threshold_window = int(
        params.get("threshold_window", spec_params.get("threshold_window", lookback_window * 10))
    )
    min_periods = int(params.get("min_periods", spec_params.get("min_periods", max(24, threshold_window // 10))))
    return spec_params, lookback_window, threshold_window, min_periods


class StatisticalBase(ThresholdDetector):
    required_columns = ("timestamp", "close", "rv_96")
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

    def compute_direction(
        self, idx: int, features: dict[str, pd.Series], **params: Any
    ) -> str:
        for key in ("px_z", "close_ret"):
            sig = features.get(key)
            if sig is not None:
                try:
                    val = float(sig.iloc[idx])
                    if val > 0:
                        return "up"
                    if val < 0:
                        return "down"
                except Exception:
                    pass
        return "non_directional"

    def compute_metadata(
        self, idx: int, features: dict[str, pd.Series], **params: Any
    ) -> dict[str, Any]:
        del params
        return {
            "family": "statistical_dislocation",
            **{
                k: float(v.iloc[idx]) if hasattr(v, "iloc") else v
                for k, v in features.items()
                if k not in ["mask", "intensity", "close"]
            },
        }


class ZScoreStretchDetector(StatisticalBase):
    event_type = "ZSCORE_STRETCH"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = df["close"]
        spec_params, lookback_window, threshold_window, min_periods = _stat_windows(
            self.event_type, params
        )
        px_z = rolling_mean_std_zscore(
            close.pct_change(12).fillna(0.0), window=lookback_window
        )
        px_abs = px_z.abs()
        zscore_quantile = float(
            params.get("zscore_quantile", spec_params.get("zscore_quantile", 0.96))
        )
        px_threshold = (
            px_abs.rolling(threshold_window, min_periods=min_periods)
            .quantile(zscore_quantile)
            .shift(1)
        )
        return {"px_abs": px_abs, "px_threshold": px_threshold, "px_z": px_z}

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        px_abs = features["px_abs"]
        px_threshold = features["px_threshold"]
        threshold = px_threshold.where(px_threshold >= 2.0, 2.0)
        return (px_abs >= threshold).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return features["px_abs"]


class BandBreakDetector(StatisticalBase):
    event_type = "BAND_BREAK"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = df["close"]
        lookback, min_periods, mult = _band_params(params)
        ma = close.rolling(lookback, min_periods=min_periods).mean()
        sd = close.rolling(lookback, min_periods=min_periods).std().replace(0.0, np.nan)
        return {
            "close": close,
            "ma": ma,
            "sd": sd,
            "mult": pd.Series(mult, index=df.index),
            "close_ret": close - ma,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        close = features["close"]
        ma = features["ma"]
        sd = features["sd"]
        mult = features["mult"]
        return ((close > (ma + mult * sd)) | (close < (ma - mult * sd))).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        ma = features["ma"]
        sd = features["sd"]
        return (df["close"] - ma).abs() / sd.fillna(1.0)


class OvershootDetector(StatisticalBase):
    event_type = "OVERSHOOT_AFTER_SHOCK"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = df["close"]
        rv_96 = df["rv_96"].ffill()
        spec_params, lookback_window, threshold_window, min_periods = _stat_windows(
            self.event_type, params
        )
        rv_z = rolling_mean_std_zscore(rv_96, window=lookback_window)
        px_z = rolling_mean_std_zscore(close.pct_change(12).fillna(0.0), window=lookback_window)
        px_abs = px_z.abs()
        price_quantile = float(
            params.get("price_quantile", spec_params.get("price_quantile", 0.95))
        )
        rv_quantile = float(params.get("rv_quantile", spec_params.get("rv_quantile", 0.95)))
        px_threshold = (
            px_abs.rolling(threshold_window, min_periods=min_periods)
            .quantile(price_quantile)
            .shift(1)
        )
        rv_threshold = (
            rv_z.rolling(threshold_window, min_periods=min_periods).quantile(rv_quantile).shift(1)
        )
        return {"rv_z": rv_z, "px_abs": px_abs, "px_threshold": px_threshold, "rv_threshold": rv_threshold, "px_z": px_z}

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        rv_z = features["rv_z"]
        px_abs = features["px_abs"]
        px_threshold = features["px_threshold"]
        rv_threshold = features["rv_threshold"]
        return ((rv_z.shift(1) >= rv_threshold).fillna(False) & (px_abs >= px_threshold).fillna(False)).fillna(
            False
        )

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return features["px_abs"] + features["rv_z"].abs()


class GapOvershootDetector(StatisticalBase):
    event_type = "GAP_OVERSHOOT"

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close_ret = df["close"].pct_change(1)
        ret_abs = close_ret.abs()
        spec_params, _, threshold_window, min_periods = _stat_windows(self.event_type, params)
        return_quantile = float(
            params.get("return_quantile", spec_params.get("return_quantile", 0.995))
        )
        ret_threshold = (
            ret_abs.rolling(threshold_window, min_periods=min_periods)
            .quantile(return_quantile)
            .shift(1)
        )
        return {"ret_abs": ret_abs, "ret_threshold": ret_threshold, "close_ret": close_ret}

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return (features["ret_abs"] >= features["ret_threshold"]).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return features["ret_abs"] * 10000.0  # bps

ensure_statistical_detectors_registered()

_DETECTORS = get_statistical_detectors()


def detect_statistical_family(
    df: pd.DataFrame, symbol: str, event_type: str, **params: Any
) -> pd.DataFrame:
    detector = get_detector(event_type)
    if detector is None:
        raise ValueError(f"Unknown statistical event type: {event_type}")
    return detector.detect(df, symbol=symbol, **params)


def analyze_statistical_family(
    df: pd.DataFrame, symbol: str, event_type: str, **params: Any
) -> tuple[pd.DataFrame, dict[str, Any]]:
    events = detect_statistical_family(df, symbol, event_type, **params)
    market = df[["timestamp", "close"]].copy() if not df.empty and "close" in df.columns else None
    results = run_analyzer_suite(events, market=market) if not events.empty else {}
    return events, results
