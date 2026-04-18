from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.threshold import ThresholdDetector
from project.events.shared import EVENT_COLUMNS, emit_event, format_event_id
from project.events.sparsify import sparsify_mask
from project.events.thresholding import rolling_quantile_threshold, rolling_mean_std_zscore
from project.events.event_aliases import resolve_event_alias
from project.research.analyzers import run_analyzer_suite
from project.spec_registry import load_event_spec


def _numeric_series(df: pd.DataFrame, column: str, *, default: float = 0.0) -> pd.Series:
    if column in df.columns:
        return pd.to_numeric(df[column], errors="coerce").astype(float)
    return pd.Series(default, index=df.index, dtype=float)


def _history_ready(df: pd.DataFrame, min_history_bars: int) -> pd.Series:
    return pd.Series(np.arange(len(df)) >= min_history_bars, index=df.index, dtype=bool)


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return (numerator / denominator.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)


def _event_params(event_type: str) -> dict[str, Any]:
    spec = load_event_spec(event_type)
    params = spec.get("parameters", {}) if isinstance(spec, dict) else {}
    return dict(params) if isinstance(params, dict) else {}


def _rolling_window(event_type: str, params: dict[str, Any]) -> tuple[dict[str, Any], int]:
    spec_params = _event_params(event_type)
    window = int(
        params.get(
            "window",
            params.get(
                "lookback_window",
                spec_params.get("window", spec_params.get("lookback_window", 288)),
            ),
        )
    )
    return spec_params, window


class _CanonicalProxyBase(ThresholdDetector):
    required_columns = ("timestamp", "close", "high", "low")
    timeframe_minutes = 5
    default_severity = "moderate"
    min_spacing = 6
    evidence_tier = "proxy"
    EXTREME_THRESHOLD = 4.0
    MAJOR_THRESHOLD = 2.5

    def compute_severity(
        self, idx: int, intensity: float, features: dict[str, pd.Series], **params: Any
    ) -> str:
        del idx, features, params
        if intensity >= self.EXTREME_THRESHOLD:
            return "extreme"
        if intensity >= self.MAJOR_THRESHOLD:
            return "major"
        return "moderate"

    def compute_metadata(
        self, idx: int, features: dict[str, pd.Series], **params: Any
    ) -> dict[str, Any]:
        del idx, features, params
        return {
            "family": "canonical_proxy",
            "source_event_type": self.source_event_type,
            "evidence_tier": self.evidence_tier,
        }


def _require_columns(df: pd.DataFrame, *, event_type: str, required: tuple[str, ...]) -> None:
    missing = [column for column in required if column not in df.columns]
    if missing:
        names = ", ".join(missing)
        raise ValueError(f"{event_type} requires columns: {names}")


class PriceVolImbalanceProxyDetector(_CanonicalProxyBase):
    event_type = "PRICE_VOL_IMBALANCE_PROXY"
    source_event_type = "ORDERFLOW_IMBALANCE_SHOCK"
    evidence_tier = "hybrid"
    required_columns = _CanonicalProxyBase.required_columns + ("volume", "rv_96")
    min_spacing = 48
    signal_profile = "price_volume_imbalance"
    DEFAULT_RET_QUANTILE = 0.992
    DEFAULT_RV_QUANTILE = 0.90
    DEFAULT_VOL_QUANTILE = 0.90
    DEFAULT_FLOW_QUANTILE = 0.90

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = _numeric_series(df, "close")
        volume = _numeric_series(df, "volume")
        rv = _numeric_series(df, "rv_96")
        ret = close.pct_change(1).fillna(0.0)
        ret_abs = ret.abs()

        ret_window = int(params.get("ret_window", 288))
        rv_window = int(params.get("rv_window", 288))
        vol_window = int(params.get("vol_window", 288))
        flow_window = int(params.get("flow_window", max(ret_window, 144)))
        min_history_bars = int(params.get("min_history_bars", 288))

        rv_z = rolling_mean_std_zscore(rv.ffill(), window=rv_window)
        volume_z = rolling_mean_std_zscore(volume.ffill(), window=vol_window)
        flow_pressure = rolling_mean_std_zscore((ret_abs * volume).ffill(), window=flow_window)
        ret_q = rolling_quantile_threshold(
            ret_abs,
            quantile=float(params.get("ret_quantile", self.DEFAULT_RET_QUANTILE)),
            window=ret_window,
        )
        rv_q = rolling_quantile_threshold(
            rv_z.clip(lower=0.0),
            quantile=float(params.get("rv_quantile", self.DEFAULT_RV_QUANTILE)),
            window=rv_window,
        ).fillna(0.0)
        vol_q = rolling_quantile_threshold(
            volume_z.clip(lower=0.0),
            quantile=float(params.get("volume_quantile", self.DEFAULT_VOL_QUANTILE)),
            window=vol_window,
        ).fillna(0.0)
        flow_q = rolling_quantile_threshold(
            flow_pressure.clip(lower=0.0),
            quantile=float(params.get("flow_quantile", self.DEFAULT_FLOW_QUANTILE)),
            window=flow_window,
        ).fillna(0.0)
        history_ready = _history_ready(df, min_history_bars)

        signal = (
            _safe_ratio(ret_abs, ret_q)
            + _safe_ratio(rv_z.clip(lower=0.0), rv_q)
            + _safe_ratio(volume_z.clip(lower=0.0), vol_q)
            + _safe_ratio(flow_pressure.clip(lower=0.0), flow_q)
        ) / 4.0

        return {
            "ret": ret,
            "ret_abs": ret_abs,
            "rv_z": rv_z,
            "volume_z": volume_z,
            "flow_pressure": flow_pressure,
            "ret_q": ret_q,
            "rv_q": rv_q,
            "vol_q": vol_q,
            "flow_q": flow_q,
            "history_ready": history_ready,
            "signal": signal,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        return (
            features["history_ready"]
            & (features["ret_abs"] >= features["ret_q"]).fillna(False)
            & (features["rv_z"].clip(lower=0.0) >= features["rv_q"]).fillna(False)
            & (features["volume_z"].clip(lower=0.0) >= features["vol_q"]).fillna(False)
            & (features["flow_pressure"].clip(lower=0.0) >= features["flow_q"]).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return features["signal"].fillna(0.0)


class WickReversalProxyDetector(_CanonicalProxyBase):
    event_type = "WICK_REVERSAL_PROXY"
    source_event_type = "SWEEP_STOPRUN"
    evidence_tier = "hybrid"
    signal_profile = "wick_reversal"
    DEFAULT_WICK_QUANTILE = 0.97
    DEFAULT_DOMINANCE_QUANTILE = 0.85
    DEFAULT_RECLAIM_QUANTILE = 0.80
    DEFAULT_RET_QUANTILE = 0.90
    DEFAULT_RANGE_QUANTILE = 0.90
    DEFAULT_VOLUME_QUANTILE = 0.90
    DEFAULT_SIGNAL_COMPONENTS = 6.0
    DEFAULT_STRUCTURE_SCORE_THRESHOLD = 2.60
    DEFAULT_RANGE_RATIO_THRESHOLD = 1.75
    DEFAULT_VOLUME_Z_FLOOR = 2.0

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = _numeric_series(df, "close")
        high = _numeric_series(df, "high")
        low = _numeric_series(df, "low")
        open_proxy = close.shift(1).fillna(close)
        bar_high = pd.concat([high, open_proxy, close], axis=1).max(axis=1)
        bar_low = pd.concat([low, open_proxy, close], axis=1).min(axis=1)
        bar_range = (bar_high - bar_low).replace(0.0, np.nan)

        upper_wick = (bar_high - pd.concat([open_proxy, close], axis=1).max(axis=1)).clip(lower=0.0)
        lower_wick = (pd.concat([open_proxy, close], axis=1).min(axis=1) - bar_low).clip(lower=0.0)
        wick_total = (upper_wick + lower_wick).clip(lower=0.0)
        wick_ratio = _safe_ratio(wick_total, bar_range).fillna(0.0)
        dominant_wick = pd.concat([upper_wick, lower_wick], axis=1).max(axis=1)
        wick_dominance = _safe_ratio(dominant_wick, wick_total).fillna(0.0)
        reclaim = (1.0 - _safe_ratio((close - open_proxy).abs(), bar_range)).clip(lower=0.0, upper=1.0)
        ret_abs = close.pct_change(1).abs()
        volume = _numeric_series(df, "volume")
        volume_z = rolling_mean_std_zscore(volume.ffill(), window=int(params.get("window", 288)))

        window = int(params.get("window", 288))
        min_history_bars = int(params.get("min_history_bars", 288))
        wick_q = rolling_quantile_threshold(
            wick_ratio,
            quantile=float(params.get("wick_quantile", self.DEFAULT_WICK_QUANTILE)),
            window=window,
        )
        dominance_q = rolling_quantile_threshold(
            wick_dominance,
            quantile=float(params.get("dominance_quantile", self.DEFAULT_DOMINANCE_QUANTILE)),
            window=window,
        )
        reclaim_q = rolling_quantile_threshold(
            reclaim,
            quantile=float(params.get("reclaim_quantile", self.DEFAULT_RECLAIM_QUANTILE)),
            window=window,
        )
        ret_q = rolling_quantile_threshold(
            ret_abs,
            quantile=float(params.get("ret_quantile", self.DEFAULT_RET_QUANTILE)),
            window=window,
        )
        range_q = rolling_quantile_threshold(
            bar_range,
            quantile=float(params.get("range_quantile", self.DEFAULT_RANGE_QUANTILE)),
            window=window,
        )
        volume_q = rolling_quantile_threshold(
            volume_z.clip(lower=0.0),
            quantile=float(params.get("volume_quantile", self.DEFAULT_VOLUME_QUANTILE)),
            window=window,
        ).fillna(0.0)
        history_ready = _history_ready(df, min_history_bars)
        signal = (
            _safe_ratio(wick_ratio, wick_q)
            + _safe_ratio(wick_dominance, dominance_q)
            + _safe_ratio(reclaim, reclaim_q)
            + _safe_ratio(ret_abs, ret_q)
            + _safe_ratio(bar_range, range_q)
            + _safe_ratio(volume_z.clip(lower=0.0), volume_q)
        ) / self.DEFAULT_SIGNAL_COMPONENTS

        return {
            "open_proxy": open_proxy,
            "bar_range": bar_range,
            "upper_wick": upper_wick,
            "lower_wick": lower_wick,
            "wick_total": wick_total,
            "wick_ratio": wick_ratio,
            "wick_dominance": wick_dominance,
            "reclaim": reclaim,
            "ret_abs": ret_abs,
            "volume_z": volume_z,
            "wick_q": wick_q,
            "dominance_q": dominance_q,
            "reclaim_q": reclaim_q,
            "ret_q": ret_q,
            "range_q": range_q,
            "volume_q": volume_q,
            "history_ready": history_ready,
            "signal": signal,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        structure_score = (
            _safe_ratio(features["wick_ratio"], features["wick_q"])
            + _safe_ratio(features["wick_dominance"], features["dominance_q"])
            + _safe_ratio(features["reclaim"], features["reclaim_q"])
        ).fillna(0.0)
        range_ratio = _safe_ratio(features["bar_range"], features["range_q"]).fillna(0.0)
        volume_floor = float(params.get("volume_z_floor", self.DEFAULT_VOLUME_Z_FLOOR))
        structure_threshold = float(
            params.get("structure_score_threshold", self.DEFAULT_STRUCTURE_SCORE_THRESHOLD)
        )
        range_threshold = float(
            params.get("range_ratio_threshold", self.DEFAULT_RANGE_RATIO_THRESHOLD)
        )
        volume_threshold = features["volume_q"].where(features["volume_q"] >= volume_floor, volume_floor)
        return (
            features["history_ready"]
            & (structure_score >= structure_threshold).fillna(False)
            & (range_ratio >= range_threshold).fillna(False)
            & (features["volume_z"] >= volume_threshold).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return features["signal"].fillna(0.0)


class AbsorptionProxyDetector(_CanonicalProxyBase):
    event_type = "ABSORPTION_PROXY"
    source_event_type = "ABSORPTION_EVENT"
    evidence_tier = "hybrid"
    min_spacing = 96
    signal_profile = "liquidity_absorption"
    DEFAULT_SPREAD_QUANTILE = 0.965
    DEFAULT_RV_QUANTILE = 0.90
    DEFAULT_IMBALANCE_QUANTILE = 0.25
    DEFAULT_ABSORPTION_QUANTILE = 0.90

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        _require_columns(
            df, event_type=self.event_type, required=("spread_zscore", "rv_96", "imbalance")
        )
        spread = _numeric_series(df, "spread_zscore")
        rv = _numeric_series(df, "rv_96")
        imbalance_abs = _numeric_series(df, "imbalance").abs()
        window = int(params.get("window", 288))
        min_history_bars = int(params.get("min_history_bars", 288))

        spread_hi = rolling_quantile_threshold(
            spread.ffill(),
            quantile=float(params.get("spread_quantile", self.DEFAULT_SPREAD_QUANTILE)),
            window=window,
        )
        rv_z = rolling_mean_std_zscore(rv.ffill(), window=window)
        rv_hi = rolling_quantile_threshold(
            rv_z.clip(lower=0.0).ffill(),
            quantile=float(params.get("rv_quantile", self.DEFAULT_RV_QUANTILE)),
            window=window,
        )
        imbalance_low = rolling_quantile_threshold(
            imbalance_abs.ffill(),
            quantile=float(params.get("imbalance_abs_quantile", self.DEFAULT_IMBALANCE_QUANTILE)),
            window=window,
        )
        absorption_score = _safe_ratio(spread_hi + rv_hi, imbalance_abs + 1e-9)
        absorption_q = rolling_quantile_threshold(
            absorption_score.ffill().replace([np.inf, -np.inf], np.nan).fillna(0.0),
            quantile=float(params.get("absorption_quantile", self.DEFAULT_ABSORPTION_QUANTILE)),
            window=window,
        )
        history_ready = _history_ready(df, min_history_bars)
        signal = (
            _safe_ratio(spread, spread_hi)
            + _safe_ratio(rv_z.clip(lower=0.0), rv_hi)
            + _safe_ratio(absorption_score.clip(lower=0.0), absorption_q)
        ) / 3.0

        return {
            "spread": spread,
            "spread_hi": spread_hi,
            "rv_z": rv_z,
            "rv_hi": rv_hi,
            "imbalance_abs": imbalance_abs,
            "imbalance_low": imbalance_low,
            "absorption_score": absorption_score,
            "absorption_q": absorption_q,
            "history_ready": history_ready,
            "signal": signal,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        return (
            features["history_ready"]
            & (features["spread"] >= features["spread_hi"]).fillna(False)
            & (features["rv_z"].clip(lower=0.0) >= features["rv_hi"]).fillna(False)
            & (features["imbalance_abs"] <= features["imbalance_low"]).fillna(False)
            & (features["absorption_score"] >= features["absorption_q"]).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return features["signal"].fillna(0.0)


class DepthStressProxyDetector(_CanonicalProxyBase):
    event_type = "DEPTH_STRESS_PROXY"
    source_event_type = "DEPTH_COLLAPSE"
    evidence_tier = "hybrid"
    min_spacing = 96
    signal_profile = "depth_stress"
    DEFAULT_SPREAD_QUANTILE = 0.99
    DEFAULT_RV_QUANTILE = 0.90
    DEFAULT_DEPTH_QUANTILE = 0.93
    DEFAULT_STRESS_QUANTILE = 0.90

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        _require_columns(
            df,
            event_type=self.event_type,
            required=("spread_zscore", "rv_96", "micro_depth_depletion"),
        )
        spec_params, window = _rolling_window(self.event_type, params)
        spread = _numeric_series(df, "spread_zscore")
        rv = _numeric_series(df, "rv_96")
        depth_depletion = _numeric_series(df, "micro_depth_depletion")
        min_history_bars = int(params.get("min_history_bars", 288))
        spread_weight = float(params.get("spread_weight", spec_params.get("spread_weight", 0.45)))
        rv_weight = float(params.get("rv_weight", spec_params.get("rv_weight", 0.35)))
        depth_weight = float(params.get("depth_weight", spec_params.get("depth_weight", 0.20)))

        spread_q = rolling_quantile_threshold(
            spread.ffill(),
            quantile=float(params.get("spread_quantile", self.DEFAULT_SPREAD_QUANTILE)),
            window=window,
        )
        rv_z = rolling_mean_std_zscore(rv.ffill(), window=window)
        rv_q = rolling_quantile_threshold(
            rv_z.clip(lower=0.0).ffill(),
            quantile=float(params.get("rv_quantile", self.DEFAULT_RV_QUANTILE)),
            window=window,
        )
        depth_q = rolling_quantile_threshold(
            depth_depletion.ffill(),
            quantile=float(params.get("depth_quantile", self.DEFAULT_DEPTH_QUANTILE)),
            window=window,
        )
        stress_score = (
            _safe_ratio(spread, spread_q) * spread_weight
            + _safe_ratio(rv_z.clip(lower=0.0), rv_q) * rv_weight
            + _safe_ratio(depth_depletion, depth_q) * depth_weight
        )
        stress_q = rolling_quantile_threshold(
            stress_score.ffill().replace([np.inf, -np.inf], np.nan).fillna(0.0),
            quantile=float(params.get("stress_quantile", self.DEFAULT_STRESS_QUANTILE)),
            window=window,
        )
        history_ready = _history_ready(df, min_history_bars)

        return {
            "spread": spread,
            "spread_q": spread_q,
            "rv_z": rv_z,
            "rv_q": rv_q,
            "depth_depletion": depth_depletion,
            "depth_q": depth_q,
            "stress_score": stress_score,
            "stress_q": stress_q,
            "history_ready": history_ready,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df
        return (
            features["history_ready"]
            & (features["spread"] >= features["spread_q"]).fillna(False)
            & (features["rv_z"].clip(lower=0.0) >= features["rv_q"]).fillna(False)
            & (features["depth_depletion"] >= features["depth_q"]).fillna(False)
            & (features["stress_score"] >= features["stress_q"]).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return features["stress_score"].fillna(0.0)


class DepthCollapseDetector(DepthStressProxyDetector):
    """Dedicated detector for DEPTH_COLLAPSE.

    The collapse variant keeps the depth-stress hypothesis but adds a second gate
    for sudden acceleration in the book failure.
    """

    event_type = "DEPTH_COLLAPSE"
    signal_profile = "depth_collapse"
    DEFAULT_SPREAD_QUANTILE = 0.95
    DEFAULT_RV_QUANTILE = 0.85
    DEFAULT_DEPTH_QUANTILE = 0.97
    DEFAULT_STRESS_QUANTILE = 0.92
    DEFAULT_COLLAPSE_QUANTILE = 0.95

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        features = super().prepare_features(df, **params)
        spread_jump = features["spread"].diff().abs().fillna(0.0)
        depth_jump = features["depth_depletion"].diff().abs().fillna(0.0)
        collapse_impulse = (spread_jump + depth_jump).astype(float)
        _, window = _rolling_window(self.event_type, params)
        collapse_q = rolling_quantile_threshold(
            collapse_impulse.ffill(),
            quantile=float(params.get("collapse_quantile", self.DEFAULT_COLLAPSE_QUANTILE)),
            window=window,
        )
        features.update({
            "spread_jump": spread_jump,
            "depth_jump": depth_jump,
            "collapse_impulse": collapse_impulse,
            "collapse_q": collapse_q,
        })
        return features

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        base_mask = super().compute_raw_mask(df, features=features, **params)
        collapse_gate = (features["collapse_impulse"] >= features["collapse_q"]).fillna(False)
        return (base_mask & collapse_gate).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (
            features["stress_score"].fillna(0.0)
            + _safe_ratio(features["collapse_impulse"].fillna(0.0), features["collapse_q"]).fillna(0.0)
        ) / 2.0


class SweepStopRunDetector(WickReversalProxyDetector):
    """Dedicated detector for SWEEP_STOPRUN.

    A stop-run is a wick-reversal with a sharper one-sided sweep, so this variant
    adds explicit sweep dominance and reclaim filters instead of only tightening the
    shared wick thresholds.
    """

    event_type = "SWEEP_STOPRUN"
    source_event_type = "SWEEP_STOPRUN"
    signal_profile = "sweep_stoprun"
    DEFAULT_WICK_QUANTILE = 0.98
    DEFAULT_DOMINANCE_QUANTILE = 0.90
    DEFAULT_RECLAIM_QUANTILE = 0.84
    DEFAULT_RET_QUANTILE = 0.85
    DEFAULT_RANGE_QUANTILE = 0.95
    DEFAULT_SWEEP_CONFIRM_QUANTILE = 0.90

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        features = super().prepare_features(df, **params)
        sweep_dominance = features["wick_dominance"]
        body_ratio = (1.0 - features["reclaim"]).clip(lower=0.0, upper=1.0)
        range_ratio = _safe_ratio(features["bar_range"], features["range_q"]).fillna(0.0)
        sweep_confirm = (sweep_dominance * range_ratio * features["reclaim"]).clip(lower=0.0)
        window = int(params.get("window", 288))
        sweep_q = rolling_quantile_threshold(
            sweep_confirm.ffill().replace([np.inf, -np.inf], np.nan).fillna(0.0),
            quantile=float(params.get("sweep_confirm_quantile", self.DEFAULT_SWEEP_CONFIRM_QUANTILE)),
            window=window,
        )
        features.update({
            "body_ratio": body_ratio,
            "range_ratio": range_ratio,
            "sweep_confirm": sweep_confirm,
            "sweep_q": sweep_q,
        })
        return features

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        base_mask = super().compute_raw_mask(df, features=features, **params)
        return (
            base_mask
            & (features["sweep_confirm"] >= features["sweep_q"]).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (
            features["signal"].fillna(0.0)
            + _safe_ratio(features["sweep_confirm"].fillna(0.0), features["sweep_q"]).fillna(0.0)
        ) / 2.0


class OrderflowImbalanceShockDetector(PriceVolImbalanceProxyDetector):
    """Dedicated detector for ORDERFLOW_IMBALANCE_SHOCK.

    The shock variant keeps the price/volume shock hypothesis but adds a directional
    flow confirmation gate. That keeps it distinct from the generic proxy detector,
    which only looks for broad pressure across returns, volume, and RV.
    """

    event_type = "ORDERFLOW_IMBALANCE_SHOCK"
    source_event_type = "ORDERFLOW_IMBALANCE_SHOCK"
    signal_profile = "directional_flow_shock"
    DEFAULT_RET_QUANTILE = 0.998
    DEFAULT_RV_QUANTILE = 0.92
    DEFAULT_VOL_QUANTILE = 0.92
    DEFAULT_FLOW_QUANTILE = 0.95
    DEFAULT_DIRECTIONAL_FLOW_QUANTILE = 0.98

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        features = super().prepare_features(df, **params)
        directional_flow = features["ret"] * features["flow_pressure"].fillna(0.0)
        flow_window = int(params.get("shock_window", int(params.get("flow_window", 288))))
        directional_flow_q = rolling_quantile_threshold(
            directional_flow.abs().ffill(),
            quantile=float(
                params.get("directional_flow_quantile", self.DEFAULT_DIRECTIONAL_FLOW_QUANTILE)
            ),
            window=flow_window,
        ).fillna(0.0)
        features.update({
            "directional_flow": directional_flow,
            "directional_flow_q": directional_flow_q,
        })
        return features

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        base_mask = super().compute_raw_mask(df, features=features, **params)
        return (
            base_mask
            & (features["directional_flow"].abs() >= features["directional_flow_q"]).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (
            features["signal"].fillna(0.0)
            + _safe_ratio(features["directional_flow"].abs().fillna(0.0), features["directional_flow_q"]).fillna(0.0)
        ) / 2.0


from project.events.registries.canonical_proxy import (
    ensure_canonical_proxy_detectors_registered,
    get_canonical_proxy_detectors,
)

ensure_canonical_proxy_detectors_registered()

_DETECTORS = get_canonical_proxy_detectors()


def detect_canonical_proxy_family(
    df: pd.DataFrame, symbol: str, event_type: str, **params: Any
) -> pd.DataFrame:
    canonical = resolve_event_alias(event_type)
    detector_cls = _DETECTORS.get(canonical)
    if detector_cls is None:
        raise ValueError(f"Unsupported canonical proxy event type: {event_type}")
    return detector_cls().detect(df, symbol=symbol, **params)


def analyze_canonical_proxy_family(
    df: pd.DataFrame, symbol: str, event_type: str, **params: Any
) -> tuple[pd.DataFrame, dict[str, Any]]:
    events = detect_canonical_proxy_family(df, symbol, event_type, **params)
    market = df[["timestamp", "close"]].copy() if not df.empty and "close" in df.columns else None
    analyzer_results = run_analyzer_suite(events, market=market) if not events.empty else {}
    return events, analyzer_results
