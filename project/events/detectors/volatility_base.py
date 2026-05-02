from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.base_v2 import BaseDetectorV2
from project.events.thresholding import dynamic_quantile_floor, rolling_vol_regime_factor
from project.features.context_guards import state_at_least


def _ewma_z(series: pd.Series, span: int = 288) -> pd.Series:
    baseline = pd.to_numeric(series, errors='coerce').shift(1)
    ewma = baseline.ewm(span=span, adjust=False).mean()
    ewmstd = baseline.ewm(span=span, adjust=False).std().replace(0.0, np.nan)
    fallback = baseline.rolling(window=max(12, span // 6), min_periods=12).std().replace(0.0, np.nan)
    scale = ewmstd.fillna(fallback).fillna(1e-6)
    return (pd.to_numeric(series, errors='coerce') - ewma) / scale



def _series_or_nan(df: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(df.get(column, pd.Series(np.nan, index=df.index)), errors="coerce")


def _present_columns(df: pd.DataFrame, columns: tuple[str, ...]) -> list[str]:
    return [column for column in columns if column in df.columns]


def _missing_columns(df: pd.DataFrame, columns: tuple[str, ...]) -> list[str]:
    return [column for column in columns if column not in df.columns]


def _context_quality(*, missing: list[str], defaulted: list[str]) -> str:
    if defaulted:
        return "defaulted"
    if missing:
        return "degraded"
    return "ok"


class VolatilityBaseDetectorV2(BaseDetectorV2):
    required_columns = ('timestamp', 'close', 'rv_96', 'range_96', 'range_med_2880')

    def compute_severity(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        if intensity >= 4.0:
            return 1.0
        if intensity >= 2.5:
            return 0.75
        return 0.45

    def compute_confidence(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> float:
        conf = 0.55
        if bool(features.get('canonical_high_vol', pd.Series(False, index=features['rv_z'].index)).iloc[idx]):
            conf += 0.20
        if float(np.nan_to_num(features['rv_z'].iloc[idx], nan=0.0)) >= float(np.nan_to_num(features['dynamic_threshold'].iloc[idx], nan=1.0)) * 1.2:
            conf += 0.10
        return float(max(0.0, min(1.0, conf)))

    def compute_data_quality(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        if pd.isna(features['rv_z'].iloc[idx]) or pd.isna(features['dynamic_threshold'].iloc[idx]):
            return 'degraded'
        return 'ok'

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        return {
            'cluster_id': 'vol_regime',
            'event_semantics': 'shock_onset_only',
            'rv_z': float(np.nan_to_num(features['rv_z'].iloc[idx], nan=0.0)),
            'dynamic_threshold': float(np.nan_to_num(features['dynamic_threshold'].iloc[idx], nan=0.0)),
            'vol_factor': float(np.nan_to_num(features.get('vol_factor', pd.Series(1.0, index=features['rv_z'].index)).iloc[idx], nan=1.0)),
        }


class VolSpikeDetectorV2(VolatilityBaseDetectorV2):
    event_name = 'VOL_SPIKE'

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        rv_96 = pd.to_numeric(df['rv_96'], errors='coerce').ffill()
        rv_z = _ewma_z(rv_96, 288)
        vol_factor = rolling_vol_regime_factor(df['range_med_2880'], window=2880)
        canonical_high_vol = state_at_least(
            df,
            'ms_vol_state',
            2.0,
            min_confidence=float(params.get('context_min_confidence', 0.55)),
            max_entropy=float(params.get('context_max_entropy', 0.90)),
        )
        dynamic_threshold = dynamic_quantile_floor(
            rv_z,
            window=2880,
            quantile=float(params.get('quantile', 0.97)),
            floor=float(params.get('expansion_z_threshold', 2.0)) * vol_factor.clip(0.8, 1.5),
        )
        context_columns = ('ms_vol_state', 'ms_vol_confidence', 'ms_vol_entropy')
        context_present = _present_columns(df, context_columns)
        context_missing = _missing_columns(df, context_columns)
        return {
            'rv_z': rv_z,
            'dynamic_threshold': dynamic_threshold,
            'vol_factor': vol_factor,
            'canonical_high_vol': canonical_high_vol,
            'close': pd.to_numeric(df['close'], errors='coerce'),
            'signed_move_bps': pd.to_numeric(df['close'], errors='coerce').pct_change() * 10000.0,
            'ms_vol_state': _series_or_nan(df, 'ms_vol_state'),
            'ms_vol_confidence': _series_or_nan(df, 'ms_vol_confidence'),
            'ms_vol_entropy': _series_or_nan(df, 'ms_vol_entropy'),
            'spread_bps': _series_or_nan(df, 'spread_bps'),
            'depth_usd': _series_or_nan(df, 'depth_usd'),
            'expected_cost_bps': _series_or_nan(df, 'expected_cost_bps'),
            'context_columns_present': context_present,
            'context_columns_missing': context_missing,
            'context_defaulted': [],
            'context_quality': _context_quality(missing=context_missing, defaulted=[]),
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (
            features['canonical_high_vol'].fillna(False)
            & (features['rv_z'] >= features['dynamic_threshold']).fillna(False)
        ).fillna(False)

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (features['rv_z'] / features['dynamic_threshold'].replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)


    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        meta = dict(super().compute_metadata(idx, features, **params))
        meta['event_semantics'] = 'volatility_spike'
        meta['detector_family'] = 'volatility'
        meta['directionality'] = 'signed_move'
        meta['signed_move_bps'] = float(np.nan_to_num(features['signed_move_bps'].iloc[idx], nan=0.0))
        meta['rv_ratio'] = float(np.nan_to_num((features['rv_z'] / features['dynamic_threshold'].replace(0.0, np.nan)).iloc[idx], nan=0.0))
        meta['signal_context'] = {
            'ms_vol_state': float(np.nan_to_num(features['ms_vol_state'].iloc[idx], nan=np.nan)),
            'ms_vol_confidence': float(np.nan_to_num(features['ms_vol_confidence'].iloc[idx], nan=np.nan)),
            'ms_vol_entropy': float(np.nan_to_num(features['ms_vol_entropy'].iloc[idx], nan=np.nan)),
        }
        meta['execution_context'] = {
            'spread_bps': float(np.nan_to_num(features['spread_bps'].iloc[idx], nan=np.nan)),
            'depth_usd': float(np.nan_to_num(features['depth_usd'].iloc[idx], nan=np.nan)),
            'expected_cost_bps': float(np.nan_to_num(features['expected_cost_bps'].iloc[idx], nan=np.nan)),
        }
        meta['context_columns_present'] = list(features.get('context_columns_present', []))
        meta['context_columns_missing'] = list(features.get('context_columns_missing', []))
        meta['context_defaulted'] = list(features.get('context_defaulted', []))
        meta['context_quality'] = str(features.get('context_quality', 'ok'))
        return meta


class VolShockDetectorV2(VolatilityBaseDetectorV2):
    event_name = 'VOL_SHOCK'

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        close = pd.to_numeric(df['close'], errors='coerce')
        rv_96 = pd.to_numeric(df['rv_96'], errors='coerce').ffill()
        rv_z = _ewma_z(rv_96, 288)
        vol_factor = rolling_vol_regime_factor(df['range_med_2880'], window=2880)
        dynamic_threshold = dynamic_quantile_floor(
            rv_z,
            window=2880,
            quantile=float(params.get('quantile', 0.985)),
            floor=float(params.get('shock_z_threshold', 2.25)) * vol_factor.clip(0.8, 1.8),
        )
        signed_move_bps = close.pct_change() * 10000.0
        bar_move_bps = signed_move_bps.abs()
        move_threshold = bar_move_bps.shift(1).rolling(window=288, min_periods=24).quantile(float(params.get('move_quantile', 0.95))).fillna(float(params.get('move_floor_bps', 25.0)))
        context_columns = ('ms_vol_state', 'ms_vol_confidence', 'ms_vol_entropy')
        context_present = _present_columns(df, context_columns)
        context_missing = _missing_columns(df, context_columns)
        context_defaulted = ['ms_vol_state'] if 'ms_vol_state' not in df.columns else []
        canonical_high_vol = state_at_least(
            df,
            'ms_vol_state',
            1.0,
            default_if_absent=True,
            min_confidence=float(params.get('context_min_confidence', 0.55)),
            max_entropy=float(params.get('context_max_entropy', 0.90)),
        )
        return {
            'rv_z': rv_z,
            'dynamic_threshold': dynamic_threshold,
            'vol_factor': vol_factor,
            'signed_move_bps': signed_move_bps,
            'bar_move_bps': bar_move_bps,
            'move_threshold': move_threshold,
            'rv_ratio': rv_z / dynamic_threshold.replace(0.0, np.nan),
            'move_ratio': bar_move_bps / move_threshold.replace(0.0, np.nan),
            'canonical_high_vol': canonical_high_vol,
            'ms_vol_state': _series_or_nan(df, 'ms_vol_state'),
            'ms_vol_confidence': _series_or_nan(df, 'ms_vol_confidence'),
            'ms_vol_entropy': _series_or_nan(df, 'ms_vol_entropy'),
            'spread_bps': _series_or_nan(df, 'spread_bps'),
            'depth_usd': _series_or_nan(df, 'depth_usd'),
            'expected_cost_bps': _series_or_nan(df, 'expected_cost_bps'),
            'context_columns_present': context_present,
            'context_columns_missing': context_missing,
            'context_defaulted': context_defaulted,
            'context_quality': _context_quality(missing=context_missing, defaulted=context_defaulted),
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (
            features['canonical_high_vol'].fillna(False)
            & (features['rv_z'] >= features['dynamic_threshold']).fillna(False)
            & (features['bar_move_bps'] >= features['move_threshold']).fillna(False)
        ).fillna(False)

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        rv_ratio = features['rv_z'] / features['dynamic_threshold'].replace(0.0, np.nan)
        move_ratio = features['bar_move_bps'] / features['move_threshold'].replace(0.0, np.nan)
        return (rv_ratio * move_ratio).replace([np.inf, -np.inf], np.nan)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        meta = dict(super().compute_metadata(idx, features, **params))
        meta['event_semantics'] = 'shock_onset_only'
        meta['detector_family'] = 'volatility'
        meta['directionality'] = 'signed_move'
        meta['signed_move_bps'] = float(np.nan_to_num(features['signed_move_bps'].iloc[idx], nan=0.0))
        meta['bar_move_bps'] = float(np.nan_to_num(features['bar_move_bps'].iloc[idx], nan=0.0))
        meta['move_threshold_bps'] = float(np.nan_to_num(features['move_threshold'].iloc[idx], nan=0.0))
        meta['rv_ratio'] = float(np.nan_to_num(features['rv_ratio'].iloc[idx], nan=0.0))
        meta['move_ratio'] = float(np.nan_to_num(features['move_ratio'].iloc[idx], nan=0.0))
        meta['signal_context'] = {
            'ms_vol_state': float(np.nan_to_num(features['ms_vol_state'].iloc[idx], nan=np.nan)),
            'ms_vol_confidence': float(np.nan_to_num(features['ms_vol_confidence'].iloc[idx], nan=np.nan)),
            'ms_vol_entropy': float(np.nan_to_num(features['ms_vol_entropy'].iloc[idx], nan=np.nan)),
        }
        meta['execution_context'] = {
            'spread_bps': float(np.nan_to_num(features['spread_bps'].iloc[idx], nan=np.nan)),
            'depth_usd': float(np.nan_to_num(features['depth_usd'].iloc[idx], nan=np.nan)),
            'expected_cost_bps': float(np.nan_to_num(features['expected_cost_bps'].iloc[idx], nan=np.nan)),
        }
        meta['context_columns_present'] = list(features.get('context_columns_present', []))
        meta['context_columns_missing'] = list(features.get('context_columns_missing', []))
        meta['context_defaulted'] = list(features.get('context_defaulted', []))
        meta['context_quality'] = str(features.get('context_quality', 'ok'))
        return meta


class VolRelaxationStartDetectorV2(VolatilityBaseDetectorV2):
    event_name = 'VOL_RELAXATION_START'

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        rv_96 = pd.to_numeric(df['rv_96'], errors='coerce').ffill()
        rv_z = _ewma_z(rv_96, 288)
        rv_q95 = rv_z.shift(1).rolling(2880, min_periods=288).quantile(float(params.get('rv_q_start', 0.95)))
        rv_q70 = rv_z.shift(1).rolling(2880, min_periods=288).quantile(float(params.get('rv_q_end', 0.70)))
        canonical_high_vol = state_at_least(
            df,
            'ms_vol_state',
            2.0,
            lag=1,
            default_if_absent=True,
            min_confidence=float(params.get('context_min_confidence', 0.55)),
            max_entropy=float(params.get('context_max_entropy', 0.90)),
        )
        return {
            'rv_z': rv_z,
            'dynamic_threshold': rv_q95,
            'rv_q95': rv_q95,
            'rv_q70': rv_q70,
            'canonical_high_vol': canonical_high_vol,
            'vol_factor': pd.Series(1.0, index=df.index),
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        rv_z = features['rv_z']
        return (
            features['canonical_high_vol'].fillna(False)
            & (rv_z.shift(1) >= features['rv_q95']).fillna(False)
            & (rv_z < features['rv_q70']).fillna(False)
            & (rv_z.diff() < 0).fillna(False)
        ).fillna(False)

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        spread = (features['rv_q95'] - features['rv_q70']).replace(0.0, np.nan)
        return ((features['rv_q95'] - features['rv_z']) / spread).replace([np.inf, -np.inf], np.nan)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        meta = dict(super().compute_metadata(idx, features, **params))
        meta['event_semantics'] = 'relaxation_start_only'
        return meta


class VolClusterShiftDetectorV2(VolatilityBaseDetectorV2):
    event_name = 'VOL_CLUSTER_SHIFT'
    required_columns = ('timestamp', 'rv_96', 'range_96', 'range_med_2880')

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        from project.features.rolling_thresholds import lagged_rolling_quantile
        rv_96 = pd.to_numeric(df['rv_96'], errors='coerce').ffill()
        rv_z = _ewma_z(rv_96, 288)
        rv_diff_abs = rv_z.diff().abs()
        shift_q = float(params.get('shift_quantile', 0.98))
        rv_shift_threshold = lagged_rolling_quantile(rv_diff_abs, window=2880, quantile=shift_q, min_periods=288)
        vol_factor = pd.Series(1.0, index=df.index)
        return {
            'rv_z': rv_z,
            'dynamic_threshold': rv_shift_threshold,
            'rv_diff_abs': rv_diff_abs,
            'rv_shift_threshold': rv_shift_threshold,
            'vol_factor': vol_factor,
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (features['rv_diff_abs'] >= features['rv_shift_threshold']).fillna(False)

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return features['rv_z'].abs().replace([np.inf, -np.inf], np.nan)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        meta = dict(super().compute_metadata(idx, features, **params))
        meta['event_semantics'] = 'cluster_shift'
        meta['cluster_id'] = 'vol_regime'
        return meta


class RangeCompressionDetectorV2(VolatilityBaseDetectorV2):
    event_name = 'RANGE_COMPRESSION_END'
    required_columns = ('timestamp', 'close', 'rv_96', 'range_96', 'range_med_2880')

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        range_96 = pd.to_numeric(df['range_96'], errors='coerce')
        range_med_2880 = pd.to_numeric(df['range_med_2880'], errors='coerce').replace(0.0, np.nan)
        comp_ratio = (range_96 / range_med_2880)
        rv_96 = pd.to_numeric(df['rv_96'], errors='coerce').ffill()
        rv_z = _ewma_z(rv_96, 288)
        vol_factor = pd.Series(1.0, index=df.index)
        return {
            'rv_z': rv_z,
            'dynamic_threshold': pd.Series(float(params.get('compression_ratio_min', 0.95)), index=df.index),
            'comp_ratio': comp_ratio,
            'vol_factor': vol_factor,
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        comp = features['comp_ratio']
        ratio_max = float(params.get('compression_ratio_max', 0.80))
        ratio_min = float(params.get('compression_ratio_min', 0.95))
        return (
            (comp.shift(1) <= ratio_max).fillna(False) & (comp >= ratio_min).fillna(False)
        ).fillna(False)

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (1.0 / features['comp_ratio'].clip(lower=0.1)).replace([np.inf, -np.inf], np.nan)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        meta = dict(super().compute_metadata(idx, features, **params))
        meta['event_semantics'] = 'compression_release'
        meta['cluster_id'] = 'vol_regime'
        return meta


class BreakoutTriggerDetectorV2(VolatilityBaseDetectorV2):
    event_name = 'BREAKOUT_TRIGGER'
    required_columns = ('timestamp', 'close', 'high', 'low', 'rv_96', 'range_96', 'range_med_2880')

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        from project.features.rolling_thresholds import lagged_rolling_quantile
        close = pd.to_numeric(df['close'], errors='coerce')
        high = pd.to_numeric(df.get('high', close), errors='coerce')
        low = pd.to_numeric(df.get('low', close), errors='coerce')
        range_96 = pd.to_numeric(df['range_96'], errors='coerce')
        range_med_2880 = pd.to_numeric(df['range_med_2880'], errors='coerce').replace(0.0, np.nan)
        comp_ratio = (range_96 / range_med_2880)

        lookback = int(params.get('vol_lookback_window', 96))
        rolling_hi = high.rolling(lookback, min_periods=max(1, lookback // 4)).max().shift(1)
        rolling_lo = low.rolling(lookback, min_periods=max(1, lookback // 4)).min().shift(1)
        close_safe = close.replace(0.0, np.nan)
        dist_up = ((close - rolling_hi) / close_safe).clip(lower=0.0)
        dist_down = ((rolling_lo - close) / close_safe).clip(lower=0.0)
        breakout_dist = pd.concat([dist_up, dist_down], axis=1).max(axis=1)

        window = int(params.get('threshold_window', 2880))
        min_periods = max(window // 10, 1)
        breakout_q = lagged_rolling_quantile(
            breakout_dist, window=window,
            quantile=float(params.get('expansion_quantile', 0.85)),
            min_periods=288,
        )
        rv_96 = pd.to_numeric(df['rv_96'], errors='coerce').ffill()
        rv_z = _ewma_z(rv_96, 288)
        vol_factor = pd.Series(1.0, index=df.index)
        breakout_side = pd.Series('ambiguous', index=df.index, dtype=object)
        breakout_side = breakout_side.mask(dist_up > dist_down, 'up')
        breakout_side = breakout_side.mask(dist_down > dist_up, 'down')
        context_columns = ('ms_vol_state', 'ms_vol_confidence', 'ms_vol_entropy')
        context_present = _present_columns(df, context_columns)
        context_missing = _missing_columns(df, context_columns)
        return {
            'rv_z': rv_z,
            'dynamic_threshold': breakout_q,
            'comp_ratio': comp_ratio,
            'rolling_hi': rolling_hi,
            'rolling_lo': rolling_lo,
            'dist_up': dist_up,
            'dist_down': dist_down,
            'breakout_side': breakout_side,
            'breakout_dist': breakout_dist,
            'breakout_threshold': breakout_q,
            'vol_factor': vol_factor,
            'ms_vol_state': _series_or_nan(df, 'ms_vol_state'),
            'ms_vol_confidence': _series_or_nan(df, 'ms_vol_confidence'),
            'ms_vol_entropy': _series_or_nan(df, 'ms_vol_entropy'),
            'spread_bps': _series_or_nan(df, 'spread_bps'),
            'depth_usd': _series_or_nan(df, 'depth_usd'),
            'expected_cost_bps': _series_or_nan(df, 'expected_cost_bps'),
            'context_columns_present': context_present,
            'context_columns_missing': context_missing,
            'context_defaulted': [],
            'context_quality': _context_quality(missing=context_missing, defaulted=[]),
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        comp = features['comp_ratio']
        ratio_max = float(params.get('compression_ratio_max', 0.80))
        min_dist = float(params.get('min_breakout_distance', 0.0015))
        compressed = (comp.shift(1) <= ratio_max).fillna(False)
        breaking_out = (features['breakout_dist'] >= features['breakout_threshold']).fillna(False)
        significant = (features['breakout_dist'] >= min_dist).fillna(False)
        return (compressed & breaking_out & significant).fillna(False)

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        threshold = features['breakout_threshold'].replace(0.0, np.nan)
        return (features['breakout_dist'] / threshold).replace([np.inf, -np.inf], np.nan)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        meta = dict(super().compute_metadata(idx, features, **params))
        meta['event_semantics'] = 'breakout_trigger'
        meta['cluster_id'] = 'trend_momentum'
        meta['detector_family'] = 'volatility_breakout'
        meta['directionality'] = 'breakout_side'
        meta['breakout_side'] = str(features['breakout_side'].iloc[idx])
        meta['breakout_dist'] = float(np.nan_to_num(features['breakout_dist'].iloc[idx], nan=0.0))
        meta['breakout_threshold'] = float(np.nan_to_num(features['breakout_threshold'].iloc[idx], nan=0.0))
        meta['comp_ratio'] = float(np.nan_to_num(features['comp_ratio'].iloc[idx], nan=np.nan))
        meta['rolling_hi'] = float(np.nan_to_num(features['rolling_hi'].iloc[idx], nan=np.nan))
        meta['rolling_lo'] = float(np.nan_to_num(features['rolling_lo'].iloc[idx], nan=np.nan))
        meta['dist_up'] = float(np.nan_to_num(features['dist_up'].iloc[idx], nan=0.0))
        meta['dist_down'] = float(np.nan_to_num(features['dist_down'].iloc[idx], nan=0.0))
        meta['signal_context'] = {
            'ms_vol_state': float(np.nan_to_num(features['ms_vol_state'].iloc[idx], nan=np.nan)),
            'ms_vol_confidence': float(np.nan_to_num(features['ms_vol_confidence'].iloc[idx], nan=np.nan)),
            'ms_vol_entropy': float(np.nan_to_num(features['ms_vol_entropy'].iloc[idx], nan=np.nan)),
        }
        meta['execution_context'] = {
            'spread_bps': float(np.nan_to_num(features['spread_bps'].iloc[idx], nan=np.nan)),
            'depth_usd': float(np.nan_to_num(features['depth_usd'].iloc[idx], nan=np.nan)),
            'expected_cost_bps': float(np.nan_to_num(features['expected_cost_bps'].iloc[idx], nan=np.nan)),
        }
        meta['context_columns_present'] = list(features.get('context_columns_present', []))
        meta['context_columns_missing'] = list(features.get('context_columns_missing', []))
        meta['context_defaulted'] = list(features.get('context_defaulted', []))
        meta['context_quality'] = str(features.get('context_quality', 'ok'))
        return meta


class VolRegimeShiftDetectorV2(VolatilityBaseDetectorV2):
    event_name = 'VOL_REGIME_SHIFT_EVENT'
    required_columns = ('timestamp', 'close', 'rv_96', 'range_96', 'range_med_2880')

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        from project.features.rolling_thresholds import lagged_rolling_quantile
        rv_96 = pd.to_numeric(df['rv_96'], errors='coerce').ffill()
        window = int(params.get('regime_window', 2880))
        min_periods = max(window // 10, 1)
        rv_low_th = lagged_rolling_quantile(
            rv_96, window=window,
            quantile=float(params.get('rv_low_quantile', 0.33)),
            min_periods=min_periods,
        )
        rv_high_th = lagged_rolling_quantile(
            rv_96, window=window,
            quantile=float(params.get('rv_high_quantile', 0.66)),
            min_periods=min_periods,
        )
        rv_z = _ewma_z(rv_96, 288)
        # Use high threshold as the "trigger level" surfaced to base metadata
        vol_factor = pd.Series(1.0, index=df.index)
        return {
            'rv_96': rv_96,
            'rv_z': rv_z,
            'dynamic_threshold': rv_high_th,
            'rv_low_th': rv_low_th,
            'rv_high_th': rv_high_th,
            'vol_factor': vol_factor,
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        rv = features['rv_96']
        hi = features['rv_high_th']
        lo = features['rv_low_th']
        up_shift = (rv > hi).fillna(False) & (rv.shift(1) <= hi.shift(1)).fillna(False)
        down_shift = (rv < lo).fillna(False) & (rv.shift(1) >= lo.shift(1)).fillna(False)
        return (up_shift | down_shift).fillna(False)

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        rv = features['rv_96']
        hi = features['rv_high_th'].replace(0.0, np.nan)
        lo = features['rv_low_th'].replace(0.0, np.nan)
        up_ratio = (rv / hi).clip(lower=0.0)
        down_ratio = (lo / rv.replace(0.0, np.nan)).clip(lower=0.0)
        return pd.concat([up_ratio, down_ratio], axis=1).max(axis=1).replace([np.inf, -np.inf], np.nan)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        meta = dict(super().compute_metadata(idx, features, **params))
        rv = float(np.nan_to_num(features['rv_96'].iloc[idx], nan=0.0))
        hi = float(np.nan_to_num(features['rv_high_th'].iloc[idx], nan=0.0))
        meta['event_semantics'] = 'regime_transition'
        meta['regime_direction'] = 'up' if rv >= hi else 'down'
        meta['cluster_id'] = 'regime_shift'
        return meta
