from __future__ import annotations

from typing import Any, Mapping

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
        return {
            'rv_z': rv_z,
            'dynamic_threshold': dynamic_threshold,
            'vol_factor': vol_factor,
            'canonical_high_vol': canonical_high_vol,
            'close': pd.to_numeric(df['close'], errors='coerce'),
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (
            features['canonical_high_vol'].fillna(False)
            & (features['rv_z'] >= features['dynamic_threshold']).fillna(False)
        ).fillna(False)

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (features['rv_z'] / features['dynamic_threshold'].replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)


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
        bar_move_bps = close.pct_change().abs() * 10000.0
        move_threshold = bar_move_bps.shift(1).rolling(window=288, min_periods=24).quantile(float(params.get('move_quantile', 0.95))).fillna(float(params.get('move_floor_bps', 25.0)))
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
            'bar_move_bps': bar_move_bps,
            'move_threshold': move_threshold,
            'canonical_high_vol': canonical_high_vol,
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
        meta['bar_move_bps'] = float(np.nan_to_num(features['bar_move_bps'].iloc[idx], nan=0.0))
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
