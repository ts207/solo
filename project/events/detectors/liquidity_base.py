from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.base_v2 import BaseDetectorV2
from project.features.context_guards import state_at_least


class BaseLiquidityStressDetectorV2(BaseDetectorV2):
    """Wave-1 liquidity detector base with normalized event emission."""

    required_columns = ("timestamp", "close", "high", "low")
    default_depth_collapse_threshold = 0.5
    default_spread_spike_threshold = 3.0
    default_imbalance_threshold = 0.5
    default_depth_floor = 0.1

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> pd.Series:
        depth = features['depth']
        spread = features['spread']
        depth_med = features['depth_median']
        spread_med = features['spread_median']
        imbalance = features['imbalance']
        spread_multiple = float(params.get('spread_spike_th', self.default_spread_spike_threshold))
        depth_collapse = float(params.get('depth_collapse_th', self.default_depth_collapse_threshold))
        dynamic_spread_multiple = spread_multiple * (
            1.0 - 0.2 * (imbalance.abs() > float(params.get('imbalance_threshold', self.default_imbalance_threshold))).astype(float)
        )
        mask = (
            depth.notna()
            & spread.notna()
            & depth_med.notna()
            & spread_med.notna()
            & (depth_med > 0.0)
            & (spread_med > 0.0)
            & (depth < depth_med * depth_collapse)
            & (spread > spread_med * dynamic_spread_multiple)
        )
        canonical_spread_wide = features.get('canonical_spread_wide')
        if canonical_spread_wide is not None:
            mask = mask & canonical_spread_wide.fillna(False)
        return mask.fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any
    ) -> pd.Series:
        depth_ratio = features['depth'] / features['depth_median'].replace(0.0, np.nan)
        spread_ratio = features['spread'] / features['spread_median'].replace(0.0, np.nan)
        imbalance_boost = 1.0 + features['imbalance'].abs()
        score = spread_ratio * (1.0 / depth_ratio.clip(lower=float(params.get('depth_floor', self.default_depth_floor)))) * imbalance_boost
        return score.replace([np.inf, -np.inf], np.nan)

    def compute_severity(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        if intensity >= 12.0:
            return 1.0
        if intensity >= 6.0:
            return 0.75
        return 0.45

    def compute_confidence(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> float:
        evidence_tier = str(features['evidence_tier'].iloc[idx])
        base = 0.55 if evidence_tier == 'direct' else 0.35
        if bool(features.get('canonical_spread_wide', pd.Series(False, index=features['depth'].index)).iloc[idx]):
            base += 0.15
        if abs(float(features['imbalance'].iloc[idx])) >= 0.5:
            base += 0.10
        if evidence_tier == 'proxy':
            base -= 0.10
        return float(max(0.0, min(1.0, base)))

    def compute_data_quality(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        if pd.isna(features['depth'].iloc[idx]) or pd.isna(features['spread'].iloc[idx]):
            return 'degraded'
        return 'ok'

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        depth_val = float(np.nan_to_num(features['depth'].iloc[idx], nan=0.0))
        depth_med = float(np.nan_to_num(features['depth_median'].iloc[idx], nan=0.0))
        spread_val = float(np.nan_to_num(features['spread'].iloc[idx], nan=0.0))
        spread_med = float(np.nan_to_num(features['spread_median'].iloc[idx], nan=0.0))
        return {
            'cluster_id': 'liquidity_stress',
            'evidence_tier': str(features['evidence_tier'].iloc[idx]),
            'depth_source': str(features['depth_source'].iloc[idx]),
            'spread_source': str(features['spread_source'].iloc[idx]),
            'depth_ratio': float(depth_val / max(depth_med, 1e-12)) if depth_med else 0.0,
            'spread_ratio': float(spread_val / max(spread_med, 1e-12)) if spread_med else 0.0,
            'imbalance': float(np.nan_to_num(features['imbalance'].iloc[idx], nan=0.0)),
        }


class DirectLiquidityStressDetectorV2(BaseLiquidityStressDetectorV2):
    event_name = 'LIQUIDITY_STRESS_DIRECT'
    required_columns = ('timestamp', 'close', 'high', 'low', 'depth_usd', 'spread_bps')

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        window = int(params.get('median_window', 288))
        min_periods = int(params.get('min_periods', max(24, window // 12)))
        depth = pd.to_numeric(df['depth_usd'], errors='coerce').astype(float)
        spread = pd.to_numeric(df['spread_bps'], errors='coerce').abs().astype(float)
        imbalance = pd.to_numeric(df.get('ms_imbalance_24', pd.Series(0.0, index=df.index)), errors='coerce').fillna(0.0).astype(float)
        if 'ms_spread_state' in df.columns:
            canonical_spread_wide = state_at_least(
                df,
                'ms_spread_state',
                1.0,
                default_if_absent=True,
                min_confidence=float(params.get('context_min_confidence', 0.55)),
                max_entropy=float(params.get('context_max_entropy', 0.90)),
            )
        else:
            canonical_spread_wide = pd.Series(True, index=df.index, dtype=bool)
        depth_median = depth.shift(1).rolling(window=window, min_periods=min_periods).median()
        spread_median = spread.shift(1).rolling(window=window, min_periods=min_periods).median()
        return {
            'depth': depth,
            'spread': spread,
            'depth_median': depth_median,
            'spread_median': spread_median,
            'imbalance': imbalance,
            'canonical_spread_wide': canonical_spread_wide,
            'evidence_tier': pd.Series('direct', index=df.index),
            'depth_source': pd.Series('depth_usd', index=df.index),
            'spread_source': pd.Series('spread_bps', index=df.index),
        }


class ProxyLiquidityStressDetectorV2(BaseLiquidityStressDetectorV2):
    event_name = 'LIQUIDITY_STRESS_PROXY'
    required_columns = ('timestamp', 'close', 'high', 'low')

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        window = int(params.get('median_window', 288))
        min_periods = int(params.get('min_periods', max(24, window // 12)))
        high = pd.to_numeric(df['high'], errors='coerce').astype(float)
        low = pd.to_numeric(df['low'], errors='coerce').astype(float)
        close = pd.to_numeric(df['close'], errors='coerce').replace(0.0, np.nan).astype(float)
        range_bps = ((high - low) / close).abs() * float(params.get('bp_scale', 10000.0))
        spread_median = range_bps.shift(1).rolling(window=window, min_periods=min_periods).median()
        if 'volume' in df.columns:
            volume = pd.to_numeric(df['volume'], errors='coerce').astype(float)
            depth = volume
            depth_source = pd.Series('volume_proxy', index=df.index)
        else:
            depth = pd.Series(1.0, index=df.index)
            depth_source = pd.Series('range_proxy', index=df.index)
        depth_median = depth.shift(1).rolling(window=window, min_periods=min_periods).median().fillna(1.0)
        return {
            'depth': depth,
            'spread': range_bps,
            'depth_median': depth_median,
            'spread_median': spread_median,
            'imbalance': pd.Series(0.0, index=df.index),
            'canonical_spread_wide': pd.Series(True, index=df.index, dtype=bool),
            'evidence_tier': pd.Series('proxy', index=df.index),
            'depth_source': depth_source,
            'spread_source': pd.Series('bar_range_bps', index=df.index),
        }

    def compute_data_quality(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        return 'degraded'


class LiquidityShockDetectorV2(BaseLiquidityStressDetectorV2):
    event_name = 'LIQUIDITY_SHOCK'

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        if 'depth_usd' in df.columns and 'spread_bps' in df.columns:
            return DirectLiquidityStressDetectorV2().prepare_features(df, **params)
        return ProxyLiquidityStressDetectorV2().prepare_features(df, **params)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        meta = dict(super().compute_metadata(idx, features, **params))
        meta['cluster_id'] = 'liquidity_shock'
        meta['event_semantics'] = 'shock_onset_only'
        return meta

    def compute_severity(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        if intensity >= 15.0:
            return 1.0
        if intensity >= 8.0:
            return 0.8
        return 0.5


class DepthCollapseDetectorV2(BaseLiquidityStressDetectorV2):
    event_name = 'DEPTH_COLLAPSE'
    required_columns = ('timestamp', 'close', 'high', 'low', 'depth_usd')

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        spread_source = 'spread_bps' if 'spread_bps' in df.columns else 'bar_range_bps'
        if 'spread_bps' in df.columns:
            spread = pd.to_numeric(df['spread_bps'], errors='coerce').abs().astype(float)
        else:
            high = pd.to_numeric(df['high'], errors='coerce').astype(float)
            low = pd.to_numeric(df['low'], errors='coerce').astype(float)
            close = pd.to_numeric(df['close'], errors='coerce').replace(0.0, np.nan).astype(float)
            spread = ((high - low) / close).abs() * 10000.0
        window = int(params.get('median_window', 288))
        min_periods = int(params.get('min_periods', max(24, window // 12)))
        depth = pd.to_numeric(df['depth_usd'], errors='coerce').astype(float)
        depth_median = depth.shift(1).rolling(window=window, min_periods=min_periods).median()
        spread_median = spread.shift(1).rolling(window=window, min_periods=min_periods).median().fillna(spread)
        return {
            'depth': depth,
            'spread': spread,
            'depth_median': depth_median,
            'spread_median': spread_median,
            'imbalance': pd.to_numeric(df.get('ms_imbalance_24', pd.Series(0.0, index=df.index)), errors='coerce').fillna(0.0).astype(float),
            'canonical_spread_wide': pd.Series(True, index=df.index, dtype=bool),
            'evidence_tier': pd.Series('direct', index=df.index),
            'depth_source': pd.Series('depth_usd', index=df.index),
            'spread_source': pd.Series(spread_source, index=df.index),
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        depth = features['depth']
        depth_med = features['depth_median']
        depth_collapse = float(params.get('depth_collapse_th', 0.35))
        spread = features['spread']
        spread_med = features['spread_median'].replace(0.0, np.nan)
        return (
            depth.notna() & depth_med.notna() & (depth_med > 0.0)
            & (depth < depth_med * depth_collapse)
            & ((spread / spread_med).fillna(1.0) >= float(params.get('spread_confirm_multiple', 1.25)))
        ).fillna(False)

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        depth_ratio = (features['depth'] / features['depth_median'].replace(0.0, np.nan)).clip(lower=0.05)
        spread_ratio = (features['spread'] / features['spread_median'].replace(0.0, np.nan)).fillna(1.0)
        return ((1.0 / depth_ratio) * spread_ratio).replace([np.inf, -np.inf], np.nan)

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        meta = dict(super().compute_metadata(idx, features, **params))
        meta['cluster_id'] = 'depth_collapse'
        meta['event_semantics'] = 'depth_failure_only'
        return meta


class LiquidityGapDetectorV2(BaseDetectorV2):
    event_name = 'LIQUIDITY_GAP_PRINT'
    required_columns = ('timestamp', 'close', 'high', 'low')

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        close = pd.to_numeric(df['close'], errors='coerce').astype(float)
        prev_close = close.shift(1)
        high = pd.to_numeric(df['high'], errors='coerce').astype(float)
        low = pd.to_numeric(df['low'], errors='coerce').astype(float)
        gap_bps = ((close / prev_close.replace(0.0, np.nan)) - 1.0).abs() * float(params.get('bp_scale', 10000.0))
        range_bps = ((high - low) / close.replace(0.0, np.nan)).abs() * float(params.get('bp_scale', 10000.0))
        window = int(params.get('threshold_window', 288))
        min_periods = int(params.get('min_periods', max(24, window // 12)))
        gap_q = gap_bps.shift(1).rolling(window=window, min_periods=min_periods).quantile(float(params.get('gap_quantile', 0.97)))
        range_q = range_bps.shift(1).rolling(window=window, min_periods=min_periods).quantile(float(params.get('range_quantile', 0.90)))
        volume = pd.to_numeric(df.get('volume', pd.Series(np.nan, index=df.index)), errors='coerce').astype(float)
        vol_q = volume.shift(1).rolling(window=window, min_periods=min_periods).quantile(float(params.get('volume_quantile', 0.80)))
        return {
            'close': close,
            'prev_close': prev_close,
            'gap_bps': gap_bps,
            'range_bps': range_bps,
            'gap_q': gap_q,
            'range_q': range_q,
            'volume': volume,
            'vol_q': vol_q,
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        return (
            features['prev_close'].notna()
            & (features['gap_bps'] >= features['gap_q'].combine_first(pd.Series(float(params.get('gap_floor_bps', 20.0)), index=features['gap_bps'].index)))
            & (features['range_bps'] >= features['range_q'].combine_first(pd.Series(float(params.get('range_floor_bps', 30.0)), index=features['range_bps'].index)))
        ).fillna(False)

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        baseline = features['gap_q'].combine_first(pd.Series(float(params.get('gap_floor_bps', 20.0)), index=features['gap_bps'].index)).replace(0.0, np.nan)
        return ((features['gap_bps'] / baseline) * (features['range_bps'] / features['range_q'].replace(0.0, np.nan).fillna(1.0))).replace([np.inf, -np.inf], np.nan)

    def compute_severity(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        return 1.0 if intensity >= 6.0 else 0.75 if intensity >= 3.0 else 0.45

    def compute_confidence(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> float:
        base = 0.45
        if pd.notna(features['volume'].iloc[idx]) and pd.notna(features['vol_q'].iloc[idx]) and features['volume'].iloc[idx] >= features['vol_q'].iloc[idx]:
            base += 0.15
        return float(max(0.0, min(1.0, base)))

    def compute_data_quality(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        return 'ok' if pd.notna(features['prev_close'].iloc[idx]) else 'invalid'

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        return {
            'cluster_id': 'liquidity_gap_print',
            'event_semantics': 'gap_print_only',
            'gap_bps': float(np.nan_to_num(features['gap_bps'].iloc[idx], nan=0.0)),
            'range_bps': float(np.nan_to_num(features['range_bps'].iloc[idx], nan=0.0)),
        }


class LiquidityVacuumDetectorV2(BaseLiquidityStressDetectorV2):
    event_name = 'LIQUIDITY_VACUUM'

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        if 'depth_usd' in df.columns and 'spread_bps' in df.columns:
            return DirectLiquidityStressDetectorV2().prepare_features(df, **params)
        features = ProxyLiquidityStressDetectorV2().prepare_features(df, **params)
        return dict(features)

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        depth_ratio = (features['depth'] / features['depth_median'].replace(0.0, np.nan)).fillna(1.0)
        spread_ratio = (features['spread'] / features['spread_median'].replace(0.0, np.nan)).fillna(1.0)
        return (
            (depth_ratio <= float(params.get('vacuum_depth_ratio_max', 0.30)))
            & (spread_ratio >= float(params.get('vacuum_spread_ratio_min', 2.5)))
        ).fillna(False)

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        depth_ratio = (features['depth'] / features['depth_median'].replace(0.0, np.nan)).clip(lower=0.05)
        spread_ratio = (features['spread'] / features['spread_median'].replace(0.0, np.nan)).clip(lower=0.5)
        return ((1.0 / depth_ratio) * (spread_ratio ** 1.25)).replace([np.inf, -np.inf], np.nan)

    def compute_severity(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        return 1.0 if intensity >= 10.0 else 0.8 if intensity >= 5.0 else 0.55

    def compute_confidence(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> float:
        base = super().compute_confidence(idx, features, **params)
        if str(features['evidence_tier'].iloc[idx]) == 'proxy':
            base -= 0.1
        return float(max(0.0, min(1.0, base)))

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        meta = dict(super().compute_metadata(idx, features, **params))
        meta['cluster_id'] = 'liquidity_vacuum'
        meta['event_semantics'] = 'thin_book_vacuum'
        return meta
