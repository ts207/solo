from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.base_v2 import BaseDetectorV2
from project.events.episodes import build_episodes
from project.events.event_output_schema import (
    empty_event_output_frame,
    normalize_event_output_frame,
)


class EpisodeBaseDetectorV2(BaseDetectorV2):
    max_gap = 0
    anchor_rule = 'peak'
    cooldown_semantics = "episode_end_plus_cooldown_bars"
    merge_key_strategy = "episode_id"

    def _episode_anchor_idx(self, episode: Any, anchor_rule: str | None) -> int:
        rule = str(anchor_rule or self.anchor_rule).strip().lower()
        if rule in {'start', 'first'}:
            return int(episode.start_idx)
        if rule in {'end', 'last'}:
            return int(episode.end_idx)
        return int(episode.peak_idx)

    def _episode_metadata(self, work: pd.DataFrame, episode: Any, symbol: str, sub_idx: int) -> dict[str, Any]:
        return {
            'cluster_id': self.event_name.lower(),
            'start_idx': int(episode.start_idx),
            'end_idx': int(episode.end_idx),
            'peak_idx': int(episode.peak_idx),
            'duration_bars': int(episode.duration_bars),
            'episode_id': f"{self.event_name.lower()}_{symbol}_{sub_idx:04d}",
        }

    def detect_events(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        self.check_required_columns(df)
        if df.empty:
            return empty_event_output_frame()
        work = df.copy().reset_index(drop=True)
        work['timestamp'] = pd.to_datetime(work['timestamp'], utc=True, errors='coerce')
        features = self.prepare_features(work, **params)
        mask = self.compute_raw_mask(work, features=features, **params)
        intensity_series = self.compute_intensity(work, features=features, **params)
        episodes = build_episodes(mask, score=intensity_series, max_gap=int(params.get('max_gap', self.max_gap)))
        symbol = str(params.get('symbol', 'UNKNOWN')).upper()
        rows = []
        for sub_idx, episode in enumerate(episodes):
            idx = self._episode_anchor_idx(episode, params.get('anchor_rule'))
            ts_anchor = work.at[idx, 'timestamp']
            ts_start = work.at[episode.start_idx, 'timestamp']
            ts_end = work.at[episode.end_idx, 'timestamp']
            if pd.isna(ts_anchor):
                continue
            intensity = float(np.nan_to_num(intensity_series.iloc[idx], nan=1.0))
            meta = self._episode_metadata(work, episode, symbol, sub_idx)
            meta.update(self.compute_metadata(idx, features, **params))
            merge_key = meta['episode_id']
            cooldown_until = self.compute_cooldown_until(idx, ts_end, **params)
            event = self.build_event(
                idx=idx,
                ts_start=ts_start,
                ts_end=ts_end,
                intensity=intensity,
                features=features,
                params=params,
                detector_metadata=meta,
                merge_key=merge_key,
                cooldown_until=cooldown_until,
            )
            rows.append(event.as_dict())
        return normalize_event_output_frame(pd.DataFrame(rows))


class LiquidationCascadeDetectorV2(EpisodeBaseDetectorV2):
    event_name = 'LIQUIDATION_CASCADE'
    required_columns = ('timestamp', 'liquidation_notional', 'oi_delta_1h', 'oi_notional', 'close', 'high', 'low')
    default_liq_multiplier = 3.0
    default_oi_drop_pct_threshold = 0.005
    max_gap = 6
    anchor_rule = 'last'

    @staticmethod
    def _resolve_liq_window(params: dict[str, Any]) -> int:
        return int(params.get('liq_median_window', params.get('median_window', 288)))

    @staticmethod
    def _resolve_liq_abs_floor(params: dict[str, Any], liq: pd.Series | None = None) -> float:
        pct = params.get('liq_vol_th_pct')
        if pct is not None and liq is not None:
            nonzero = liq[liq > 0]
            if len(nonzero) >= 10:
                return float(np.nanpercentile(np.asarray(nonzero, dtype=float), float(pct) * 100.0))
        return float(params.get('liq_vol_th', 0.0) or 0.0)

    @staticmethod
    def _resolve_oi_thresholds(params: dict[str, Any]) -> tuple[float, float | None]:
        pct_value = params.get('oi_drop_pct_th')
        abs_value = params.get('oi_drop_abs_th')
        legacy = params.get('oi_drop_th')
        if pct_value is None and legacy is not None:
            try:
                legacy_f = float(legacy)
            except (TypeError, ValueError):
                legacy_f = 0.0
            if abs(legacy_f) < 1.0:
                pct_value = abs(legacy_f)
            else:
                abs_value = legacy_f
        pct_threshold = (
            float(pct_value)
            if pct_value is not None
            else LiquidationCascadeDetectorV2.default_oi_drop_pct_threshold
        )
        abs_threshold = float(abs_value) if abs_value is not None else None
        return pct_threshold, abs_threshold

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        liq_window = self._resolve_liq_window(params)
        min_periods = int(params.get('min_periods', min(liq_window, 24)))
        liq = pd.to_numeric(df['liquidation_notional'], errors='coerce').astype(float)
        liq_median = liq.shift(1).rolling(window=liq_window, min_periods=min_periods).median().fillna(0.0)
        liq_th = liq_median * float(params.get('liq_multiplier', self.default_liq_multiplier))
        oi_delta = pd.to_numeric(df['oi_delta_1h'], errors='coerce').astype(float)
        oi_notional = pd.to_numeric(df['oi_notional'], errors='coerce').astype(float)
        close = pd.to_numeric(df['close'], errors='coerce').astype(float)
        low = pd.to_numeric(df['low'], errors='coerce').astype(float)
        funding_rate = pd.to_numeric(df.get('funding_rate', pd.Series(np.nan, index=df.index)), errors='coerce').astype(float)
        signed_move_bps = close.pct_change() * 10000.0
        oi_delta_fraction = oi_delta / oi_notional.replace(0.0, np.nan)
        cascade_side = pd.Series('ambiguous', index=df.index, dtype=object)
        cascade_side = cascade_side.mask((signed_move_bps < 0) & (funding_rate > 0), 'longs_liquidated')
        cascade_side = cascade_side.mask((signed_move_bps > 0) & (funding_rate < 0), 'shorts_liquidated')
        liq_abs_floor = self._resolve_liq_abs_floor(params, liq)
        return {
            'liquidation_notional': liq,
            'liq_median': liq_median,
            'liq_th': liq_th,
            'liq_abs_floor': liq_abs_floor,
            'oi_delta_1h': oi_delta,
            'oi_notional': oi_notional,
            'oi_delta_fraction': oi_delta_fraction,
            'funding_rate': funding_rate,
            'signed_move_bps': signed_move_bps,
            'cascade_side': cascade_side,
            'close': close,
            'low': low,
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        pct_threshold, abs_threshold = self._resolve_oi_thresholds(params)
        liq_abs_floor = float(features.get('liq_abs_floor', self._resolve_liq_abs_floor(params)))
        liq = features['liquidation_notional']
        liq_th = features['liq_th']
        oi_delta = features['oi_delta_1h']
        oi_notional = features['oi_notional']
        liq_mask = (liq > liq_th) & (liq > 0)
        if liq_abs_floor > 0:
            liq_mask = liq_mask & (liq >= liq_abs_floor)
        oi_mask = oi_delta < -(oi_notional * pct_threshold)
        if abs_threshold is not None:
            oi_mask = oi_mask & (oi_delta <= abs_threshold)
        return (liq_mask & oi_mask).fillna(False)

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        baseline = features['liq_th'].replace(0.0, np.nan)
        return (features['liquidation_notional'] / baseline).replace([np.inf, -np.inf], np.nan)

    def compute_severity(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        if intensity >= 10.0:
            return 1.0
        if intensity >= 5.0:
            return 0.8
        return 0.5

    def compute_confidence(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> float:
        return 0.9

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        funding = features.get('funding_rate')
        funding_value = float(np.nan_to_num(funding.iloc[idx], nan=np.nan)) if funding is not None else float('nan')
        oi_fraction = features.get('oi_delta_fraction')
        oi_fraction_value = float(np.nan_to_num(oi_fraction.iloc[idx], nan=np.nan)) if oi_fraction is not None else float('nan')
        missing_inputs: list[str] = []
        if funding is None or pd.isna(funding.iloc[idx]):
            missing_inputs.append('funding_rate')
        if pd.isna(features['oi_delta_1h'].iloc[idx]):
            missing_inputs.append('oi_delta_1h')
        if pd.isna(features['oi_notional'].iloc[idx]):
            missing_inputs.append('oi_notional')
        return {
            'event_semantics': 'cascade_episode',
            'detector_family': 'liquidation',
            'directionality': 'cascade_side',
            'cascade_side': str(features.get('cascade_side', pd.Series('ambiguous', index=features['close'].index)).iloc[idx]),
            'signed_move_bps': float(np.nan_to_num(features['signed_move_bps'].iloc[idx], nan=0.0)),
            'liquidation_notional': float(np.nan_to_num(features['liquidation_notional'].iloc[idx], nan=0.0)),
            'oi_delta_1h': float(np.nan_to_num(features['oi_delta_1h'].iloc[idx], nan=0.0)),
            'oi_notional': float(np.nan_to_num(features['oi_notional'].iloc[idx], nan=0.0)),
            'oi_delta_fraction': oi_fraction_value,
            'funding_rate': funding_value,
            'signal_context': {
                'cascade_side': str(features.get('cascade_side', pd.Series('ambiguous', index=features['close'].index)).iloc[idx]),
                'oi_delta_fraction': oi_fraction_value,
                'funding_rate': funding_value,
            },
            'execution_context': {},
            'context_columns_missing': missing_inputs,
            'context_defaulted': [],
            'context_quality': 'degraded' if missing_inputs else 'ok',
        }

    def build_event(self, *, idx: int, ts_start: pd.Timestamp, ts_end: pd.Timestamp, intensity: float, features: Mapping[str, pd.Series], params: dict, detector_metadata: Mapping[str, Any] | None = None, merge_key: str | None = None, cooldown_until: pd.Timestamp | None = None):
        metadata = dict(detector_metadata or {})
        start = int(metadata.get('start_idx', idx))
        end = int(metadata.get('end_idx', idx))
        work = params.get('_work_frame')
        if work is not None:
            subset = work.iloc[start:end+1]
            metadata['total_liquidation_notional'] = float(subset['liquidation_notional'].sum())
            oi_start = float(work['oi_notional'].iloc[max(0, start-1)])
            oi_end = float(work['oi_notional'].iloc[end])
            metadata['oi_reduction_pct'] = (oi_start - oi_end) / oi_start if oi_start > 0 else 0.0
            p_start = float(work['close'].iloc[max(0, start-1)])
            p_low = float(subset['low'].min())
            metadata['price_drawdown'] = (p_start - p_low) / p_start if p_start > 0 else 0.0
        return super().build_event(idx=idx, ts_start=ts_start, ts_end=ts_end, intensity=intensity, features=features, params=params, detector_metadata=metadata, merge_key=merge_key, cooldown_until=cooldown_until)

    def detect_events(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        params = dict(params)
        params['_work_frame'] = df.copy().reset_index(drop=True)
        return super().detect_events(df, params)


class LiquidationCascadeProxyDetectorV2(EpisodeBaseDetectorV2):
    event_name = 'LIQUIDATION_CASCADE_PROXY'
    required_columns = ('timestamp', 'oi_notional', 'oi_delta_1h', 'close', 'high', 'low', 'volume')
    max_gap = 3
    anchor_rule = 'peak'

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> Mapping[str, pd.Series]:
        oi_window = int(params.get('oi_window', 288))
        vol_window = int(params.get('vol_window', 288))
        min_periods = int(params.get('min_periods', 24))
        oi = pd.to_numeric(df['oi_notional'], errors='coerce').fillna(0.0)
        oi_delta = pd.to_numeric(df['oi_delta_1h'], errors='coerce').fillna(0.0)
        vol_col = 'taker_base_volume' if 'taker_base_volume' in df.columns and pd.to_numeric(df['taker_base_volume'], errors='coerce').gt(0).any() else 'volume'
        volume = pd.to_numeric(df[vol_col], errors='coerce').fillna(0.0)
        close = pd.to_numeric(df['close'], errors='coerce')
        low = pd.to_numeric(df['low'], errors='coerce')
        oi_pct_drop = -(oi_delta / oi.replace(0.0, np.nan)).fillna(0.0)
        oi_drop_th = oi_pct_drop.shift(1).rolling(oi_window, min_periods=min_periods).quantile(float(params.get('oi_drop_quantile', 0.95))).fillna(0.01)
        vol_th = volume.shift(1).rolling(vol_window, min_periods=min_periods).quantile(float(params.get('vol_surge_quantile', 0.90))).fillna(0.0)
        ret_window = int(params.get('ret_window', 3))
        rolling_low = low.rolling(ret_window, min_periods=1).min()
        price_drop = -((rolling_low / close.shift(ret_window).replace(0.0, np.nan)) - 1.0).fillna(0.0)
        return {
            'oi': oi,
            'oi_delta': oi_delta,
            'oi_pct_drop': oi_pct_drop,
            'oi_drop_th': oi_drop_th,
            'volume': volume,
            'vol_th': vol_th,
            'price_drop': price_drop,
            'price_drop_th': pd.Series(float(params.get('price_drop_th', 0.003)), index=df.index),
            'close': close,
            'low': low,
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        mask = (
            (features['oi_pct_drop'] >= features['oi_drop_th'])
            & (features['volume'] >= features['vol_th'])
            & (features['price_drop'] >= features['price_drop_th'])
        ).fillna(False)
        warmup = max(int(params.get('oi_window', 288)), int(params.get('vol_window', 288)), int(params.get('ret_window', 3)))
        if warmup > 0 and len(mask) > 0:
            mask = mask.copy()
            mask.iloc[:warmup] = False
        return mask

    def compute_intensity(self, df: pd.DataFrame, *, features: Mapping[str, pd.Series], **params: Any) -> pd.Series:
        oi_ratio = features['oi_pct_drop'] / features['oi_drop_th'].replace(0.0, np.nan)
        vol_ratio = features['volume'] / features['vol_th'].replace(0.0, np.nan)
        price_ratio = features['price_drop'] / features['price_drop_th'].replace(0.0, np.nan)
        return (oi_ratio * vol_ratio * price_ratio).replace([np.inf, -np.inf], np.nan)

    def compute_severity(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        return 1.0 if intensity >= 6.0 else 0.75 if intensity >= 3.0 else 0.45

    def compute_confidence(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> float:
        return float(max(0.0, min(1.0, 0.45 + float(np.nan_to_num(features['oi_pct_drop'].iloc[idx], nan=0.0)) * 4.0)))

    def compute_data_quality(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> str:
        return 'degraded'

    def compute_metadata(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> Mapping[str, Any]:
        return {'event_semantics': 'cascade_proxy_episode', 'evidence_tier': 'proxy'}

    def build_event(self, *, idx: int, ts_start: pd.Timestamp, ts_end: pd.Timestamp, intensity: float, features: Mapping[str, pd.Series], params: dict, detector_metadata: Mapping[str, Any] | None = None, merge_key: str | None = None, cooldown_until: pd.Timestamp | None = None):
        metadata = dict(detector_metadata or {})
        start = int(metadata.get('start_idx', idx))
        end = int(metadata.get('end_idx', idx))
        work = params.get('_work_frame')
        if work is not None:
            subset = work.iloc[start:end+1]
            oi_start = float(work['oi_notional'].iloc[max(0, start-1)])
            oi_end = float(work['oi_notional'].iloc[end])
            metadata['oi_reduction_pct'] = (oi_start - oi_end) / oi_start if oi_start > 0 else 0.0
            p_start = float(work['close'].iloc[max(0, start-1)])
            p_low = float(subset['low'].min())
            metadata['price_drawdown'] = (p_start - p_low) / p_start if p_start > 0 else 0.0
        return super().build_event(idx=idx, ts_start=ts_start, ts_end=ts_end, intensity=intensity, features=features, params=params, detector_metadata=metadata, merge_key=merge_key, cooldown_until=cooldown_until)

    def detect_events(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        params = dict(params)
        params['_work_frame'] = df.copy().reset_index(drop=True)
        return super().detect_events(df, params)
