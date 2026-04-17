from typing import Any, Mapping

import numpy as np
import pandas as pd

from project.events.detectors.base_v2 import FamilyBaseDetector
from project.events.episodes import build_episodes
from project.events.event_output_schema import DetectedEvent

def _episode_anchor_idx(episode, anchor_rule: Any, default: str) -> int:
    rule = str(anchor_rule or default).strip().lower()
    if rule in {"start", "first"}:
        return int(episode.start_idx)
    if rule in {"end", "last"}:
        return int(episode.end_idx)
    return int(episode.peak_idx)

class EpisodeBaseDetectorV2(FamilyBaseDetector):
    max_gap = 0
    anchor_rule = "peak"

    def detect_events(self, df: pd.DataFrame, params: dict) -> pd.DataFrame:
        self.check_required_columns(df)
        if df.empty:
            return pd.DataFrame()

        work = df.copy().reset_index(drop=True)
        work["timestamp"] = pd.to_datetime(work["timestamp"], utc=True, errors="coerce")

        features = self.prepare_features(work, **params)
        mask = self.compute_raw_mask(work, features=features, **params)
        intensity = self.compute_intensity(work, features=features, **params)

        episodes = build_episodes(
            mask, score=intensity, max_gap=int(params.get("max_gap", self.max_gap))
        )
        
        rows = []
        symbol = params.get("symbol", "UNKNOWN")
        timeframe = params.get("timeframe", "5m")
        
        for sub_idx, episode in enumerate(episodes):
            idx = _episode_anchor_idx(episode, params.get("anchor_rule"), self.anchor_rule)
            ts_eval = work.at[idx, "timestamp"]
            ts_start = work.at[episode.start_idx, "timestamp"]
            ts_end = work.at[episode.end_idx, "timestamp"]
            
            if pd.isna(ts_eval):
                continue
                
            intensity_val = float(np.nan_to_num(intensity.iloc[idx], nan=1.0))
            severity = self.compute_severity(idx, intensity_val, features, **params)
            confidence = self.compute_confidence(idx, features, **params)
            quality_flag = self.compute_data_quality(idx, features, **params)
            meta = self.compute_metadata(idx, features, **params)
            
            # Enrich episode metadata
            meta["start_idx"] = int(episode.start_idx)
            meta["end_idx"] = int(episode.end_idx)
            meta["peak_idx"] = int(episode.peak_idx)
            meta["duration_bars"] = int(episode.duration_bars)
            meta["episode_id"] = f"{self.event_name.lower()}_{symbol}_{sub_idx:04d}"
            
            self._enrich_episode_meta(meta, work, features, episode, params)
            
            ev = DetectedEvent(
                event_name=self._contract.event_name,
                event_version=self._contract.event_version,
                detector_class=self._contract.detector_class,
                symbol=symbol,
                timeframe=timeframe,
                ts_start=ts_start,
                ts_end=ts_end,
                canonical_family=self._contract.canonical_family,
                subtype=self._contract.subtype,
                phase=self._contract.phase,
                evidence_mode=self._contract.evidence_mode,
                role=self._contract.role,
                confidence=confidence if self._contract.supports_confidence else None,
                severity=severity if self._contract.supports_severity else None,
                trigger_value=intensity_val,
                threshold_snapshot={"version": self._contract.threshold_schema_version},
                source_features={},
                detector_metadata=meta,
                required_context_present=True,
                data_quality_flag=quality_flag if self._contract.emits_quality_flag else "ok",
                merge_key=meta["episode_id"],
                cooldown_until=None,
            )
            rows.append(vars(ev))
            
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def _enrich_episode_meta(self, meta: dict, df: pd.DataFrame, features: dict, episode: Any, params: dict):
        pass


class LiquidationCascadeDetectorV2(EpisodeBaseDetectorV2):
    event_name = "LIQUIDATION_CASCADE"
    required_columns = (
        "timestamp", "liquidation_notional", "oi_delta_1h", "oi_notional", "close", "high", "low",
    )
    default_liq_multiplier = 3.0
    default_oi_drop_pct_threshold = 0.005

    @staticmethod
    def _resolve_liq_window(params: dict[str, Any]) -> int:
        return int(params.get("liq_median_window", params.get("median_window", 288)))

    @staticmethod
    def _resolve_liq_abs_floor(params: dict[str, Any]) -> float:
        return float(params.get("liq_vol_th", 0.0) or 0.0)

    @staticmethod
    def _resolve_oi_thresholds(params: dict[str, Any]) -> tuple[float | None, float | None]:
        pct_value = params.get("oi_drop_pct_th")
        abs_value = params.get("oi_drop_abs_th")
        legacy = params.get("oi_drop_th")
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

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        liq_window = self._resolve_liq_window(params)
        min_periods = int(params.get("min_periods", min(liq_window, 24)))
        liq = pd.to_numeric(df["liquidation_notional"], errors="coerce").astype(float)
        liq_median = (
            liq.shift(1).rolling(window=liq_window, min_periods=min_periods).median().fillna(0.0)
        )

        liq_multiplier = float(params.get("liq_multiplier", self.default_liq_multiplier))
        liq_th = liq_median * liq_multiplier

        oi_delta = pd.to_numeric(df["oi_delta_1h"], errors="coerce").astype(float)
        oi_notional = pd.to_numeric(df["oi_notional"], errors="coerce").astype(float)
        close = pd.to_numeric(df["close"], errors="coerce").astype(float)
        low = pd.to_numeric(df["low"], errors="coerce").astype(float)
        return {
            "liquidation_notional": liq,
            "liq_median": liq_median,
            "liq_th": liq_th,
            "oi_delta_1h": oi_delta,
            "oi_notional": oi_notional,
            "close": close,
            "low": low,
        }

    def compute_raw_mask(self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any) -> pd.Series:
        liq = features["liquidation_notional"]
        liq_th = features["liq_th"]
        oi_delta = features["oi_delta_1h"]
        oi_notional = features["oi_notional"]
        liq_abs_floor = self._resolve_liq_abs_floor(params)
        oi_drop_pct_th, oi_drop_abs_th = self._resolve_oi_thresholds(params)

        liq_mask = (liq > liq_th) & (liq > 0)
        if liq_abs_floor > 0:
            liq_mask = liq_mask & (liq >= liq_abs_floor)

        oi_mask = oi_delta < -(oi_notional * oi_drop_pct_th)
        if oi_drop_abs_th is not None:
            oi_mask = oi_mask & (oi_delta <= oi_drop_abs_th)

        mask = (liq_mask & oi_mask).fillna(False)
        return mask

    def compute_intensity(self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any) -> pd.Series:
        baseline = features["liq_th"].replace(0.0, np.nan)
        intensity = features["liquidation_notional"] / baseline
        return intensity.replace([np.inf, -np.inf], np.nan)

    def compute_severity(self, idx: int, intensity: float, features: Mapping[str, pd.Series], **params: Any) -> float:
        if intensity >= 10.0:
            return 1.0 # extreme
        if intensity >= 5.0:
            return 0.7 # major
        return 0.4 # moderate

    def compute_confidence(self, idx: int, features: Mapping[str, pd.Series], **params: Any) -> float:
        # direct event with required OI and Price drop, so base confidence is strong
        return 0.9

    def _enrich_episode_meta(self, meta: dict, df: pd.DataFrame, features: dict, episode: Any, params: dict):
        start = episode.start_idx
        end = episode.end_idx
        subset = df.iloc[int(start) : int(end) + 1]
        meta["total_liquidation_notional"] = float(subset["liquidation_notional"].sum())

        oi_start = float(df["oi_notional"].iloc[max(0, int(start) - 1)])
        oi_end = float(df["oi_notional"].iloc[int(end)])
        meta["oi_reduction_pct"] = ((oi_start - oi_end) / oi_start if oi_start > 0 else 0.0)

        p_start = float(df["close"].iloc[max(0, int(start) - 1)])
        p_low = float(subset["low"].min())
        meta["price_drawdown"] = (p_start - p_low) / p_start if p_start > 0 else 0.0
