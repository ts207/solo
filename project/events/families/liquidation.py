from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from project.events.detectors.episode import EpisodeDetector
from project.events.episodes import build_episodes
from project.events.shared import EVENT_COLUMNS, emit_event, format_event_id
from project.research.analyzers import run_analyzer_suite


def _episode_anchor_idx(episode, anchor_rule: Any, default: str) -> int:
    rule = str(anchor_rule or default).strip().lower()
    if rule in {"start", "first"}:
        return int(episode.start_idx)
    if rule in {"end", "last"}:
        return int(episode.end_idx)
    return int(episode.peak_idx)


class LiquidationCascadeDetector(EpisodeDetector):
    event_type = "LIQUIDATION_CASCADE"
    required_columns = (
        "timestamp",
        "liquidation_notional",
        "oi_delta_1h",
        "oi_notional",
        "close",
        "high",
        "low",
    )
    signal_column = "liquidation_notional"
    threshold = 1.0
    timeframe_minutes = 5
    max_gap = 0
    anchor_rule = "peak"
    default_severity = "major"
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
            else LiquidationCascadeDetector.default_oi_drop_pct_threshold
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

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
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

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        baseline = features["liq_th"].replace(0.0, np.nan)
        intensity = features["liquidation_notional"] / baseline
        return intensity.replace([np.inf, -np.inf], np.nan)

    def detect(self, df: pd.DataFrame, *, symbol: str, **params: Any) -> pd.DataFrame:
        self.check_required_columns(df)
        if df.empty:
            return pd.DataFrame(columns=EVENT_COLUMNS)
        features = self.prepare_features(df, **params)
        mask = self.compute_raw_mask(df, features=features, **params)
        intensity = self.compute_intensity(df, features=features, **params)
        episodes = build_episodes(
            mask, score=intensity, max_gap=int(params.get("max_gap", self.max_gap))
        )
        rows = []
        for sub_idx, episode in enumerate(episodes):
            idx = _episode_anchor_idx(episode, params.get("anchor_rule"), self.anchor_rule)
            ts = pd.to_datetime(df.at[idx, "timestamp"], utc=True, errors="coerce")
            if pd.isna(ts):
                continue
            row = emit_event(
                event_type=self.event_type,
                symbol=symbol,
                event_id=format_event_id(self.event_type, symbol, idx, sub_idx),
                eval_bar_ts=ts,
                direction="down",
                intensity=float(np.nan_to_num(intensity.iloc[idx], nan=1.0)),
                severity=self.default_severity,
                timeframe_minutes=self.timeframe_minutes,
                causal=self.causal,
                metadata={
                    "start_idx": int(episode.start_idx),
                    "end_idx": int(episode.end_idx),
                    "peak_idx": int(episode.peak_idx),
                    "duration_bars": int(episode.duration_bars),
                    "episode_id": f"{self.event_type.lower()}_{symbol}_{sub_idx:04d}",
                },
            )
            row["event_idx"] = idx
            rows.append(row)

        events = pd.DataFrame(rows) if rows else pd.DataFrame(columns=EVENT_COLUMNS)
        if not events.empty:
            # Reconstruct total_liquidation_notional and oi_reduction_pct
            def enrich_row(row):
                start = row.get("start_idx")
                end = row.get("end_idx")
                if pd.notna(start) and pd.notna(end):
                    subset = df.iloc[int(start) : int(end) + 1]
                    row["total_liquidation_notional"] = float(subset["liquidation_notional"].sum())

                    # Compute OI reduction across the whole episode
                    oi_start = float(df["oi_notional"].iloc[max(0, int(start) - 1)])
                    oi_end = float(df["oi_notional"].iloc[int(end)])
                    row["oi_reduction_pct"] = (
                        (oi_start - oi_end) / oi_start if oi_start > 0 else 0.0
                    )
                    # Compute price drawdown
                    p_start = float(df["close"].iloc[max(0, int(start) - 1)])
                    p_low = float(subset["low"].min())
                    row["price_drawdown"] = (p_start - p_low) / p_start if p_start > 0 else 0.0
                else:
                    row["total_liquidation_notional"] = 0.0
                    row["oi_reduction_pct"] = 0.0
                    row["price_drawdown"] = 0.0
                return row

            events = events.apply(enrich_row, axis=1)
        return events


class LiquidationCascadeProxyDetector(EpisodeDetector):
    """OI-native proxy for LIQUIDATION_CASCADE using only Bybit-native data.

    Requires: OI pct drop above rolling quantile + volume surge + directional price drop.
    Does NOT require liquidation_notional.
    """

    event_type = "LIQUIDATION_CASCADE_PROXY"
    required_columns = (
        "timestamp",
        "oi_notional",
        "oi_delta_1h",
        "close",
        "high",
        "low",
        "volume",
    )
    signal_column = "oi_delta_1h"
    timeframe_minutes = 5
    lookback_bars = 288
    warmup_bars = 288
    max_gap = 3
    anchor_rule = "peak"
    default_severity = "major"

    @staticmethod
    def _resolve_min_episode_oi_reduction(params: dict[str, Any]) -> float:
        return float(params.get("min_episode_oi_reduction_pct", 0.0) or 0.0)

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        oi_window = int(params.get("oi_window", 288))
        vol_window = int(params.get("vol_window", 288))
        min_periods = int(params.get("min_periods", 24))

        oi = pd.to_numeric(df["oi_notional"], errors="coerce").fillna(0.0)
        oi_delta = pd.to_numeric(df["oi_delta_1h"], errors="coerce").fillna(0.0)
        # prefer taker_base_volume for directional confirmation; fall back to total volume
        vol_col = "taker_base_volume" if "taker_base_volume" in df.columns and pd.to_numeric(df["taker_base_volume"], errors="coerce").gt(0).any() else "volume"
        volume = pd.to_numeric(df[vol_col], errors="coerce").fillna(0.0)
        close = pd.to_numeric(df["close"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")

        # OI pct drop: how large a drop relative to OI size
        oi_pct_drop = -(oi_delta / oi.replace(0.0, np.nan)).fillna(0.0)
        oi_drop_quantile = float(params.get("oi_drop_quantile", 0.95))
        oi_drop_th = (
            oi_pct_drop.shift(1)
            .rolling(oi_window, min_periods=min_periods)
            .quantile(oi_drop_quantile)
            .fillna(0.01)
        )

        # Volume surge threshold
        vol_surge_quantile = float(params.get("vol_surge_quantile", 0.90))
        vol_th = (
            volume.shift(1)
            .rolling(vol_window, min_periods=min_periods)
            .quantile(vol_surge_quantile)
            .fillna(0.0)
        )

        # Short-window price drawdown
        ret_window = int(params.get("ret_window", 3))
        rolling_low = low.rolling(ret_window, min_periods=1).min()
        price_drop = -(
            (rolling_low / close.shift(ret_window).replace(0.0, np.nan)) - 1.0
        ).fillna(0.0)
        price_drop_th = float(params.get("price_drop_th", 0.003))

        return {
            "oi": oi,
            "oi_delta": oi_delta,
            "oi_pct_drop": oi_pct_drop,
            "oi_drop_th": oi_drop_th,
            "volume": volume,
            "vol_th": vol_th,
            "price_drop": price_drop,
            "price_drop_th": price_drop_th,
            "close": close,
            "low": low,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        oi_pct_drop = features["oi_pct_drop"]
        oi_drop_th = features["oi_drop_th"]
        volume = features["volume"]
        vol_th = features["vol_th"]
        price_drop = features["price_drop"]
        price_drop_th = features["price_drop_th"]

        oi_mask = oi_pct_drop >= oi_drop_th
        vol_mask = volume >= vol_th
        price_mask = price_drop >= price_drop_th

        mask = (oi_mask & vol_mask & price_mask).fillna(False)
        warmup = max(
            int(params.get("oi_window", self.lookback_bars)),
            int(params.get("vol_window", self.lookback_bars)),
            int(params.get("ret_window", 3)),
        )
        if warmup > 0 and len(mask) > 0:
            mask = mask.copy()
            mask.iloc[:warmup] = False
        return mask

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        baseline = features["oi_drop_th"].replace(0.0, np.nan)
        intensity = features["oi_pct_drop"] / baseline
        return intensity.replace([np.inf, -np.inf], np.nan)

    def detect(self, df: pd.DataFrame, *, symbol: str, **params: Any) -> pd.DataFrame:
        self.check_required_columns(df)
        if df.empty:
            return pd.DataFrame(columns=EVENT_COLUMNS)
        min_episode_oi_reduction = self._resolve_min_episode_oi_reduction(params)
        features = self.prepare_features(df, **params)
        mask = self.compute_raw_mask(df, features=features, **params)
        intensity = self.compute_intensity(df, features=features, **params)
        episodes = build_episodes(
            mask, score=intensity, max_gap=int(params.get("max_gap", self.max_gap))
        )
        rows = []
        for sub_idx, episode in enumerate(episodes):
            idx = _episode_anchor_idx(episode, params.get("anchor_rule"), self.anchor_rule)
            ts = pd.to_datetime(df.at[idx, "timestamp"], utc=True, errors="coerce")
            if pd.isna(ts):
                continue
            row = emit_event(
                event_type=self.event_type,
                symbol=symbol,
                event_id=format_event_id(self.event_type, symbol, idx, sub_idx),
                eval_bar_ts=ts,
                direction="down",
                intensity=float(np.nan_to_num(intensity.iloc[idx], nan=1.0)),
                severity=self.default_severity,
                timeframe_minutes=self.timeframe_minutes,
                causal=self.causal,
                metadata={
                    "start_idx": int(episode.start_idx),
                    "end_idx": int(episode.end_idx),
                    "peak_idx": int(episode.peak_idx),
                    "duration_bars": int(episode.duration_bars),
                    "episode_id": f"{self.event_type.lower()}_{symbol}_{sub_idx:04d}",
                },
            )
            row["event_idx"] = idx

            # Enrich with OI reduction and price drawdown stats
            start = episode.start_idx
            end = episode.end_idx
            subset = df.iloc[int(start) : int(end) + 1]
            oi_start = float(df["oi_notional"].iloc[max(0, int(start) - 1)])
            oi_end = float(df["oi_notional"].iloc[int(end)])
            row["oi_reduction_pct"] = (oi_start - oi_end) / oi_start if oi_start > 0 else 0.0
            p_start = float(df["close"].iloc[max(0, int(start) - 1)])
            p_low = float(subset["low"].min())
            row["price_drawdown"] = (p_start - p_low) / p_start if p_start > 0 else 0.0
            if min_episode_oi_reduction > 0.0 and row["oi_reduction_pct"] < min_episode_oi_reduction:
                continue
            rows.append(row)

        return pd.DataFrame(rows) if rows else pd.DataFrame(columns=EVENT_COLUMNS)


from project.events.detectors.registry import register_detector
from project.events.detectors.liquidation_base import LiquidationCascadeDetectorV2, LiquidationCascadeProxyDetectorV2

register_detector("LIQUIDATION_CASCADE", LiquidationCascadeDetectorV2)
register_detector("LIQUIDATION_CASCADE_PROXY", LiquidationCascadeProxyDetectorV2)


def detect_liquidation_family(df: pd.DataFrame, symbol: str, **params: Any) -> pd.DataFrame:
    detector = LiquidationCascadeDetectorV2()
    return detector.detect(df, symbol=symbol, **params)


def analyze_liquidation_family(
    df: pd.DataFrame, symbol: str, **params: Any
) -> tuple[pd.DataFrame, dict[str, Any]]:
    events = detect_liquidation_family(df, symbol, **params)
    market = df[["timestamp", "close"]].copy() if not df.empty and "close" in df.columns else None
    analyzer_results = run_analyzer_suite(events, market=market) if not events.empty else {}
    return events, analyzer_results
