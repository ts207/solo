from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from project.core.copula_pairs import copula_pair_universe, copula_partners, load_copula_pairs
from project.events.detectors.threshold import ThresholdDetector
from project.features.context_guards import state_at_least
from project.features.rolling_thresholds import lagged_rolling_quantile
from project.research.analyzers import run_analyzer_suite
from project.spec_registry import load_event_spec


class SessionOpenDetector(ThresholdDetector):
    event_type = "SESSION_OPEN_EVENT"
    required_columns = ("timestamp",)

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        features = {"ts": ts}

        spec = load_event_spec(self.event_type)
        spec_params = spec.get("parameters", {}) if isinstance(spec, dict) else {}
        range_q = float(params.get("session_range_quantile", spec_params.get("session_range_quantile", 0.0)))
        vol_z_min = params.get("session_vol_z_min", spec_params.get("session_vol_z_min"))

        if range_q > 0 and "session_range_pct" in df.columns:
            window = int(params.get("range_window", 20))
            rolling_q = df["session_range_pct"].rolling(window, min_periods=1).quantile(range_q)
            features["range_gate"] = df["session_range_pct"] >= rolling_q
        else:
            features["range_gate"] = pd.Series(True, index=df.index)

        if vol_z_min is not None and "session_vol_z" in df.columns:
            features["vol_gate"] = df["session_vol_z"] >= float(vol_z_min)
        else:
            features["vol_gate"] = pd.Series(True, index=df.index)

        return features

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        ts = features["ts"]
        spec = load_event_spec(self.event_type)
        spec_params = spec.get("parameters", {}) if isinstance(spec, dict) else {}
        hours = spec_params["hours_utc"]
        minute_open = int(params.get("minute_open", spec_params.get("minute_open", 0)))

        time_mask = ((ts.dt.minute == minute_open) & ts.dt.hour.isin(hours)).fillna(False)
        return (time_mask & features["range_gate"] & features["vol_gate"]).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        ts = features["ts"]
        mins = (ts.dt.minute.fillna(59)).astype(float)
        spec = load_event_spec(self.event_type)
        spec_params = spec.get("parameters", {}) if isinstance(spec, dict) else {}
        val = params.get("intensity_scale", spec_params.get("intensity_scale", 60))
        intensity_scale = float(val) if val is not None else float(60)
        return (intensity_scale - mins).clip(lower=0.0)


class SessionCloseDetector(ThresholdDetector):
    event_type = "SESSION_CLOSE_EVENT"
    required_columns = ("timestamp",)

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        features = {"ts": ts}

        spec = load_event_spec(self.event_type)
        spec_params = spec.get("parameters", {}) if isinstance(spec, dict) else {}
        range_q = float(params.get("session_range_quantile", spec_params.get("session_range_quantile", 0.0)))
        vol_z_min = params.get("session_vol_z_min", spec_params.get("session_vol_z_min"))

        if range_q > 0 and "session_range_pct" in df.columns:
            window = int(params.get("range_window", 20))
            rolling_q = df["session_range_pct"].rolling(window, min_periods=1).quantile(range_q)
            features["range_gate"] = df["session_range_pct"] >= rolling_q
        else:
            features["range_gate"] = pd.Series(True, index=df.index)

        if vol_z_min is not None and "session_vol_z" in df.columns:
            features["vol_gate"] = df["session_vol_z"] >= float(vol_z_min)
        else:
            features["vol_gate"] = pd.Series(True, index=df.index)

        return features

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        ts = features["ts"]
        spec = load_event_spec(self.event_type)
        spec_params = spec.get("parameters", {}) if isinstance(spec, dict) else {}
        hours = spec_params["hours_utc"]
        minute_close_start = int(params.get("minute_close_start", spec_params.get("minute_close_start", 55)))

        time_mask = ((ts.dt.minute >= minute_close_start) & ts.dt.hour.isin(hours)).fillna(False)
        return (time_mask & features["range_gate"] & features["vol_gate"]).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        ts = features["ts"]
        mins_to_close = (59 - ts.dt.minute.fillna(0)).abs().astype(float)
        spec = load_event_spec(self.event_type)
        spec_params = spec.get("parameters", {}) if isinstance(spec, dict) else {}
        val = params.get("intensity_scale", spec_params.get("intensity_scale", 60))
        intensity_scale = float(val) if val is not None else float(60)
        return (intensity_scale - mins_to_close).clip(lower=0.0)


class FundingTimestampDetector(ThresholdDetector):
    event_type = "FUNDING_TIMESTAMP_EVENT"
    required_columns = ("timestamp",)

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        funding = df["funding_rate_scaled"].fillna(0.0) if "funding_rate_scaled" in df.columns else pd.Series(0.0, index=df.index)
        features = {"ts": ts, "funding": funding}

        spec = load_event_spec(self.event_type)
        spec_params = spec.get("parameters", {}) if isinstance(spec, dict) else {}
        abs_q = float(params.get("funding_abs_quantile", spec_params.get("funding_abs_quantile", 0.0)))

        if abs_q > 0 and "funding_rate_scaled" in df.columns:
            window = int(params.get("funding_window", 20))
            funding_abs = funding.abs()
            rolling_q = funding_abs.rolling(window, min_periods=1).quantile(abs_q)
            features["funding_gate"] = funding_abs >= rolling_q
        else:
            features["funding_gate"] = pd.Series(True, index=df.index)

        return features

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        ts = features["ts"]
        spec = load_event_spec(self.event_type)
        spec_params = spec.get("parameters", {}) if isinstance(spec, dict) else {}
        hours = spec_params.get("hours_utc", [0, 8, 16])

        time_mask = ((ts.dt.minute == 0) & ts.dt.hour.isin(hours)).fillna(False)
        return (time_mask & features["funding_gate"]).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return features["funding"].abs().fillna(0.0)


class ScheduledNewsDetector(ThresholdDetector):
    event_type = "SCHEDULED_NEWS_WINDOW_EVENT"
    required_columns = ("timestamp",)

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        news_mask = pd.Series(False, index=df.index, dtype=bool)
        for col in [
            "scheduled_news_event",
            "news_event",
            "has_news_event",
            "econ_news_event",
            "macro_news_event",
            "calendar_event",
            "scheduled_event",
        ]:
            if col in df.columns:
                news_mask = df[col].fillna(False).astype(bool)
                break

        # News intensity logic
        news_intensity_cols = [
            c
            for c in ["news_intensity", "calendar_importance", "event_importance", "headline_count"]
            if c in df.columns
        ]
        if news_intensity_cols:
            intensity = sum(
                pd.to_numeric(df[c], errors="coerce").fillna(0.0).abs() for c in news_intensity_cols
            )
        else:
            intensity = pd.Series(1.0, index=df.index)

        return {"ts": ts, "news_mask_col": news_mask, "intensity": intensity}

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        # Always evaluate spec windows — do not short-circuit on column presence.
        ts = features["ts"]
        hh = ts.dt.hour
        mm = ts.dt.minute
        spec = load_event_spec(self.event_type)
        spec_params = spec.get("parameters", {}) if isinstance(spec, dict) else {}
        windows = spec_params.get("windows_utc", [])
        spec_mask = pd.Series(False, index=df.index, dtype=bool)
        for win in windows:
            if not isinstance(win, dict):
                continue
            hour = int(win.get("hour", -1))
            m_start = int(win.get("minute_start", 25))
            m_end = int(win.get("minute_end", 35))
            if hour != -1:
                spec_mask = spec_mask | ((hh == hour) & mm.between(m_start, m_end))
        # Merge column-based mask with spec windows via OR.
        return (spec_mask | features["news_mask_col"]).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return features["intensity"]


class SpreadRegimeWideningDetector(ThresholdDetector):
    """Detects sustained spread widening with positive regime acceleration."""

    event_type = "SPREAD_REGIME_WIDENING_EVENT"
    required_columns = ("timestamp", "volume")
    min_spacing = 48

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        if "spread_zscore" in df.columns:
            spread = pd.to_numeric(df["spread_zscore"], errors="coerce").abs().astype(float)
        elif "spread_bps" in df.columns:
            spread = pd.to_numeric(df["spread_bps"], errors="coerce").abs().astype(float)
        else:
            spread = pd.Series(0.0, index=df.index)
        volume = pd.to_numeric(df["volume"], errors="coerce").astype(float)
        trend_window = int(params.get("trend_window", 24))
        lookback_window = int(params.get("lookback_window", 2880))
        min_periods = int(params.get("min_periods", 288))
        low_volume_quantile = float(params.get("low_volume_quantile", 0.25))

        spread_avg = spread.rolling(trend_window, min_periods=max(4, trend_window // 4)).mean()
        spread_q85 = lagged_rolling_quantile(
            spread,
            window=lookback_window,
            quantile=float(params.get("spread_quantile", 0.85)),
            min_periods=min_periods,
        )
        accel = spread_avg - spread_avg.shift(trend_window // 2 or 1)
        accel_q75 = lagged_rolling_quantile(
            accel.abs(),
            window=lookback_window,
            quantile=float(params.get("accel_quantile", 0.75)),
            min_periods=min_periods,
        )
        volume_low_q = lagged_rolling_quantile(
            volume,
            window=lookback_window,
            quantile=low_volume_quantile,
            min_periods=min_periods,
        )
        history_ready = spread_q85.notna() & accel_q75.notna() & volume_low_q.notna()
        canonical_wide = state_at_least(
            df,
            "ms_spread_state",
            1.0,
            min_confidence=float(params.get("context_min_confidence", 0.55)),
            max_entropy=float(params.get("context_max_entropy", 0.90)),
        )
        return {
            "spread": spread,
            "spread_avg": spread_avg,
            "spread_q85": spread_q85,
            "accel": accel,
            "accel_q75": accel_q75,
            "volume": volume,
            "volume_low_q": volume_low_q,
            "history_ready": history_ready,
            "canonical_wide": canonical_wide,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        return (
            features["history_ready"]
            & features["canonical_wide"]
            & (features["spread_avg"] >= features["spread_q85"]).fillna(False)
            & (features["accel"] > 0).fillna(False)
            & (features["accel"] >= features["accel_q75"]).fillna(False)
            & (features["volume"] <= features["volume_low_q"]).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return (
            features["spread_avg"].fillna(0.0)
            * (1.0 + features["accel"].clip(lower=0.0).fillna(0.0))
        ).clip(lower=0.0)


class SlippageSpikeDetector(ThresholdDetector):
    """Detects abnormal execution slippage relative to prevailing spread conditions."""

    event_type = "SLIPPAGE_SPIKE_EVENT"
    required_columns = ("timestamp",)

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        if "slippage_bps" in df.columns:
            slippage = pd.to_numeric(df["slippage_bps"], errors="coerce").abs().astype(float)
        elif "spread_zscore" in df.columns:
            slippage = pd.to_numeric(df["spread_zscore"], errors="coerce").abs().astype(float)
        else:
            slippage = pd.Series(0.0, index=df.index)
        spread_proxy = (
            pd.to_numeric(
                df.get("spread_bps", df.get("spread_zscore", pd.Series(0.0, index=df.index))),
                errors="coerce",
            )
            .abs()
            .astype(float)
        )

        lookback_window = int(params.get("lookback_window", 2880))
        min_periods = int(params.get("min_periods", 288))

        slip_q99 = lagged_rolling_quantile(
            slippage,
            window=lookback_window,
            quantile=float(params.get("slip_quantile", 0.99)),
            min_periods=min_periods,
        )
        slippage_ratio = slippage / spread_proxy.replace(0.0, np.nan)
        ratio_q90 = lagged_rolling_quantile(
            slippage_ratio,
            window=lookback_window,
            quantile=float(params.get("ratio_quantile", 0.90)),
            min_periods=min_periods,
        )
        return {
            "slippage": slippage,
            "slip_q99": slip_q99,
            "slippage_ratio": slippage_ratio,
            "ratio_q90": ratio_q90,
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return (
            (features["slippage"] >= features["slip_q99"]).fillna(False)
            & (features["slippage_ratio"] >= features["ratio_q90"]).fillna(False)
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return (features["slippage"].fillna(0.0) * features["slippage_ratio"].fillna(0.0)).clip(
            lower=0.0
        )


class FeeRegimeChangeDetector(ThresholdDetector):
    """Detects discrete fee regime steps that persist beyond one bar."""

    event_type = "FEE_REGIME_CHANGE_EVENT"
    required_columns = ("timestamp",)
    causal = False

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        if "fee_bps" in df.columns:
            fee = pd.to_numeric(df["fee_bps"], errors="coerce").astype(float)
            fee_change = fee.diff(1).abs()
            fee_baseline = (
                fee.rolling(int(params.get("baseline_window", 96)), min_periods=12)
                .median()
                .shift(1)
            )

            lookback_window = int(params.get("lookback_window", 2880))
            min_periods = int(params.get("min_periods", 288))

            fee_q95 = lagged_rolling_quantile(
                fee_change,
                window=lookback_window,
                quantile=float(params.get("fee_change_quantile", 0.95)),
                min_periods=min_periods,
            )
            persistent_shift = fee.shift(2).notna() & (fee == fee.shift(1)) & (fee != fee.shift(2))
        else:
            fee_change = pd.Series(0.0, index=df.index)
            fee_baseline = pd.Series(0.0, index=df.index)
            fee_q95 = pd.Series(np.inf, index=df.index)
            persistent_shift = pd.Series(False, index=df.index)
        baseline_delta = (
            pd.to_numeric(df.get("fee_bps", pd.Series(0.0, index=df.index)), errors="coerce")
            - fee_baseline
        ).abs()
        return {
            "fee_change": fee_change,
            "fee_q95": fee_q95,
            "persistent_shift": persistent_shift.fillna(False),
            "baseline_delta": baseline_delta.fillna(0.0),
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        # Audit LT-003: fee_change (diff at T) must be shifted to match
        # persistent_shift (confirmation at T+1).
        magnitude_at_confirmation = features["fee_change"].shift(1)
        threshold_at_confirmation = features["fee_q95"].shift(1)

        return (
            (magnitude_at_confirmation >= threshold_at_confirmation).fillna(False)
            & features["persistent_shift"]
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        return (features["fee_change"].fillna(0.0) + features["baseline_delta"].fillna(0.0)).clip(
            lower=0.0
        )



class CopulaPairsTradingDetector(ThresholdDetector):
    """Detects mean-reversion pairs dislocations with pair-universe gating.

    The detector prefers explicit pair-close data when present, but it still
    accepts the legacy `pairs_zscore` signal and a proxy spread z-score fallback.
    That keeps it compatible with older synthetic fixtures while remaining tied
    to the explicit copula universe.
    """

    event_type = "COPULA_PAIRS_TRADING"
    required_columns = ("timestamp", "close")
    signal_profile = "pair_reversion"
    min_spacing = 12
    DEFAULT_Z_QUANTILE = 0.90
    DEFAULT_SPREAD_QUANTILE = 0.75
    DEFAULT_PAIR_WINDOW = 96
    DEFAULT_FALLBACK_WINDOW = 96

    def prepare_features(self, df: pd.DataFrame, **params: Any) -> dict[str, pd.Series]:
        close = pd.to_numeric(df["close"], errors="coerce").astype(float)
        ret = close.pct_change(1).fillna(0.0)
        spread_proxy = pd.to_numeric(
            df.get("spread_zscore", pd.Series(0.0, index=df.index)), errors="coerce"
        ).abs().astype(float)

        trend_window = int(params.get("trend_window", 96))
        pair_window = int(params.get("pair_window", trend_window))
        lookback_window = int(params.get("lookback_window", 2880))
        min_periods = int(params.get("min_periods", 48))

        pair_close_col = next((c for c in ("pair_close", "paired_close", "close_pair") if c in df.columns), None)
        pairs_zscore_col = "pairs_zscore" if "pairs_zscore" in df.columns else None

        if pairs_zscore_col is not None:
            zscore = pd.to_numeric(df[pairs_zscore_col], errors="coerce").astype(float)
            zscore_source = "pairs_zscore"
        elif pair_close_col is not None:
            pair_close = pd.to_numeric(df[pair_close_col], errors="coerce").astype(float)
            pair_spread = close - pair_close
            pair_spread_mean = pair_spread.rolling(pair_window, min_periods=12).mean()
            pair_spread_std = pair_spread.rolling(pair_window, min_periods=12).std().replace(0.0, np.nan)
            zscore = (pair_spread - pair_spread_mean) / pair_spread_std
            zscore_source = "pair_spread"
        else:
            zscore = (ret - ret.rolling(trend_window, min_periods=12).mean()) / (
                ret.rolling(trend_window, min_periods=12).std().replace(0.0, np.nan)
            )
            zscore_source = "return_zscore"

        zscore_abs = zscore.abs()
        pair_reversal = ((zscore.shift(1) > 0) & (zscore.diff() < 0)) | ((zscore.shift(1) < 0) & (zscore.diff() > 0))
        if zscore_source == "pair_spread":
            pair_reversal = pair_reversal | (spread_proxy.diff().fillna(0.0) > 0)

        pair_dispersion = zscore_abs.rolling(pair_window, min_periods=12).std().fillna(0.0)
        z_q95 = lagged_rolling_quantile(
            zscore_abs,
            window=lookback_window,
            quantile=float(params.get("z_quantile", self.DEFAULT_Z_QUANTILE)),
            min_periods=min_periods,
        )
        spread_q75 = lagged_rolling_quantile(
            spread_proxy,
            window=lookback_window,
            quantile=float(params.get("spread_quantile", self.DEFAULT_SPREAD_QUANTILE)),
            min_periods=min_periods,
        )
        dispersion_q = lagged_rolling_quantile(
            pair_dispersion.abs().fillna(0.0),
            window=lookback_window,
            quantile=float(params.get("pair_dispersion_quantile", 0.65)),
            min_periods=min_periods,
        )

        pair_universe = load_copula_pairs()
        symbol_value = ""
        for candidate in ("symbol", "base_symbol", "asset_symbol"):
            if candidate in df.columns and not df.empty:
                symbol_value = str(df[candidate].iloc[0]).strip().upper()
                break
        symbol_value = str(params.get("symbol", symbol_value)).strip().upper()
        partners = copula_partners(symbol_value) if symbol_value else []
        in_universe = symbol_value in copula_pair_universe() if symbol_value else True

        pair_strength = (
            zscore_abs.fillna(0.0)
            * (1.0 + spread_proxy.fillna(0.0))
            * (1.0 + pair_dispersion.fillna(0.0))
        )

        return {
            "zscore": zscore,
            "zscore_abs": zscore_abs,
            "z_q95": z_q95,
            "mean_reversion": pair_reversal.fillna(False),
            "spread_proxy": spread_proxy,
            "spread_q75": spread_q75,
            "pair_dispersion": pair_dispersion,
            "dispersion_q": dispersion_q,
            "pair_strength": pair_strength,
            "pair_universe_size": pd.Series(len(pair_universe), index=df.index, dtype=float),
            "partner_count": pd.Series(len(partners), index=df.index, dtype=float),
            "pair_in_universe": pd.Series(bool(in_universe), index=df.index, dtype=bool),
            "has_pair_spread": pd.Series(bool(pair_close_col is not None), index=df.index, dtype=bool),
            "signal_profile": pd.Series("pair_reversion", index=df.index, dtype=object),
        }

    def compute_raw_mask(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        pair_gate = features.get("pair_in_universe")
        if pair_gate is None:
            pair_gate = pd.Series(True, index=features["zscore_abs"].index)
        dislocation_gate = (
            (features["zscore_abs"] >= features["z_q95"]).fillna(False)
            | (features["spread_proxy"] >= features["spread_q75"]).fillna(False)
        )
        confirmation_gate = (
            features["mean_reversion"].fillna(False)
            | (features["spread_proxy"].diff().fillna(0.0) >= 0.0)
        )
        return (
            pair_gate.fillna(False)
            & features["pair_in_universe"].fillna(False)
            & dislocation_gate
            & confirmation_gate
        ).fillna(False)

    def compute_intensity(
        self, df: pd.DataFrame, *, features: dict[str, pd.Series], **params: Any
    ) -> pd.Series:
        del df, params
        pair_weight = 1.0 + features.get("partner_count", pd.Series(0.0, index=features["zscore_abs"].index)).fillna(0.0) * 0.05
        return (
            features["pair_strength"].fillna(0.0)
            * (1.0 + pair_weight * 0.1)
        ).clip(lower=0.0)

    def compute_direction(self, idx: int, features: dict[str, pd.Series], **params: Any) -> str:
        del params
        zscore = float(features["zscore"].iloc[idx] if not pd.isna(features["zscore"].iloc[idx]) else 0.0)
        return "down" if zscore > 0 else "up" if zscore < 0 else "non_directional"

    def compute_metadata(
        self, idx: int, features: dict[str, pd.Series], **params: Any
    ) -> dict[str, Any]:
        del idx, params
        return {
            "signal_profile": "pair_reversion",
            "pair_universe_size": int(features["pair_universe_size"].iloc[0]),
            "partner_count": int(features["partner_count"].iloc[0]),
            "pair_in_universe": bool(features["pair_in_universe"].iloc[0]),
            "has_pair_spread": bool(features["has_pair_spread"].iloc[0]),
        }


from project.events.detectors.registry import get_detector
from project.events.registries.temporal import (
    ensure_temporal_detectors_registered,
    get_temporal_detectors,
)

ensure_temporal_detectors_registered()

_DETECTORS = get_temporal_detectors()


def detect_temporal_family(
    df: pd.DataFrame, symbol: str, event_type: str = "SESSION_OPEN_EVENT", **params: Any
) -> pd.DataFrame:
    detector = get_detector(event_type)
    if detector is None:
        raise ValueError(f"Unknown temporal event type: {event_type}")
    return detector.detect(df, symbol=symbol, **params)


def analyze_temporal_family(
    df: pd.DataFrame, symbol: str, event_type: str = "SESSION_OPEN_EVENT", **params: Any
) -> tuple[pd.DataFrame, dict[str, Any]]:
    events = detect_temporal_family(df, symbol, event_type=event_type, **params)
    market = df[["timestamp", "close"]].copy() if not df.empty and "close" in df.columns else None
    analyzer_results = run_analyzer_suite(events, market=market) if not events.empty else {}
    return events, analyzer_results
