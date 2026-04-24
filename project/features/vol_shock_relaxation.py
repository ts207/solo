from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from project.contracts.temporal_contracts import TemporalContract
from project.core.causal_primitives import trailing_median, trailing_quantile
from project.events.shared import (
    emit_event,
    format_event_id,
)

# --- Temporal Contract ---

TEMPORAL_CONTRACT = TemporalContract(
    name="vol_shock_relaxation",
    output_mode="event_detector",
    observation_clock="bar_close",
    decision_lag_bars=1,
    lookback_bars=288,
    uses_current_observation=False,
    calibration_mode="rolling",
    fit_scope="streaming",
    approved_primitives=("trailing_quantile", "trailing_median"),
    notes="Uses causal shock threshold on RV shock ratio.",
)

VSR_DEF_VERSION = "v1"


@dataclass(frozen=True)
class VolShockRelaxationConfig:
    def_version: str = VSR_DEF_VERSION
    rv_window: int = 12
    baseline_window: int = 288
    shock_quantile: float = 0.99
    relax_threshold: float = 1.25
    relax_consecutive_bars: int = 3
    cooldown_bars: int = 12
    post_horizon_bars: int = 96
    auc_horizon_bars: int = 96
    range_expansion_threshold: float = 0.02


DEFAULT_VSR_CONFIG = VolShockRelaxationConfig()


def _compute_core_series(df: pd.DataFrame, cfg: VolShockRelaxationConfig) -> pd.DataFrame:
    out = df.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)
    out = out.sort_values("timestamp").reset_index(drop=True)

    close = out["close"].astype(float)
    out["logret"] = np.log(close / close.shift(1))
    # RV computed on logrets; lag=0 here because rv_base will be lagged
    # Actually, we want RV at t to be based on periods up to t.
    # But if we use it for a signal at t, it must be known.
    # RV window 12.
    out["rv"] = np.sqrt(
        out["logret"].pow(2).rolling(cfg.rv_window, min_periods=cfg.rv_window).mean()
    )

    # rv_base and mad should be PIT-safe (lag=1)
    out["rv_base"] = trailing_median(out["rv"], window=cfg.baseline_window, lag=1)

    # MAD calculation: (rv[t] - rv_base[t]) is NOT PIT safe if rv_base[t] uses rv[t-window:t-1]
    # because rv[t] is from logret[t].
    # But if the trigger is at t, we need to compare rv[t] to a threshold known at t-1.

    mad = (
        (out["rv"] - out["rv_base"])
        .abs()
        .rolling(cfg.baseline_window, min_periods=cfg.baseline_window)
        .median()
        .shift(1)
    )
    denom = (1.4826 * mad).replace(0.0, np.nan)
    out["shock_ratio"] = (out["rv"] / out["rv_base"]).replace([np.inf, -np.inf], np.nan)
    out["shock_z"] = (out["rv"] - out["rv_base"]) / denom
    return out


def detect_vol_shock_relaxation_events(
    frame: pd.DataFrame,
    symbol: str,
    config: VolShockRelaxationConfig = DEFAULT_VSR_CONFIG,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, float]]:
    required = {"timestamp", "close", "high", "low"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df = _compute_core_series(frame, config)
    df["t_shock_dynamic"] = trailing_quantile(
        df["shock_ratio"], window=config.baseline_window, q=config.shock_quantile, lag=1
    )
    threshold_series = df["t_shock_dynamic"]
    threshold_valid = threshold_series[np.isfinite(threshold_series)]
    if threshold_valid.empty:
        return pd.DataFrame(), df, {"t_shock": np.nan, "t_relax": config.relax_threshold}
    t_shock_ref = float(threshold_valid.median())

    event_rows: List[Dict[str, object]] = []
    n = len(df)
    i = 1
    cooldown_until = -1
    event_num = 0

    while i < n:
        if i <= cooldown_until:
            i += 1
            continue

        sr_now = float(df["shock_ratio"].iat[i]) if pd.notna(df["shock_ratio"].iat[i]) else np.nan
        sr_prev = (
            float(df["shock_ratio"].iat[i - 1])
            if pd.notna(df["shock_ratio"].iat[i - 1])
            else np.nan
        )
        thr_now = float(threshold_series.iat[i]) if pd.notna(threshold_series.iat[i]) else np.nan
        thr_prev = (
            float(threshold_series.iat[i - 1]) if pd.notna(threshold_series.iat[i - 1]) else np.nan
        )
        onset = (
            np.isfinite(sr_now)
            and np.isfinite(sr_prev)
            and np.isfinite(thr_now)
            and np.isfinite(thr_prev)
            and (sr_now >= thr_now)
            and (sr_prev < thr_prev)
        )
        if not onset:
            i += 1
            continue

        enter = i
        relax_run = 0
        exit_idx: Optional[int] = None
        j = i
        while j < n:
            sr = float(df["shock_ratio"].iat[j]) if pd.notna(df["shock_ratio"].iat[j]) else np.nan
            if np.isfinite(sr) and sr <= config.relax_threshold:
                relax_run += 1
            else:
                relax_run = 0
            if relax_run >= config.relax_consecutive_bars:
                exit_idx = j
                break
            j += 1

        if exit_idx is None:
            exit_idx = n - 1

        event_num += 1
        event_id = format_event_id("VOL_SHOCK", symbol, int(enter), event_num)

        window = df.iloc[enter : exit_idx + 1].copy()
        rv_peak = float(window["rv"].max()) if not window.empty else np.nan
        peak_idx = (
            int(window["rv"].idxmax()) if not window.empty and window["rv"].notna().any() else enter
        )
        sr_peak = float(window["shock_ratio"].max()) if not window.empty else np.nan
        t_rv_peak = int(max(0, peak_idx - enter))
        rv_base_enter = (
            float(df["rv_base"].iat[enter]) if pd.notna(df["rv_base"].iat[enter]) else np.nan
        )
        half_target = (
            rv_base_enter + 0.5 * (rv_peak - rv_base_enter)
            if np.isfinite(rv_base_enter) and np.isfinite(rv_peak)
            else np.nan
        )

        half_life = np.nan
        if np.isfinite(half_target):
            for h in range(0, min(config.post_horizon_bars, n - enter)):
                rv_h = (
                    float(df["rv"].iat[enter + h]) if pd.notna(df["rv"].iat[enter + h]) else np.nan
                )
                if np.isfinite(rv_h) and rv_h <= half_target:
                    half_life = float(h)
                    break

        h_end = min(n - 1, enter + config.auc_horizon_bars)
        h_window = df.iloc[enter : h_end + 1]
        excess = (
            (h_window["rv"] - rv_base_enter).clip(lower=0.0)
            if np.isfinite(rv_base_enter)
            else pd.Series(dtype=float)
        )
        auc_excess = float(excess.fillna(0.0).sum()) if not excess.empty else np.nan

        post_end = min(n - 1, enter + config.post_horizon_bars)
        post = df.iloc[enter : post_end + 1].copy()
        sec_time = np.nan
        sec_end = min(exit_idx, post_end)
        if peak_idx + 1 <= sec_end:
            for k in range(peak_idx + 1, sec_end + 1):
                sr_k = (
                    float(df["shock_ratio"].iat[k])
                    if pd.notna(df["shock_ratio"].iat[k])
                    else np.nan
                )
                thr_k = (
                    float(threshold_series.iat[k]) if pd.notna(threshold_series.iat[k]) else np.nan
                )
                if np.isfinite(sr_k) and np.isfinite(thr_k) and sr_k >= thr_k:
                    sec_time = float(k - enter)
                    break

        close_enter = float(df["close"].iat[enter])
        range_pct_96 = np.nan
        if not post.empty and close_enter > 0:
            range_pct_96 = float((post["high"].max() - post["low"].min()) / close_enter)

        ret_post = post["logret"].dropna()

        # Collect diagnostic metadata
        metadata = {
            "vsr_def_version": config.def_version,
            "enter_idx": int(enter),
            "exit_idx": int(exit_idx),
            "duration_bars": int(exit_idx - enter + 1),
            "time_to_relax": int(exit_idx - enter),
            "shock_ratio_onset": sr_now,
            "shock_ratio_peak": sr_peak,
            "shock_threshold_enter": thr_now,
            "rv_peak": rv_peak,
            "t_rv_peak": int(t_rv_peak),
            "rv_base_enter": rv_base_enter,
            "rv_decay_half_life": half_life,
            "auc_excess_rv": auc_excess,
            "secondary_shock_within_h": int(np.isfinite(sec_time)),
            "time_to_secondary_shock": sec_time,
            "realized_vol_mean_96": float(post["rv"].mean()) if not post.empty else np.nan,
            "realized_vol_p90_96": float(post["rv"].quantile(0.9)) if not post.empty else np.nan,
            "range_pct_96": range_pct_96,
            "relaxed_within_96": int((exit_idx - enter) <= config.post_horizon_bars),
            "skew_returns_96": float(ret_post.skew()) if len(ret_post) >= 3 else np.nan,
            "kurtosis_returns_96": float(ret_post.kurtosis()) if len(ret_post) >= 4 else np.nan,
        }

        row = emit_event(
            event_type="VOL_SHOCK",
            symbol=symbol,
            event_id=event_id,
            eval_bar_ts=df["timestamp"].iat[enter],
            intensity=sr_peak,
            metadata=metadata,
            shift_bars=0,
        )
        event_rows.append(row)
        cooldown_until = exit_idx + config.cooldown_bars
        i = cooldown_until + 1

    events = pd.DataFrame(event_rows)
    if events.empty:
        from project.events.shared import EVENT_COLUMNS

        events = pd.DataFrame(columns=EVENT_COLUMNS)
    return events, df, {"t_shock": t_shock_ref, "t_relax": config.relax_threshold}


def build_event_state_frame(core: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    out = core[["timestamp"]].copy()
    out["vsr_active"] = 0
    out["vsr_event_id"] = None
    out["vsr_age_bars"] = 0
    out["vsr_enter_ts"] = pd.NaT
    out["vsr_exit_ts"] = pd.NaT
    if events.empty:
        return out

    for _, ev in events.iterrows():
        s = int(ev["enter_idx"])
        e = int(ev["exit_idx"])
        eid = ev["event_id"]
        enter_ts = ev["enter_ts"]
        exit_ts = ev["exit_ts"]
        out.loc[s:e, "vsr_active"] = 1
        out.loc[s:e, "vsr_event_id"] = eid
        out.loc[s:e, "vsr_enter_ts"] = enter_ts
        out.loc[s:e, "vsr_age_bars"] = np.arange(1, e - s + 2)
        out.loc[e, "vsr_exit_ts"] = exit_ts
    return out
