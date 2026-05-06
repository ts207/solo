from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.scripts.detector_shadow_report import DEFAULT_HORIZONS, _prepare_symbol_frame, _return_summary, _rolling_pct_rank


DEFAULT_SYMBOLS = ("BTCUSDT", "ETHUSDT")
DEFAULT_YEARS = (2022, 2023, 2024, 2025)
RARITY_BUCKETS = (
    ("rare", 20, 300),
    ("medium", 300, 1500),
    ("broad", 1500, 5000),
    ("too_broad", 5001, 10**12),
)
TP_GRID = (10.0, 15.0, 25.0, 40.0)
SL_GRID = (10.0, 15.0, 25.0, 40.0)
MAX_HOLD_GRID = (12, 24, 48, 96)


@dataclass(frozen=True)
class VariantSpec:
    variant_id: str
    family: str
    direction: str
    mask: pd.Series
    params: dict[str, Any]


def _parse_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _parse_years(value: str) -> list[int]:
    return [int(item) for item in _parse_csv(value)]


def _parse_ints(value: str) -> list[int]:
    return [int(item) for item in _parse_csv(value)]


def _finite(values: list[float]) -> list[float]:
    return [float(value) for value in values if np.isfinite(value)]


def _rarity_bucket(count: int) -> str:
    for label, low, high in RARITY_BUCKETS:
        if low <= int(count) <= high:
            return label
    return "too_rare"


def _safe_mean(values: list[float]) -> float | None:
    clean = _finite(values)
    return float(np.mean(clean)) if clean else None


def _direction_mult(direction: str) -> float:
    token = str(direction).strip().lower()
    if token in {"long", "bullish", "up"}:
        return 1.0
    if token in {"short", "bearish", "down"}:
        return -1.0
    raise ValueError(f"unsupported direction: {direction}")


def _add_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    close = pd.to_numeric(out["close"], errors="coerce")
    high = pd.to_numeric(out["high"], errors="coerce")
    low = pd.to_numeric(out["low"], errors="coerce")
    open_ = pd.to_numeric(out["open"], errors="coerce")
    volume = pd.to_numeric(out.get("volume", 0.0), errors="coerce").fillna(0.0)
    oi = pd.to_numeric(out["oi_notional"], errors="coerce")
    funding = pd.to_numeric(out["funding_rate_scaled"], errors="coerce").fillna(0.0)

    out["ret_1"] = close.pct_change()
    out["ret_12"] = close.pct_change(12)
    out["ret_24"] = close.pct_change(24)
    out["price_move_abs_pct_12"] = _rolling_pct_rank(out["ret_12"].abs())
    out["price_move_abs_pct_24"] = _rolling_pct_rank(out["ret_24"].abs())
    out["price_up_12"] = out["ret_12"] > 0.0
    out["price_down_12"] = out["ret_12"] < 0.0
    out["price_up_24"] = out["ret_24"] > 0.0
    out["price_down_24"] = out["ret_24"] < 0.0

    out["oi_chg_12"] = np.log(oi.replace(0.0, np.nan)).diff(12)
    out["oi_chg_24"] = np.log(oi.replace(0.0, np.nan)).diff(24)
    out["oi_abs_pct_12"] = _rolling_pct_rank(out["oi_chg_12"].abs())
    out["oi_abs_pct_24"] = _rolling_pct_rank(out["oi_chg_24"].abs())
    out["oi_up_12"] = out["oi_chg_12"] > 0.0
    out["oi_down_12"] = out["oi_chg_12"] < 0.0
    out["oi_up_24"] = out["oi_chg_24"] > 0.0
    out["oi_down_24"] = out["oi_chg_24"] < 0.0

    out["funding_sign"] = np.select([funding > 0.0, funding < 0.0], ["positive", "negative"], default="zero")
    out["funding_abs"] = funding.abs()
    out["funding_abs_pct"] = _rolling_pct_rank(out["funding_abs"])
    out["funding_change_96"] = funding.diff(96)
    out["funding_accel_pct"] = _rolling_pct_rank(out["funding_change_96"].abs())
    out["funding_abs_slope_96"] = out["funding_abs"].diff(96)
    out["funding_rising"] = out["funding_abs_slope_96"] > 0.0
    out["funding_falling"] = out["funding_abs_slope_96"] < 0.0

    true_range = (high - low).replace(0.0, np.nan)
    body = (close - open_).abs().replace(0.0, np.nan)
    out["close_location"] = ((close - low) / true_range).clip(0.0, 1.0)
    out["close_near_low"] = out["close_location"] <= 0.25
    out["close_near_high"] = out["close_location"] >= 0.75
    out["upper_wick_body"] = ((high - np.maximum(open_, close)) / body).replace([np.inf, -np.inf], np.nan)
    out["lower_wick_body"] = ((np.minimum(open_, close) - low) / body).replace([np.inf, -np.inf], np.nan)
    out["volume_z"] = ((volume - volume.rolling(288, min_periods=48).mean()) / volume.rolling(288, min_periods=48).std()).replace([np.inf, -np.inf], np.nan)
    out["vol_confirm_z1"] = out["volume_z"] > 1.0
    out["vol_confirm_z2"] = out["volume_z"] > 2.0

    for lookback in (12, 24, 48, 96):
        prior_low = low.shift(1).rolling(lookback, min_periods=max(6, lookback // 2)).min()
        prior_high = high.shift(1).rolling(lookback, min_periods=max(6, lookback // 2)).max()
        midpoint = (open_ + close) / 2.0
        out[f"failed_breakdown_reclaim_{lookback}"] = (low < prior_low) & (close > prior_low)
        out[f"failed_breakdown_mid_reclaim_{lookback}"] = (low < prior_low) & (close > midpoint)
        out[f"failed_breakdown_wick_{lookback}"] = out[f"failed_breakdown_reclaim_{lookback}"] & (out["lower_wick_body"] >= 1.5)
        out[f"failed_breakout_rejection_{lookback}"] = (high > prior_high) & (close < prior_high)
        out[f"failed_breakout_mid_rejection_{lookback}"] = (high > prior_high) & (close < midpoint)
        out[f"failed_breakout_wick_{lookback}"] = out[f"failed_breakout_rejection_{lookback}"] & (out["upper_wick_body"] >= 1.5)

    ma_fast = close.rolling(96, min_periods=24).mean()
    ma_slow = close.rolling(288, min_periods=72).mean()
    trend_spread = (ma_fast / ma_slow - 1.0).replace([np.inf, -np.inf], np.nan)
    out["trend_regime"] = np.select([trend_spread > 0.01, trend_spread < -0.01], ["uptrend", "downtrend"], default="chop")
    out["oi_regime"] = np.select([out["oi_chg_24"] > 0.0, out["oi_chg_24"] < 0.0], ["expanding", "contracting"], default="flat")
    hour = out["timestamp"].dt.hour
    out["session"] = np.select(
        [hour.between(0, 7), hour.between(8, 13), hour.between(14, 21)],
        ["asia", "europe", "us"],
        default="late_us",
    )
    return out


def _cooldown_indices(mask: pd.Series, cooldown: int) -> np.ndarray:
    raw = np.flatnonzero(mask.fillna(False).to_numpy())
    if len(raw) == 0:
        return raw
    kept: list[int] = []
    last = -10**9
    for idx in raw:
        if idx - last >= cooldown:
            kept.append(int(idx))
            last = int(idx)
    return np.asarray(kept, dtype=int)


def _cooldown_indices_by_symbol(df: pd.DataFrame, mask: pd.Series, cooldown: int) -> np.ndarray:
    kept: list[int] = []
    mask_values = mask.fillna(False)
    for _, group in df[mask_values].groupby("symbol", sort=False):
        raw = group.index.to_numpy(dtype=int)
        last = -10**9
        for idx in raw:
            if int(idx) - last >= cooldown:
                kept.append(int(idx))
                last = int(idx)
    return np.asarray(sorted(kept), dtype=int)


def _make_variant_id(parts: list[Any]) -> str:
    return "_".join(str(part).upper().replace(".", "P").replace("-", "NEG").replace(" ", "_") for part in parts if str(part))


def _base_filter(df: pd.DataFrame, price_pct: float, oi_pct: float, lookback: int = 12) -> dict[str, pd.Series]:
    return {
        "price_down_oi_up": df[f"price_down_{lookback}"] & df[f"oi_up_{lookback}"] & (df[f"price_move_abs_pct_{lookback}"] >= price_pct) & (df[f"oi_abs_pct_{lookback}"] >= oi_pct),
        "price_down_oi_down": df[f"price_down_{lookback}"] & df[f"oi_down_{lookback}"] & (df[f"price_move_abs_pct_{lookback}"] >= price_pct) & (df[f"oi_abs_pct_{lookback}"] >= oi_pct),
        "price_up_oi_up": df[f"price_up_{lookback}"] & df[f"oi_up_{lookback}"] & (df[f"price_move_abs_pct_{lookback}"] >= price_pct) & (df[f"oi_abs_pct_{lookback}"] >= oi_pct),
        "price_up_oi_down": df[f"price_up_{lookback}"] & df[f"oi_down_{lookback}"] & (df[f"price_move_abs_pct_{lookback}"] >= price_pct) & (df[f"oi_abs_pct_{lookback}"] >= oi_pct),
    }


def _generate_variants(df: pd.DataFrame, max_raw_variants: int) -> list[VariantSpec]:
    variants: list[VariantSpec] = []
    price_pcts = (70.0, 80.0, 90.0)
    oi_pcts = (90.0, 95.0, 97.5)
    funding_abs_pcts = (90.0, 95.0, 97.5, 99.0)
    funding_accel_pcts = (80.0, 90.0, 95.0)
    persist_bars = (2, 3, 6, 12)
    reclaim_lookbacks = (12, 24, 48, 96)

    for price_pct in price_pcts:
        for oi_pct in oi_pcts:
            base = _base_filter(df, price_pct, oi_pct, lookback=12)
            for reclaim in reclaim_lookbacks:
                failed_down = df[f"failed_breakdown_reclaim_{reclaim}"]
                failed_up = df[f"failed_breakout_rejection_{reclaim}"]
                strong_down = df[f"failed_breakdown_wick_{reclaim}"] | df[f"failed_breakdown_mid_reclaim_{reclaim}"]
                strong_up = df[f"failed_breakout_wick_{reclaim}"] | df[f"failed_breakout_mid_rejection_{reclaim}"]
                params = {"price_pct": price_pct, "oi_pct": oi_pct, "failure_lookback": reclaim}
                variants.extend(
                    [
                        VariantSpec(
                            _make_variant_id(["OI_FLUSH_DOWN_REVERSAL_STRICT", price_pct, oi_pct, reclaim]),
                            "oi_flush",
                            "long",
                            base["price_down_oi_down"] & strong_down,
                            params,
                        ),
                        VariantSpec(
                            _make_variant_id(["OI_FLUSH_DOWN_CONTINUATION_STRICT", price_pct, oi_pct, reclaim]),
                            "oi_flush",
                            "short",
                            base["price_down_oi_down"] & df["close_near_low"] & ~failed_down,
                            params,
                        ),
                        VariantSpec(
                            _make_variant_id(["OI_FLUSH_UP_REVERSAL_STRICT", price_pct, oi_pct, reclaim]),
                            "oi_flush",
                            "short",
                            base["price_up_oi_down"] & strong_up,
                            params,
                        ),
                        VariantSpec(
                            _make_variant_id(["OI_FLUSH_UP_CONTINUATION_STRICT", price_pct, oi_pct, reclaim]),
                            "oi_flush",
                            "long",
                            base["price_up_oi_down"] & df["close_near_high"] & ~failed_up,
                            params,
                        ),
                        VariantSpec(
                            _make_variant_id(["SHORT_BUILD_CONTINUATION_STRICT", price_pct, oi_pct, reclaim]),
                            "oi_expansion",
                            "short",
                            base["price_down_oi_up"] & ((df["funding_sign"] == "negative") | df["funding_falling"]) & df["close_near_low"] & ~failed_down,
                            params,
                        ),
                        VariantSpec(
                            _make_variant_id(["LONG_STRESS_REVERSAL_STRICT", price_pct, oi_pct, reclaim]),
                            "oi_expansion",
                            "long",
                            base["price_down_oi_up"] & (df["funding_sign"] == "positive") & strong_down,
                            params,
                        ),
                        VariantSpec(
                            _make_variant_id(["LONG_BUILD_CONTINUATION_STRICT", price_pct, oi_pct, reclaim]),
                            "oi_expansion",
                            "long",
                            base["price_up_oi_up"] & ((df["funding_sign"] == "positive") | df["funding_rising"]) & df["close_near_high"] & ~failed_up,
                            params,
                        ),
                        VariantSpec(
                            _make_variant_id(["SHORT_SQUEEZE_SETUP_STRICT", price_pct, oi_pct, reclaim]),
                            "oi_expansion",
                            "short",
                            base["price_up_oi_up"] & (df["funding_sign"] == "negative") & strong_up,
                            params,
                        ),
                    ]
                )

    for funding_pct in funding_abs_pcts:
        for accel_pct in funding_accel_pcts:
            for persist in persist_bars:
                pos_extreme = (df["funding_abs_pct"] >= funding_pct) & (df["funding_sign"] == "positive")
                neg_extreme = (df["funding_abs_pct"] >= funding_pct) & (df["funding_sign"] == "negative")
                pos_persist = pos_extreme.rolling(persist, min_periods=persist).sum() >= persist
                neg_persist = neg_extreme.rolling(persist, min_periods=persist).sum() >= persist
                oi_expand = df["oi_up_12"] & (df["oi_abs_pct_12"] >= 90.0)
                accel = df["funding_accel_pct"] >= accel_pct
                accel_decay = df["funding_falling"] & (df["funding_accel_pct"] <= accel_pct)
                params = {"funding_abs_pct": funding_pct, "funding_accel_pct": accel_pct, "persistence_bars": persist}
                variants.extend(
                    [
                        VariantSpec(
                            _make_variant_id(["FUNDING_POS_CONTINUATION_STRICT", funding_pct, accel_pct, persist]),
                            "funding",
                            "long",
                            pos_extreme & accel & oi_expand & df["price_up_12"] & df["close_near_high"] & ~df["failed_breakout_rejection_24"],
                            params,
                        ),
                        VariantSpec(
                            _make_variant_id(["FUNDING_NEG_CONTINUATION_STRICT", funding_pct, accel_pct, persist]),
                            "funding",
                            "short",
                            neg_extreme & accel & oi_expand & df["price_down_12"] & df["close_near_low"] & ~df["failed_breakdown_reclaim_24"],
                            params,
                        ),
                        VariantSpec(
                            _make_variant_id(["FUNDING_POS_BREAK_STRICT", funding_pct, accel_pct, persist]),
                            "funding",
                            "short",
                            pos_extreme & oi_expand & df["failed_breakout_wick_24"],
                            params,
                        ),
                        VariantSpec(
                            _make_variant_id(["FUNDING_NEG_BREAK_STRICT", funding_pct, accel_pct, persist]),
                            "funding",
                            "long",
                            neg_extreme & oi_expand & df["failed_breakdown_wick_24"],
                            params,
                        ),
                        VariantSpec(
                            _make_variant_id(["FUNDING_POS_EXHAUSTION_AFTER_PERSISTENCE", funding_pct, accel_pct, persist]),
                            "funding",
                            "short",
                            pos_persist & accel_decay & ~df["oi_up_12"] & df["failed_breakout_rejection_48"],
                            params,
                        ),
                        VariantSpec(
                            _make_variant_id(["FUNDING_NEG_EXHAUSTION_AFTER_PERSISTENCE", funding_pct, accel_pct, persist]),
                            "funding",
                            "long",
                            neg_persist & accel_decay & ~df["oi_up_12"] & df["failed_breakdown_reclaim_48"],
                            params,
                        ),
                    ]
                )

    for lookback in reclaim_lookbacks:
        for wick_mult in (1.5, 2.0):
            for vol_z in (1.0, 2.0):
                params = {"failure_lookback": lookback, "wick_body_min": wick_mult, "volume_z_min": vol_z}
                vol_mask = df["volume_z"] >= vol_z
                variants.extend(
                    [
                        VariantSpec(
                            _make_variant_id(["FAILED_BREAKDOWN_RECLAIM", lookback, wick_mult, vol_z]),
                            "failed_continuation",
                            "long",
                            df[f"failed_breakdown_reclaim_{lookback}"] & (df["lower_wick_body"] >= wick_mult) & vol_mask,
                            params,
                        ),
                        VariantSpec(
                            _make_variant_id(["FAILED_BREAKOUT_REJECTION", lookback, wick_mult, vol_z]),
                            "failed_continuation",
                            "short",
                            df[f"failed_breakout_rejection_{lookback}"] & (df["upper_wick_body"] >= wick_mult) & vol_mask,
                            params,
                        ),
                        VariantSpec(
                            _make_variant_id(["FAILED_CONTINUATION_STRONG_RECLAIM", lookback, wick_mult, vol_z]),
                            "failed_continuation",
                            "long",
                            df[f"failed_breakdown_mid_reclaim_{lookback}"] & (df["lower_wick_body"] >= wick_mult) & vol_mask,
                            params,
                        ),
                        VariantSpec(
                            _make_variant_id(["FAILED_CONTINUATION_WICK_REVERSAL", lookback, wick_mult, vol_z]),
                            "failed_continuation",
                            "short",
                            df[f"failed_breakout_mid_rejection_{lookback}"] & (df["upper_wick_body"] >= wick_mult) & vol_mask,
                            params,
                        ),
                    ]
                )
    return variants[:max_raw_variants]


def _path_metrics(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    indices: np.ndarray,
    direction: str,
    horizon: int,
    cost_bps: float,
) -> dict[str, Any]:
    mult = _direction_mult(direction)
    close_gross: list[float] = []
    close_net: list[float] = []
    mfe: list[float] = []
    mae: list[float] = []
    time_to_mfe: list[float] = []
    time_to_mae: list[float] = []
    hit_after_cost = 0
    mae_exceeds_cost = 0
    for idx in indices:
        if idx + horizon >= len(close) or not np.isfinite(close[idx]) or close[idx] <= 0.0:
            continue
        entry = close[idx]
        end_close = close[idx + horizon]
        gross = float(((end_close / entry) - 1.0) * 10000.0 * mult)
        close_gross.append(gross)
        close_net.append(gross - cost_bps)
        hi = high[idx + 1 : idx + horizon + 1]
        lo = low[idx + 1 : idx + horizon + 1]
        if mult > 0:
            fav_path = (hi / entry - 1.0) * 10000.0
            adv_path = (lo / entry - 1.0) * 10000.0
        else:
            fav_path = (entry / lo - 1.0) * 10000.0
            adv_path = (entry / hi - 1.0) * 10000.0
        if len(fav_path) == 0 or len(adv_path) == 0:
            continue
        fav = float(np.nanmax(fav_path))
        adv = float(np.nanmin(adv_path))
        mfe.append(fav)
        mae.append(adv)
        time_to_mfe.append(float(np.nanargmax(fav_path) + 1))
        time_to_mae.append(float(np.nanargmin(adv_path) + 1))
        if fav > cost_bps:
            hit_after_cost += 1
        if adv < -cost_bps:
            mae_exceeds_cost += 1
    mfe_mean = _safe_mean(mfe)
    mae_mean = _safe_mean(mae)
    return {
        "forward_close_gross_bps": _return_summary(close_gross),
        "forward_close_net_bps": _return_summary(close_net),
        "max_favorable_bps": mfe_mean,
        "max_adverse_bps": mae_mean,
        "edge_ratio": float(mfe_mean / abs(mae_mean)) if mfe_mean is not None and mae_mean not in (None, 0.0) else None,
        "time_to_mfe": _safe_mean(time_to_mfe),
        "time_to_mae": _safe_mean(time_to_mae),
        "mfe_hit_rate_after_cost": float(hit_after_cost / len(mfe)) if mfe else None,
        "mae_exceeds_cost_rate": float(mae_exceeds_cost / len(mae)) if mae else None,
    }


def _simulate_exit_policy(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    indices: np.ndarray,
    direction: str,
    *,
    take_profit_bps: float,
    stop_loss_bps: float,
    max_hold_bars: int,
    cost_bps: float,
) -> dict[str, Any]:
    mult = _direction_mult(direction)
    returns: list[float] = []
    holds: list[float] = []
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for idx in indices:
        if idx + 1 >= len(close) or not np.isfinite(close[idx]) or close[idx] <= 0.0:
            continue
        entry = close[idx]
        exit_bps: float | None = None
        hold = 0
        for step in range(1, min(max_hold_bars, len(close) - idx - 1) + 1):
            if mult > 0:
                fav = (high[idx + step] / entry - 1.0) * 10000.0
                adv = (low[idx + step] / entry - 1.0) * 10000.0
            else:
                fav = (entry / low[idx + step] - 1.0) * 10000.0
                adv = (entry / high[idx + step] - 1.0) * 10000.0
            hold = step
            if adv <= -stop_loss_bps:
                exit_bps = -stop_loss_bps
                break
            if fav >= take_profit_bps:
                exit_bps = take_profit_bps
                break
        if exit_bps is None:
            end_idx = min(idx + max_hold_bars, len(close) - 1)
            exit_bps = ((close[end_idx] / entry) - 1.0) * 10000.0 * mult
            hold = end_idx - idx
        net = float(exit_bps - cost_bps)
        returns.append(net)
        holds.append(float(hold))
        cumulative += net
        peak = max(peak, cumulative)
        max_drawdown = min(max_drawdown, cumulative - peak)
    summary = _return_summary(returns)
    return {
        "net_bps": summary.get("mean_bps"),
        "t_stat": summary.get("t_stat"),
        "hit_rate": float(sum(1 for value in returns if value > 0.0) / len(returns)) if returns else None,
        "avg_hold_bars": _safe_mean(holds),
        "max_drawdown_bps": float(max_drawdown),
        "n": len(returns),
    }


def _best_exit_policy(close: np.ndarray, high: np.ndarray, low: np.ndarray, indices: np.ndarray, direction: str, cost_bps: float) -> dict[str, Any] | None:
    if len(indices) == 0:
        return None
    best: dict[str, Any] | None = None
    for tp in TP_GRID:
        for sl in SL_GRID:
            for hold in MAX_HOLD_GRID:
                result = _simulate_exit_policy(
                    close,
                    high,
                    low,
                    indices,
                    direction,
                    take_profit_bps=tp,
                    stop_loss_bps=sl,
                    max_hold_bars=hold,
                    cost_bps=cost_bps,
                )
                score = (result.get("net_bps") or -10**9, result.get("t_stat") or -10**9)
                if best is None or score > (best.get("net_bps") or -10**9, best.get("t_stat") or -10**9):
                    best = {"policy": f"tp{int(tp)}_sl{int(sl)}_max{hold}", **result}
    return best


def _slice_counts(df: pd.DataFrame, indices: np.ndarray) -> dict[str, Any]:
    rows = df.iloc[indices] if len(indices) else df.iloc[[]]
    return {
        "by_symbol": dict(Counter(rows["symbol"])),
        "by_year": dict(Counter(rows["shadow_year"])),
        "by_month_top": dict(Counter(rows["shadow_month"]).most_common(8)),
        "by_vol_regime": dict(Counter(rows["shadow_vol_regime"])),
        "by_trend_regime": dict(Counter(rows["trend_regime"])),
        "by_funding_regime": dict(Counter(rows["funding_sign"])),
        "by_oi_regime": dict(Counter(rows["oi_regime"])),
        "by_session": dict(Counter(rows["session"])),
    }


def _status(
    count: int,
    best_fixed: dict[str, Any],
    best_fixed_gross: dict[str, Any],
    best_path: dict[str, Any],
    best_exit: dict[str, Any] | None,
) -> str:
    if count < 20:
        return "too_rare"
    if count > 5000:
        return "too_broad"
    net = best_fixed.get("mean_bps")
    t_stat = best_fixed.get("t_stat")
    gross = best_fixed_gross.get("mean_bps")
    cost_survival = float(net / gross) if gross is not None and gross > 0.0 and net is not None else None
    exit_net = None if best_exit is None else best_exit.get("net_bps")
    exit_t = None if best_exit is None else best_exit.get("t_stat")
    edge_ratio = best_path.get("edge_ratio")
    if (
        count >= 50
        and count <= 1500
        and net is not None
        and net > 0.0
        and t_stat is not None
        and t_stat > 2.0
        and cost_survival is not None
        and cost_survival >= 0.8
    ):
        return "fresh_validation_candidate"
    if count >= 50 and count <= 1500 and exit_net is not None and exit_net > 0.0 and exit_t is not None and exit_t > 2.0:
        return "exit_research_candidate"
    if count >= 50 and count <= 1500 and edge_ratio is not None and edge_ratio >= 1.5:
        return "path_research_candidate"
    return "research_only"


def _score(row: dict[str, Any]) -> float:
    fixed = row.get("best_fixed_horizon", {})
    path = row.get("best_path", {})
    exit_policy = row.get("best_exit_policy") or {}
    score = 0.0
    score += max(0.0, float(fixed.get("net_bps") or 0.0))
    score += 8.0 * max(0.0, float(fixed.get("t_stat") or 0.0))
    score += 6.0 * max(0.0, float(path.get("edge_ratio") or 0.0))
    score += 0.5 * max(0.0, float(exit_policy.get("net_bps") or 0.0))
    if row.get("event_rate_bucket") in {"rare", "medium"}:
        score += 20.0
    if row.get("status") == "fresh_validation_candidate":
        score += 50.0
    if row.get("status") == "exit_research_candidate":
        score += 30.0
    return float(score)


def build_tuning_report(
    *,
    repo_root: Path,
    symbols: list[str],
    years: list[int],
    horizons: list[int],
    cost_bps: float,
    cooldown_bars: int,
    max_raw_variants: int,
    top_n: int,
) -> tuple[dict[str, Any], pd.DataFrame]:
    frames = []
    input_summary: dict[str, Any] = {}
    for symbol in symbols:
        frame = _add_features(_prepare_symbol_frame(repo_root, symbol, years))
        frame["symbol"] = symbol
        frames.append(frame)
        input_summary[symbol] = {
            "rows": int(len(frame)),
            "start": str(frame["timestamp"].min()),
            "end": str(frame["timestamp"].max()),
            "years": sorted(frame["shadow_year"].dropna().unique().tolist()),
        }
    df = pd.concat(frames, ignore_index=True).sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce").to_numpy()
    high = pd.to_numeric(df["high"], errors="coerce").to_numpy()
    low = pd.to_numeric(df["low"], errors="coerce").to_numpy()

    variants = _generate_variants(df, max_raw_variants)
    rows: list[dict[str, Any]] = []
    for variant in variants:
        indices = _cooldown_indices_by_symbol(df, variant.mask, cooldown_bars)
        count = int(len(indices))
        if count < 20:
            continue
        horizon_metrics = {
            f"{horizon}b": _path_metrics(close, high, low, indices, variant.direction, horizon, cost_bps)
            for horizon in horizons
        }
        best_horizon = None
        best_fixed = {"mean_bps": None, "t_stat": None, "n": 0}
        best_fixed_gross = {"mean_bps": None, "t_stat": None, "n": 0}
        best_path = {"edge_ratio": None}
        for horizon in horizons:
            metrics = horizon_metrics[f"{horizon}b"]
            fixed = metrics["forward_close_net_bps"]
            mean = fixed.get("mean_bps")
            if mean is not None and (best_fixed["mean_bps"] is None or mean > best_fixed["mean_bps"]):
                best_horizon = horizon
                best_fixed = fixed
                best_fixed_gross = metrics["forward_close_gross_bps"]
                best_path = metrics
        evaluate_exit = count <= 1500 and (
            (best_fixed.get("mean_bps") is not None and best_fixed["mean_bps"] > 0.0)
            or (best_path.get("edge_ratio") is not None and best_path["edge_ratio"] >= 1.2)
        )
        best_exit = _best_exit_policy(close, high, low, indices, variant.direction, cost_bps) if evaluate_exit else None
        status = _status(count, best_fixed, best_fixed_gross, best_path, best_exit)
        gross_mean = best_fixed_gross.get("mean_bps")
        net_mean = best_fixed.get("mean_bps")
        cost_survival = float(net_mean / gross_mean) if gross_mean is not None and gross_mean > 0.0 and net_mean is not None else None
        row = {
            "variant_id": variant.variant_id,
            "family": variant.family,
            "symbol_scope": symbols,
            "direction": variant.direction,
            "event_count": count,
            "event_rate_bucket": _rarity_bucket(count),
            "cooldown_bars": cooldown_bars,
            "bar_interval": "5m",
            "params": variant.params,
            "best_horizon_bars": best_horizon,
            "gross_bps": gross_mean,
            "net_bps": best_fixed.get("mean_bps"),
            "t_stat": best_fixed.get("t_stat"),
            "cost_survival": cost_survival,
            "mfe_bps": best_path.get("max_favorable_bps"),
            "mae_bps": best_path.get("max_adverse_bps"),
            "edge_ratio": best_path.get("edge_ratio"),
            "mfe_hit_rate_after_cost": best_path.get("mfe_hit_rate_after_cost"),
            "mae_exceeds_cost_rate": best_path.get("mae_exceeds_cost_rate"),
            "best_exit_policy": best_exit,
            "status": status,
            "paper_approved": False,
            "live_approved": False,
            "horizon_diagnostics": horizon_metrics,
            "regime_slices": _slice_counts(df, indices),
        }
        row["score"] = _score(row)
        rows.append(row)

    rows.sort(key=lambda item: item["score"], reverse=True)
    top_rows = rows[:top_n]
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "symbols": symbols,
            "years": years,
            "timeframe": "5m",
            "horizons": horizons,
            "cost_round_trip_bps": cost_bps,
            "cooldown_bars": cooldown_bars,
        },
        "input_summary": input_summary,
        "search_dimensions": {
            "event_rate_buckets": {label: [low, high] for label, low, high in RARITY_BUCKETS},
            "preferred_event_count": [50, 1500],
            "funding_abs_percentiles": [90, 95, 97.5, 99],
            "funding_acceleration_percentiles": [80, 90, 95],
            "funding_persistence_bars": [2, 3, 6, 12],
            "oi_delta_percentiles": [90, 95, 97.5],
            "price_move_percentiles": [70, 80, 90],
            "failure_lookbacks": [12, 24, 48, 96],
            "exit_grid": {"take_profit_bps": TP_GRID, "stop_loss_bps": SL_GRID, "max_hold_bars": MAX_HOLD_GRID},
        },
        "variant_count": len(rows),
        "status_counts": dict(Counter(row["status"] for row in rows)),
        "top_variants": top_rows,
    }
    csv_rows = []
    for row in rows:
        exit_policy = row.get("best_exit_policy") or {}
        csv_rows.append(
            {
                "variant_id": row["variant_id"],
                "family": row["family"],
                "direction": row["direction"],
                "event_count": row["event_count"],
                "event_rate_bucket": row["event_rate_bucket"],
                "cooldown_bars": row["cooldown_bars"],
                "symbol_scope": ",".join(row["symbol_scope"]),
                "params": json.dumps(row["params"], sort_keys=True),
                "best_horizon_bars": row["best_horizon_bars"],
                "gross_bps": row["gross_bps"],
                "net_bps": row["net_bps"],
                "t_stat": row["t_stat"],
                "cost_survival": row["cost_survival"],
                "mfe_bps": row["mfe_bps"],
                "mae_bps": row["mae_bps"],
                "edge_ratio": row["edge_ratio"],
                "mfe_hit_rate_after_cost": row["mfe_hit_rate_after_cost"],
                "mae_exceeds_cost_rate": row["mae_exceeds_cost_rate"],
                "best_exit": exit_policy.get("policy"),
                "exit_net_bps": exit_policy.get("net_bps"),
                "exit_t_stat": exit_policy.get("t_stat"),
                "exit_hit_rate": exit_policy.get("hit_rate"),
                "avg_hold_bars": exit_policy.get("avg_hold_bars"),
                "max_drawdown_bps": exit_policy.get("max_drawdown_bps"),
                "status": row["status"],
                "score": row["score"],
            }
        )
    return report, pd.DataFrame(csv_rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Search no-liquidation detector event definitions for research-only edge candidates")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in DEFAULT_YEARS))
    parser.add_argument("--horizons", default=",".join(str(horizon) for horizon in DEFAULT_HORIZONS))
    parser.add_argument("--cost-bps", type=float, default=6.0)
    parser.add_argument("--cooldown-bars", default="6,12,24")
    parser.add_argument("--max-raw-variants", type=int, default=20000)
    parser.add_argument("--top-n", type=int, default=250)
    parser.add_argument("--json-output", default="data/reports/detectors/no_liquidations_v1/tuning_lab_report.json")
    parser.add_argument("--csv-output", default="data/reports/detectors/no_liquidations_v1/top_event_variants.csv")
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[2]
    symbols = [item.upper() for item in _parse_csv(args.symbols)]
    years = _parse_years(args.years)
    horizons = _parse_ints(args.horizons)
    cooldowns = _parse_ints(args.cooldown_bars)
    if len(cooldowns) == 1:
        report, csv = build_tuning_report(
            repo_root=repo_root,
            symbols=symbols,
            years=years,
            horizons=horizons,
            cost_bps=float(args.cost_bps),
            cooldown_bars=cooldowns[0],
            max_raw_variants=int(args.max_raw_variants),
            top_n=int(args.top_n),
        )
    else:
        reports: list[dict[str, Any]] = []
        csv_frames: list[pd.DataFrame] = []
        for cooldown in cooldowns:
            sub_report, sub_csv = build_tuning_report(
                repo_root=repo_root,
                symbols=symbols,
                years=years,
                horizons=horizons,
                cost_bps=float(args.cost_bps),
                cooldown_bars=cooldown,
                max_raw_variants=int(args.max_raw_variants),
                top_n=int(args.top_n),
            )
            reports.append(sub_report)
            csv_frames.append(sub_csv)
        all_top = [item for sub_report in reports for item in sub_report["top_variants"]]
        all_top.sort(key=lambda item: item.get("score", 0.0), reverse=True)
        status_counts: Counter[str] = Counter()
        for sub_report in reports:
            status_counts.update(sub_report["status_counts"])
        report = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "scope": {
                "symbols": symbols,
                "years": years,
                "timeframe": "5m",
                "horizons": horizons,
                "cost_round_trip_bps": float(args.cost_bps),
                "cooldown_bars": cooldowns,
            },
            "input_summary": reports[0]["input_summary"] if reports else {},
            "search_dimensions": reports[0]["search_dimensions"] if reports else {},
            "variant_count": sum(int(sub_report["variant_count"]) for sub_report in reports),
            "status_counts": dict(status_counts),
            "top_variants": all_top[: int(args.top_n)],
        }
        report["search_dimensions"]["cooldown_bars"] = cooldowns
        csv = pd.concat(csv_frames, ignore_index=True).sort_values("score", ascending=False) if csv_frames else pd.DataFrame()
    json_output = repo_root / args.json_output
    csv_output = repo_root / args.csv_output
    json_output.parent.mkdir(parents=True, exist_ok=True)
    csv_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    csv.to_csv(csv_output, index=False)
    print(
        json.dumps(
            {
                "status": "pass",
                "json_output": str(json_output),
                "csv_output": str(csv_output),
                "variant_count": report["variant_count"],
                "status_counts": report["status_counts"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
