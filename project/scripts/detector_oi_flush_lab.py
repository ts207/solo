from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.scripts.detector_shadow_report import _return_summary
from project.scripts.detector_tuning_lab import (
    _cooldown_indices_by_symbol,
    _direction_mult,
    _make_variant_id,
    _parse_csv,
    _parse_ints,
)


DEFAULT_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "LINKUSDT",
    "AVAXUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "LTCUSDT",
)
DEFAULT_YEARS = (2022, 2023, 2024, 2025)
DEFAULT_PRICE_THRESHOLDS = (70.0, 80.0, 90.0, 95.0)
DEFAULT_OI_THRESHOLDS = (80.0, 90.0, 95.0, 97.5)
DEFAULT_VOLUME_MODES = ("optional", "required")
DEFAULT_CONFIRM_MODES = ("optional", "required")
DEFAULT_WICK_MODES = ("optional", "required")
DEFAULT_VOL_REGIMES = ("any",)
DEFAULT_TREND_REGIMES = ("any",)
DEEP_VOL_REGIMES = ("any", "low_vol", "mid_vol", "high_vol")
DEEP_TREND_REGIMES = ("any", "uptrend", "downtrend", "chop")
DEFAULT_COOLDOWN_BARS = 12
DEFAULT_REPORT_DIR = Path("data/reports/detectors/no_liquidations_v1")
DEFAULT_COST_BPS_BY_SYMBOL = {
    "BTCUSDT": 6.0,
    "ETHUSDT": 6.0,
    "SOLUSDT": 10.0,
    "BNBUSDT": 10.0,
    "XRPUSDT": 12.0,
    "LINKUSDT": 12.0,
    "AVAXUSDT": 15.0,
    "ADAUSDT": 15.0,
    "DOGEUSDT": 15.0,
    "LTCUSDT": 15.0,
}
TIME_STOP_POLICIES = (
    {"name": "time_stop12_max48", "time_stop_bars": 12, "max_hold_bars": 48},
    {"name": "time_stop24_max96", "time_stop_bars": 24, "max_hold_bars": 96},
)
TP_BPS_GRID = (10.0, 15.0, 25.0, 40.0, 60.0)
SL_BPS_GRID = (10.0, 15.0, 25.0, 40.0, 60.0)
TP_SL_MAX_HOLD_GRID = (48, 96)


QUADRANTS = (
    {
        "family": "OI_FLUSH_DOWN_CONTINUATION",
        "price_side": "down",
        "trade_shape": "continuation",
        "direction": "short",
        "confirmation_column": "close_near_low",
        "failure_column": "failed_breakdown_reclaim_24",
        "wick_column": "close_near_low",
        "approval_requires_book": False,
    },
    {
        "family": "OI_FLUSH_DOWN_REVERSAL",
        "price_side": "down",
        "trade_shape": "reversal",
        "direction": "long",
        "confirmation_column": "failed_breakdown_reclaim_24",
        "failure_column": "failed_breakdown_reclaim_24",
        "wick_column": "failed_breakdown_wick_24",
        "approval_requires_book": True,
    },
    {
        "family": "OI_FLUSH_UP_CONTINUATION",
        "price_side": "up",
        "trade_shape": "continuation",
        "direction": "long",
        "confirmation_column": "close_near_high",
        "failure_column": "failed_breakout_rejection_24",
        "wick_column": "close_near_high",
        "approval_requires_book": False,
    },
    {
        "family": "OI_FLUSH_UP_REVERSAL",
        "price_side": "up",
        "trade_shape": "reversal",
        "direction": "short",
        "confirmation_column": "failed_breakout_rejection_24",
        "failure_column": "failed_breakout_rejection_24",
        "wick_column": "failed_breakout_wick_24",
        "approval_requires_book": True,
    },
)


def _safe_mean(values: list[float]) -> float | None:
    clean = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    return float(np.mean(clean)) if clean else None


def _parse_cost_overrides(raw: str | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for item in _parse_csv(raw or ""):
        if ":" not in item:
            continue
        symbol, value = item.split(":", 1)
        out[symbol.strip().upper()] = float(value)
    return out


def _cost_for_symbol(symbol: str, overrides: dict[str, float]) -> float:
    token = str(symbol).strip().upper()
    return float(overrides.get(token, DEFAULT_COST_BPS_BY_SYMBOL.get(token, 18.0)))


def _read_many(repo_root: Path, patterns: list[str]) -> pd.DataFrame:
    files: list[Path] = []
    for pattern in patterns:
        files.extend(sorted(repo_root.glob(pattern)))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(path) for path in files], ignore_index=True).sort_values("timestamp")


def _global_pct_rank(series: pd.Series) -> pd.Series:
    ranked = pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan)
    return ranked.rank(pct=True).fillna(0.0) * 100.0


def _prepare_oi_flush_frame(repo_root: Path, symbol: str, years: list[int]) -> pd.DataFrame:
    bars = _read_many(
        repo_root,
        [
            f"data/lake/raw/bybit/perp/{symbol}/ohlcv_5m/year={year}/month=*/ohlcv_{symbol}_5m_{year}-*.parquet"
            for year in years
        ],
    )
    oi = _read_many(
        repo_root,
        [
            f"data/lake/raw/bybit/perp/{symbol}/open_interest/year={year}/month=*/oi_{symbol}_{year}-*.parquet"
            for year in years
        ],
    )
    funding = _read_many(
        repo_root,
        [
            f"data/lake/raw/bybit/perp/{symbol}/funding/year={year}/month=*/funding_{symbol}_{year}-*.parquet"
            for year in years
        ],
    )
    if bars.empty or oi.empty or funding.empty:
        raise RuntimeError(
            f"missing required bars/OI/funding for {symbol}: "
            f"bars={len(bars)} oi={len(oi)} funding={len(funding)}"
        )
    for frame in (bars, oi, funding):
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    bars = bars.sort_values("timestamp").drop_duplicates("timestamp")
    oi = oi.sort_values("timestamp").drop_duplicates("timestamp")
    funding = funding.sort_values("timestamp").drop_duplicates("timestamp")
    out = bars.merge(oi[["timestamp", "open_interest"]], on="timestamp", how="left")
    out = pd.merge_asof(
        out.sort_values("timestamp"),
        funding[["timestamp", "funding_rate"]].sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )
    close = pd.to_numeric(out["close"], errors="coerce")
    high = pd.to_numeric(out["high"], errors="coerce")
    low = pd.to_numeric(out["low"], errors="coerce")
    open_ = pd.to_numeric(out["open"], errors="coerce")
    volume = pd.to_numeric(out.get("volume", 0.0), errors="coerce").fillna(0.0)
    out["open_interest"] = pd.to_numeric(out["open_interest"], errors="coerce").ffill()
    out["funding_rate_scaled"] = pd.to_numeric(out["funding_rate"], errors="coerce").ffill().fillna(0.0)
    out["oi_notional"] = out["open_interest"] * close

    out["ret_12"] = close.pct_change(12)
    out["price_move_abs_pct_12"] = _global_pct_rank(out["ret_12"].abs())
    out["price_up_12"] = out["ret_12"] > 0.0
    out["price_down_12"] = out["ret_12"] < 0.0
    out["oi_chg_12"] = np.log(out["oi_notional"].replace(0.0, np.nan)).diff(12)
    out["oi_abs_pct_12"] = _global_pct_rank(out["oi_chg_12"].abs())
    out["oi_down_12"] = out["oi_chg_12"] < 0.0

    true_range = (high - low).replace(0.0, np.nan)
    body = (close - open_).abs().replace(0.0, np.nan)
    out["close_location"] = ((close - low) / true_range).clip(0.0, 1.0)
    out["close_near_low"] = out["close_location"] <= 0.25
    out["close_near_high"] = out["close_location"] >= 0.75
    out["upper_wick_body"] = ((high - np.maximum(open_, close)) / body).replace([np.inf, -np.inf], np.nan)
    out["lower_wick_body"] = ((np.minimum(open_, close) - low) / body).replace([np.inf, -np.inf], np.nan)
    out["volume_z"] = ((volume - volume.rolling(288, min_periods=48).mean()) / volume.rolling(288, min_periods=48).std()).replace([np.inf, -np.inf], np.nan)

    prior_low = low.shift(1).rolling(24, min_periods=12).min()
    prior_high = high.shift(1).rolling(24, min_periods=12).max()
    out["failed_breakdown_reclaim_24"] = (low < prior_low) & (close > prior_low)
    out["failed_breakdown_wick_24"] = out["failed_breakdown_reclaim_24"] & (out["lower_wick_body"] >= 1.5)
    out["failed_breakout_rejection_24"] = (high > prior_high) & (close < prior_high)
    out["failed_breakout_wick_24"] = out["failed_breakout_rejection_24"] & (out["upper_wick_body"] >= 1.5)

    ma_fast = close.rolling(96, min_periods=24).mean()
    ma_slow = close.rolling(288, min_periods=72).mean()
    trend_spread = (ma_fast / ma_slow - 1.0).replace([np.inf, -np.inf], np.nan)
    out["trend_regime"] = np.select([trend_spread > 0.01, trend_spread < -0.01], ["uptrend", "downtrend"], default="chop")
    rv_96 = close.pct_change().rolling(96, min_periods=12).std() * math.sqrt(96)
    vol_rank = _global_pct_rank(rv_96)
    out["shadow_vol_regime"] = np.select([vol_rank >= 75.0, vol_rank >= 45.0], ["high_vol", "mid_vol"], default="low_vol")
    out["shadow_year"] = out["timestamp"].dt.year.astype(str)
    out["shadow_month"] = out["timestamp"].dt.strftime("%Y-%m")
    out["funding_sign"] = np.select(
        [out["funding_rate_scaled"] > 0.0, out["funding_rate_scaled"] < 0.0],
        ["positive", "negative"],
        default="zero",
    )
    hour = out["timestamp"].dt.hour
    out["session"] = np.select(
        [hour.between(0, 7), hour.between(8, 13), hour.between(14, 21)],
        ["asia", "europe", "us"],
        default="late_us",
    )
    return out.reset_index(drop=True)


def _load_frames(repo_root: Path, symbols: list[str], years: list[int]) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
    frames: dict[str, pd.DataFrame] = {}
    missing: dict[str, str] = {}
    for symbol in symbols:
        try:
            frame = _prepare_oi_flush_frame(repo_root, symbol, years)
        except Exception as exc:
            missing[symbol] = str(exc)
            continue
        frame["symbol"] = symbol
        frames[symbol] = frame
    return frames, missing


def _add_symbol_bounds(df: pd.DataFrame) -> pd.DataFrame:
    out = df.sort_values(["symbol", "timestamp"]).reset_index(drop=True).copy()
    out["_symbol_end_idx"] = 0
    for _, group in out.groupby("symbol", sort=False):
        out.loc[group.index, "_symbol_end_idx"] = int(group.index.max())
    return out


def _base_oi_flush_mask(df: pd.DataFrame, *, price_side: str, price_pct: float, oi_pct: float) -> pd.Series:
    price_col = "price_down_12" if price_side == "down" else "price_up_12"
    return (
        df[price_col].fillna(False)
        & df["oi_down_12"].fillna(False)
        & (df["price_move_abs_pct_12"] >= float(price_pct))
        & (df["oi_abs_pct_12"] >= float(oi_pct))
    )


def _variant_mask(
    df: pd.DataFrame,
    quadrant: dict[str, Any],
    *,
    price_pct: float,
    oi_pct: float,
    volume_mode: str,
    confirm_mode: str,
    wick_mode: str,
    vol_regime: str,
    trend_regime: str,
) -> pd.Series:
    mask = _base_oi_flush_mask(df, price_side=str(quadrant["price_side"]), price_pct=price_pct, oi_pct=oi_pct)
    if volume_mode == "required":
        mask &= df["volume_z"].fillna(-999.0) >= 1.0
    if confirm_mode == "required":
        mask &= df[str(quadrant["confirmation_column"])].fillna(False)
    elif quadrant["trade_shape"] == "continuation":
        # Continuation rows should not be failed-continuation reversals.
        mask &= ~df[str(quadrant["failure_column"])].fillna(False)
    if wick_mode == "required":
        mask &= df[str(quadrant["wick_column"])].fillna(False)
    if vol_regime != "any":
        mask &= df["shadow_vol_regime"].astype(str) == vol_regime
    if trend_regime != "any":
        mask &= df["trend_regime"].astype(str) == trend_regime
    return mask.fillna(False)


def _policy_defs(*, include_tp_sl: bool = True) -> list[dict[str, Any]]:
    policies: list[dict[str, Any]] = [dict(policy, kind="time_stop") for policy in TIME_STOP_POLICIES]
    if not include_tp_sl:
        return policies
    for tp_bps in TP_BPS_GRID:
        for sl_bps in SL_BPS_GRID:
            for max_hold_bars in TP_SL_MAX_HOLD_GRID:
                policies.append(
                    {
                        "kind": "tp_sl",
                        "name": f"tp{int(tp_bps)}_sl{int(sl_bps)}_max{int(max_hold_bars)}",
                        "tp_bps": float(tp_bps),
                        "sl_bps": float(sl_bps),
                        "max_hold_bars": int(max_hold_bars),
                    }
                )
    return policies


def _bounded_path(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    symbol_end_idx: np.ndarray,
    idx: int,
    direction: str,
    max_hold_bars: int,
) -> tuple[list[float], list[float]]:
    mult = _direction_mult(direction)
    entry = close[idx]
    max_end = min(int(symbol_end_idx[idx]), idx + int(max_hold_bars))
    fav_path: list[float] = []
    adv_path: list[float] = []
    for step_idx in range(idx + 1, max_end + 1):
        if mult > 0:
            fav_path.append(float((high[step_idx] / entry - 1.0) * 10000.0))
            adv_path.append(float((low[step_idx] / entry - 1.0) * 10000.0))
        else:
            fav_path.append(float((entry / low[step_idx] - 1.0) * 10000.0))
            adv_path.append(float((entry / high[step_idx] - 1.0) * 10000.0))
    return fav_path, adv_path


def _simulate_policy(
    df: pd.DataFrame,
    arrays: dict[str, np.ndarray],
    indices: np.ndarray,
    direction: str,
    policy: dict[str, Any],
    symbol_costs: dict[str, float],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    close = arrays["close"]
    high = arrays["high"]
    low = arrays["low"]
    open_ = arrays["open"]
    symbols = arrays["symbol"]
    years = arrays["year"]
    months = arrays["month"]
    symbol_end_idx = arrays["symbol_end_idx"]
    mult = _direction_mult(direction)
    returns: list[float] = []
    gross_returns: list[float] = []
    holds: list[float] = []
    mfe_values: list[float] = []
    mae_values: list[float] = []
    time_to_mfe: list[float] = []
    time_to_mae: list[float] = []
    first_profit_times: list[float] = []
    mae_before_mfe_values: list[float] = []
    gap_against_count = 0
    event_details: list[dict[str, Any]] = []
    max_hold = int(policy["max_hold_bars"])

    for idx in indices:
        if idx + 1 >= len(close) or int(symbol_end_idx[idx]) <= idx:
            continue
        if not np.isfinite(close[idx]) or close[idx] <= 0.0:
            continue
        entry = close[idx]
        symbol = str(symbols[idx])
        cost_bps = float(symbol_costs.get(symbol, 18.0))
        max_end = min(int(symbol_end_idx[idx]), idx + max_hold)
        if max_end <= idx:
            continue
        if idx + 1 <= int(symbol_end_idx[idx]) and np.isfinite(open_[idx + 1]):
            gap_bps = ((open_[idx + 1] / entry) - 1.0) * 10000.0 * mult
            if gap_bps < 0.0:
                gap_against_count += 1

        fav_path, adv_path = _bounded_path(close, high, low, symbol_end_idx, int(idx), direction, max_hold)
        if fav_path and adv_path:
            mfe = float(np.nanmax(fav_path))
            mae = float(np.nanmin(adv_path))
            mfe_values.append(mfe)
            mae_values.append(mae)
            mfe_step = int(np.nanargmax(fav_path)) + 1
            mae_step = int(np.nanargmin(adv_path)) + 1
            time_to_mfe.append(float(mfe_step))
            time_to_mae.append(float(mae_step))
            if any(value > 0.0 for value in fav_path):
                first_profit_times.append(float(next(i + 1 for i, value in enumerate(fav_path) if value > 0.0)))
            adverse_before = adv_path[:mfe_step]
            if adverse_before:
                mae_before_mfe_values.append(float(np.nanmin(adverse_before)))

        exit_bps: float | None = None
        hold = 0
        if policy["kind"] == "time_stop":
            check_idx = min(max_end, idx + int(policy["time_stop_bars"]))
            check_bps = ((close[check_idx] / entry) - 1.0) * 10000.0 * mult
            if check_bps <= 0.0 or check_idx >= max_end:
                exit_bps = float(check_bps)
                hold = int(check_idx - idx)
            else:
                exit_bps = float(((close[max_end] / entry) - 1.0) * 10000.0 * mult)
                hold = int(max_end - idx)
        else:
            for step_idx in range(idx + 1, max_end + 1):
                if mult > 0:
                    fav = (high[step_idx] / entry - 1.0) * 10000.0
                    adv = (low[step_idx] / entry - 1.0) * 10000.0
                else:
                    fav = (entry / low[step_idx] - 1.0) * 10000.0
                    adv = (entry / high[step_idx] - 1.0) * 10000.0
                hold = int(step_idx - idx)
                if adv <= -float(policy["sl_bps"]):
                    exit_bps = -float(policy["sl_bps"])
                    break
                if fav >= float(policy["tp_bps"]):
                    exit_bps = float(policy["tp_bps"])
                    break
            if exit_bps is None:
                exit_bps = float(((close[max_end] / entry) - 1.0) * 10000.0 * mult)
                hold = int(max_end - idx)

        gross_returns.append(float(exit_bps))
        returns.append(float(exit_bps - cost_bps))
        holds.append(float(hold))
        event_details.append(
            {
                "symbol": symbol,
                "year": str(years[idx]),
                "month": str(months[idx]),
                "symbol_month": f"{symbol}:{months[idx]}",
                "gross_bps": float(exit_bps),
                "net_bps": float(exit_bps - cost_bps),
            }
        )

    net_summary = _return_summary(returns)
    gross_summary = _return_summary(gross_returns)
    net = net_summary.get("mean_bps")
    gross = gross_summary.get("mean_bps")
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in returns:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = min(max_drawdown, cumulative - peak)
    mfe_mean = _safe_mean(mfe_values)
    mae_mean = _safe_mean(mae_values)
    return (
        {
            "policy": str(policy["name"]),
            "kind": str(policy["kind"]),
            "event_count": int(len(returns)),
            "net_bps": net,
            "gross_bps": gross,
            "t_stat": net_summary.get("t_stat"),
            "hit_rate": float(sum(1 for value in returns if value > 0.0) / len(returns)) if returns else None,
            "avg_hold_bars": _safe_mean(holds),
            "max_drawdown_bps": float(max_drawdown),
            "cost_survival": float(net / gross) if net is not None and gross is not None and gross > 0.0 else None,
            "mfe_bps": mfe_mean,
            "mae_bps": mae_mean,
            "edge_ratio": float(mfe_mean / abs(mae_mean)) if mfe_mean is not None and mae_mean not in (None, 0.0) else None,
            "time_to_mfe": _safe_mean(time_to_mfe),
            "time_to_mae": _safe_mean(time_to_mae),
            "time_to_first_profit": _safe_mean(first_profit_times),
            "first_profit_hit_rate": float(len(first_profit_times) / len(returns)) if returns else None,
            "mae_before_mfe_bps": _safe_mean(mae_before_mfe_values),
            "gap_against_entry_rate": float(gap_against_count / len(returns)) if returns else None,
            "total_net_bps": float(np.sum(returns)) if returns else 0.0,
        },
        event_details,
    )


def _return_stats(values: list[float], gross_values: list[float] | None = None) -> dict[str, Any]:
    summary = _return_summary(values)
    gross_summary = _return_summary(gross_values or [])
    net = summary.get("mean_bps")
    gross = gross_summary.get("mean_bps")
    return {
        "event_count": int(len(values)),
        "net_bps": net,
        "gross_bps": gross,
        "t_stat": summary.get("t_stat"),
        "cost_survival": float(net / gross) if net is not None and gross is not None and gross > 0.0 else None,
        "total_net_bps": float(np.sum(values)) if values else 0.0,
    }


def _group_return_stats(event_details: list[dict[str, Any]], key: str) -> dict[str, Any]:
    net_grouped: dict[str, list[float]] = {}
    gross_grouped: dict[str, list[float]] = {}
    for event in event_details:
        label = str(event.get(key, "unknown"))
        net = event.get("net_bps")
        gross = event.get("gross_bps")
        if net is None or gross is None or not math.isfinite(float(net)) or not math.isfinite(float(gross)):
            continue
        net_grouped.setdefault(label, []).append(float(net))
        gross_grouped.setdefault(label, []).append(float(gross))
    return {label: _return_stats(values, gross_grouped.get(label, [])) for label, values in sorted(net_grouped.items())}


def _max_abs_pnl_share(group_stats: dict[str, Any]) -> tuple[float | None, str | None]:
    totals = {label: abs(float(row.get("total_net_bps") or 0.0)) for label, row in group_stats.items()}
    denom = float(sum(totals.values()))
    if denom <= 0.0 or not totals:
        return None, None
    top_label, top_value = max(totals.items(), key=lambda item: item[1])
    return float(top_value / denom), str(top_label)


def _positive_symbols(by_symbol: dict[str, Any]) -> list[str]:
    return sorted(
        symbol
        for symbol, stats in by_symbol.items()
        if int(stats.get("event_count") or 0) > 0
        and stats.get("net_bps") is not None
        and float(stats["net_bps"]) > 0.0
    )


def _walk_forward(by_year: dict[str, Any]) -> dict[str, Any]:
    validations: dict[str, Any] = {}
    for year in ("2024", "2025"):
        stats = by_year.get(year, {})
        validations[year] = {
            "event_count": int(stats.get("event_count") or 0),
            "net_bps": stats.get("net_bps"),
            "t_stat": stats.get("t_stat"),
            "cost_survival": stats.get("cost_survival"),
            "passed": (
                int(stats.get("event_count") or 0) >= 20
                and (stats.get("net_bps") or -10**9) > 0.0
                and (stats.get("t_stat") or -10**9) > 2.0
                and (stats.get("cost_survival") or -10**9) >= 0.8
            ),
        }
    return validations


def _status(row: dict[str, Any]) -> str:
    best = row.get("best_exit") or {}
    if int(row.get("event_count") or 0) < 50:
        return "too_rare"
    if int(row.get("event_count") or 0) < 100:
        return "needs_sample_expansion"
    if (best.get("net_bps") or -10**9) <= 0.0:
        return "failed_net"
    if (best.get("t_stat") or -10**9) <= 2.0:
        return "failed_t_stat"
    if (best.get("cost_survival") or -10**9) < 0.8:
        return "failed_cost_survival"
    if len(row.get("positive_symbols") or []) < 3:
        return "symbol_scoped_research_only"
    if (row.get("top_symbol_month_pnl_share") or 0.0) > 0.35:
        return "symbol_month_concentrated_research_only"
    walk_forward = row.get("walk_forward") or {}
    if not any((stats or {}).get("passed") for stats in walk_forward.values()):
        return "walk_forward_failed"
    if bool(row.get("approval_requires_book")):
        return "needs_book_data"
    return "fresh_validation_candidate"


def _score(row: dict[str, Any]) -> float:
    best = row.get("best_exit") or {}
    return float(
        max(0.0, best.get("net_bps") or 0.0)
        + 12.0 * max(0.0, best.get("t_stat") or 0.0)
        + 25.0 * max(0.0, best.get("cost_survival") or 0.0)
        + 6.0 * max(0.0, best.get("edge_ratio") or 0.0)
        + 5.0 * len(row.get("positive_symbols") or [])
        - 50.0 * max(0.0, (row.get("top_symbol_month_pnl_share") or 0.0) - 0.35)
    )


def _evaluate_variant(
    df: pd.DataFrame,
    arrays: dict[str, np.ndarray],
    policies: list[dict[str, Any]],
    symbol_costs: dict[str, float],
    quadrant: dict[str, Any],
    *,
    price_pct: float,
    oi_pct: float,
    volume_mode: str,
    confirm_mode: str,
    wick_mode: str,
    vol_regime: str,
    trend_regime: str,
    cooldown_bars: int,
) -> dict[str, Any]:
    mask = _variant_mask(
        df,
        quadrant,
        price_pct=price_pct,
        oi_pct=oi_pct,
        volume_mode=volume_mode,
        confirm_mode=confirm_mode,
        wick_mode=wick_mode,
        vol_regime=vol_regime,
        trend_regime=trend_regime,
    )
    indices = _cooldown_indices_by_symbol(df, mask, cooldown_bars)
    variant_id = _make_variant_id(
        [
            quadrant["family"],
            price_pct,
            oi_pct,
            volume_mode,
            confirm_mode,
            wick_mode,
            vol_regime,
            trend_regime,
            "CD",
            cooldown_bars,
        ]
    )
    if len(indices) < 20 or len(indices) > 8000:
        status = "too_rare" if len(indices) < 20 else "too_broad"
        return {
            "variant_id": variant_id,
            "base_family": quadrant["family"],
            "trade_shape": quadrant["trade_shape"],
            "direction": quadrant["direction"],
            "event_count": int(len(indices)),
            "params": {
                "price_pct": price_pct,
                "oi_drop_pct": oi_pct,
                "volume_mode": volume_mode,
                "confirm_mode": confirm_mode,
                "wick_mode": wick_mode,
                "vol_regime": vol_regime,
                "trend_regime": trend_regime,
                "cooldown_bars": cooldown_bars,
            },
            "status": status,
            "approval_requires_book": bool(quadrant["approval_requires_book"]),
            "paper_approved": False,
            "live_approved": False,
        }

    time_policies = [policy for policy in policies if policy["kind"] == "time_stop"]
    tp_sl_policies = [policy for policy in policies if policy["kind"] == "tp_sl"]
    best_exit: dict[str, Any] | None = None
    best_details: list[dict[str, Any]] = []
    for policy in time_policies:
        result, details = _simulate_policy(df, arrays, indices, str(quadrant["direction"]), policy, symbol_costs)
        key = (result.get("net_bps") or -10**9, result.get("t_stat") or -10**9, result.get("cost_survival") or -10**9)
        best_key = (
            (best_exit or {}).get("net_bps") or -10**9,
            (best_exit or {}).get("t_stat") or -10**9,
            (best_exit or {}).get("cost_survival") or -10**9,
        )
        if best_exit is None or key > best_key:
            best_exit = result
            best_details = details
    evaluate_tp_sl = (
        50 <= int((best_exit or {}).get("event_count") or 0) <= 1500
        and (
            ((best_exit or {}).get("net_bps") or -10**9) > 0.0
            or ((best_exit or {}).get("t_stat") or -10**9) > 0.75
            or ((best_exit or {}).get("edge_ratio") or 0.0) >= 1.75
        )
    )
    if evaluate_tp_sl:
        for policy in tp_sl_policies:
            result, details = _simulate_policy(df, arrays, indices, str(quadrant["direction"]), policy, symbol_costs)
            key = (result.get("net_bps") or -10**9, result.get("t_stat") or -10**9, result.get("cost_survival") or -10**9)
            best_key = (
                (best_exit or {}).get("net_bps") or -10**9,
                (best_exit or {}).get("t_stat") or -10**9,
                (best_exit or {}).get("cost_survival") or -10**9,
            )
            if key > best_key:
                best_exit = result
                best_details = details

    by_symbol = _group_return_stats(best_details, "symbol")
    by_year = _group_return_stats(best_details, "year")
    by_month = _group_return_stats(best_details, "month")
    by_symbol_month = _group_return_stats(best_details, "symbol_month")
    top_symbol_month_share, top_symbol_month = _max_abs_pnl_share(by_symbol_month)
    top_month_share, top_month = _max_abs_pnl_share(by_month)
    top_symbol_share, top_symbol = _max_abs_pnl_share(by_symbol)
    event_rows = df.iloc[indices]
    regime_slices = {
        "by_symbol": dict(Counter(event_rows["symbol"].astype(str))),
        "by_year": dict(Counter(event_rows["shadow_year"].astype(str))),
        "by_month_top": dict(Counter(event_rows["shadow_month"].astype(str)).most_common(10)),
        "by_vol_regime": dict(Counter(event_rows["shadow_vol_regime"].astype(str))),
        "by_trend_regime": dict(Counter(event_rows["trend_regime"].astype(str))),
        "by_funding_sign": dict(Counter(event_rows["funding_sign"].astype(str))),
        "by_session": dict(Counter(event_rows["session"].astype(str))),
    }
    row = {
        "variant_id": variant_id,
        "base_family": quadrant["family"],
        "trade_shape": quadrant["trade_shape"],
        "direction": quadrant["direction"],
        "event_count": int((best_exit or {}).get("event_count") or len(best_details)),
        "params": {
            "price_pct": price_pct,
            "oi_drop_pct": oi_pct,
            "volume_mode": volume_mode,
            "confirm_mode": confirm_mode,
            "wick_mode": wick_mode,
            "vol_regime": vol_regime,
            "trend_regime": trend_regime,
            "cooldown_bars": cooldown_bars,
        },
        "best_exit": best_exit or {},
        "tp_sl_grid_evaluated": bool(evaluate_tp_sl),
        "positive_symbols": _positive_symbols(by_symbol),
        "by_symbol": by_symbol,
        "by_year": by_year,
        "by_month": by_month,
        "by_symbol_month": by_symbol_month,
        "top_symbol_month_pnl_share": top_symbol_month_share,
        "top_symbol_month": top_symbol_month,
        "top_month_pnl_share": top_month_share,
        "top_month": top_month,
        "top_symbol_pnl_share": top_symbol_share,
        "top_symbol": top_symbol,
        "walk_forward": _walk_forward(by_year),
        "regime_slices": regime_slices,
        "approval_requires_book": bool(quadrant["approval_requires_book"]),
        "paper_approved": False,
        "live_approved": False,
    }
    row["status"] = _status(row)
    row["score"] = _score(row)
    return row


def build_oi_flush_lab_report(
    *,
    repo_root: Path,
    symbols: list[str],
    years: list[int],
    price_thresholds: list[float],
    oi_thresholds: list[float],
    volume_modes: list[str],
    confirm_modes: list[str],
    wick_modes: list[str],
    vol_regimes: list[str],
    trend_regimes: list[str],
    cooldown_bars: int,
    cost_overrides: dict[str, float],
    json_output: Path,
    csv_output: Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    frames_by_symbol, missing = _load_frames(repo_root, symbols, years)
    symbol_costs = {symbol: _cost_for_symbol(symbol, cost_overrides) for symbol in symbols}
    rows: list[dict[str, Any]] = []
    if frames_by_symbol:
        pooled = _add_symbol_bounds(pd.concat(frames_by_symbol.values(), ignore_index=True))
        arrays = {
            "close": pd.to_numeric(pooled["close"], errors="coerce").to_numpy(),
            "open": pd.to_numeric(pooled["open"], errors="coerce").to_numpy(),
            "high": pd.to_numeric(pooled["high"], errors="coerce").to_numpy(),
            "low": pd.to_numeric(pooled["low"], errors="coerce").to_numpy(),
            "symbol": pooled["symbol"].astype(str).to_numpy(),
            "year": pooled["shadow_year"].astype(str).to_numpy(),
            "month": pooled["shadow_month"].astype(str).to_numpy(),
            "symbol_end_idx": pooled["_symbol_end_idx"].to_numpy(dtype=int),
        }
        policies = _policy_defs()
        for quadrant in QUADRANTS:
            for price_pct in price_thresholds:
                for oi_pct in oi_thresholds:
                    for volume_mode in volume_modes:
                        for confirm_mode in confirm_modes:
                            for wick_mode in wick_modes:
                                for vol_regime in vol_regimes:
                                    for trend_regime in trend_regimes:
                                        rows.append(
                                            _evaluate_variant(
                                                pooled,
                                                arrays,
                                                policies,
                                                symbol_costs,
                                                quadrant,
                                                price_pct=float(price_pct),
                                                oi_pct=float(oi_pct),
                                                volume_mode=str(volume_mode),
                                                confirm_mode=str(confirm_mode),
                                                wick_mode=str(wick_mode),
                                                vol_regime=str(vol_regime),
                                                trend_regime=str(trend_regime),
                                                cooldown_bars=int(cooldown_bars),
                                            )
                                        )

    rows.sort(key=lambda item: item.get("score", -10**9), reverse=True)
    status_counts = dict(Counter(row.get("status", "unknown") for row in rows))
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "family": "OI_FLUSH",
            "symbols_requested": symbols,
            "symbols_evaluated": sorted(frames_by_symbol),
            "years": years,
            "timeframe": "5m",
            "quadrants": [str(item["family"]) for item in QUADRANTS],
            "price_thresholds": price_thresholds,
            "oi_drop_thresholds": oi_thresholds,
            "volume_modes": volume_modes,
            "confirm_modes": confirm_modes,
            "wick_modes": wick_modes,
            "vol_regimes": vol_regimes,
            "trend_regimes": trend_regimes,
            "cooldown_bars": cooldown_bars,
            "exit_policy_count": len(_policy_defs()),
            "exit_search": "time_stop_all_variants_tp_sl_only_for_viable_path_rows",
            "cost_bps_by_symbol": symbol_costs,
            "approval_policy": "research_only_outputs; reversal approval requires real spread/depth",
        },
        "missing_symbol_data": missing,
        "candidate_count": len(rows),
        "status_counts": status_counts,
        "top_variants": rows[:50],
        "paper_approved_events": [],
        "live_approved_events": [],
    }
    csv_rows = []
    for row in rows:
        best = row.get("best_exit") or {}
        csv_rows.append(
            {
                "variant_id": row.get("variant_id"),
                "base_family": row.get("base_family"),
                "trade_shape": row.get("trade_shape"),
                "direction": row.get("direction"),
                "event_count": row.get("event_count"),
                "price_pct": (row.get("params") or {}).get("price_pct"),
                "oi_drop_pct": (row.get("params") or {}).get("oi_drop_pct"),
                "volume_mode": (row.get("params") or {}).get("volume_mode"),
                "confirm_mode": (row.get("params") or {}).get("confirm_mode"),
                "wick_mode": (row.get("params") or {}).get("wick_mode"),
                "vol_regime": (row.get("params") or {}).get("vol_regime"),
                "trend_regime": (row.get("params") or {}).get("trend_regime"),
                "best_exit_policy": best.get("policy"),
                "net_bps": best.get("net_bps"),
                "gross_bps": best.get("gross_bps"),
                "t_stat": best.get("t_stat"),
                "hit_rate": best.get("hit_rate"),
                "avg_hold_bars": best.get("avg_hold_bars"),
                "cost_survival": best.get("cost_survival"),
                "mfe_bps": best.get("mfe_bps"),
                "mae_bps": best.get("mae_bps"),
                "edge_ratio": best.get("edge_ratio"),
                "time_to_first_profit": best.get("time_to_first_profit"),
                "mae_before_mfe_bps": best.get("mae_before_mfe_bps"),
                "gap_against_entry_rate": best.get("gap_against_entry_rate"),
                "positive_symbols": ",".join(row.get("positive_symbols") or []),
                "top_symbol_month_pnl_share": row.get("top_symbol_month_pnl_share"),
                "top_symbol_month": row.get("top_symbol_month"),
                "top_month_pnl_share": row.get("top_month_pnl_share"),
                "top_month": row.get("top_month"),
                "top_symbol_pnl_share": row.get("top_symbol_pnl_share"),
                "top_symbol": row.get("top_symbol"),
                "walk_forward_2024_passed": ((row.get("walk_forward") or {}).get("2024") or {}).get("passed"),
                "walk_forward_2025_passed": ((row.get("walk_forward") or {}).get("2025") or {}).get("passed"),
                "approval_requires_book": row.get("approval_requires_book"),
                "tp_sl_grid_evaluated": row.get("tp_sl_grid_evaluated"),
                "status": row.get("status"),
                "score": row.get("score"),
            }
        )
    csv = pd.DataFrame(csv_rows)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    csv_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    csv.to_csv(csv_output, index=False)
    return report, csv


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Research-only OI flush/unwind variant lab")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in DEFAULT_YEARS))
    parser.add_argument("--price-thresholds", default=",".join(str(value) for value in DEFAULT_PRICE_THRESHOLDS))
    parser.add_argument("--oi-thresholds", default=",".join(str(value) for value in DEFAULT_OI_THRESHOLDS))
    parser.add_argument("--volume-modes", default=",".join(DEFAULT_VOLUME_MODES))
    parser.add_argument("--confirm-modes", default=",".join(DEFAULT_CONFIRM_MODES))
    parser.add_argument("--wick-modes", default=",".join(DEFAULT_WICK_MODES))
    parser.add_argument("--vol-regimes", default=",".join(DEFAULT_VOL_REGIMES))
    parser.add_argument("--trend-regimes", default=",".join(DEFAULT_TREND_REGIMES))
    parser.add_argument("--deep-regime-grid", action="store_true")
    parser.add_argument("--cooldown-bars", default=str(DEFAULT_COOLDOWN_BARS))
    parser.add_argument("--cost-overrides", default="")
    parser.add_argument("--json-output", default=str(DEFAULT_REPORT_DIR / "oi_flush_lab_report.json"))
    parser.add_argument("--csv-output", default=str(DEFAULT_REPORT_DIR / "top_oi_flush_variants.csv"))
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[2]
    report, _ = build_oi_flush_lab_report(
        repo_root=repo_root,
        symbols=[item.upper() for item in _parse_csv(args.symbols)],
        years=_parse_ints(args.years),
        price_thresholds=[float(item) for item in _parse_csv(args.price_thresholds)],
        oi_thresholds=[float(item) for item in _parse_csv(args.oi_thresholds)],
        volume_modes=_parse_csv(args.volume_modes),
        confirm_modes=_parse_csv(args.confirm_modes),
        wick_modes=_parse_csv(args.wick_modes),
        vol_regimes=list(DEEP_VOL_REGIMES) if args.deep_regime_grid else _parse_csv(args.vol_regimes),
        trend_regimes=list(DEEP_TREND_REGIMES) if args.deep_regime_grid else _parse_csv(args.trend_regimes),
        cooldown_bars=int(args.cooldown_bars),
        cost_overrides=_parse_cost_overrides(args.cost_overrides),
        json_output=repo_root / args.json_output,
        csv_output=repo_root / args.csv_output,
    )
    print(
        json.dumps(
            {
                "status": "pass",
                "json_output": str(repo_root / args.json_output),
                "csv_output": str(repo_root / args.csv_output),
                "candidate_count": report["candidate_count"],
                "status_counts": report["status_counts"],
                "symbols_evaluated": report["scope"]["symbols_evaluated"],
                "missing_symbols": sorted(report["missing_symbol_data"]),
                "paper_approved_events": report["paper_approved_events"],
                "live_approved_events": report["live_approved_events"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
