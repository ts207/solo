from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.scripts.detector_shadow_report import (
    _prepare_symbol_frame,
    _return_summary,
    _rolling_pct_rank,
)
from project.scripts.detector_targeted_expansion import (
    DEFAULT_COST_BPS_BY_SYMBOL,
    _max_share,
    _parse_cost_overrides,
    _parse_exit_policy,
)
from project.scripts.detector_tuning_lab import _parse_csv, _parse_ints

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
DEFAULT_REPORT_DIR = Path("data/reports/detectors/no_liquidations_v1")
DEFAULT_LOOKBACKS = (12, 48)
DEFAULT_FUNDING_PCTS = (80.0, 90.0, 95.0)
DEFAULT_EXIT_POLICIES = ("time_stop12_max48", "time_stop24_max96")
DEFAULT_COOLDOWN_BARS = 12
DEFAULT_EXTRA_SLIPPAGE_BPS = 10.0
FAMILY = "FUNDING_DIVERGENCE"


VARIANT_SPECS = (
    {
        "variant": "PRICE_UP_FUNDING_DOWN_CONTINUATION_LONG",
        "price_side": "up",
        "funding_side": "down",
        "direction_mode": "fixed_long",
        "failed_confirm_side": "up",
    },
    {
        "variant": "PRICE_UP_FUNDING_HIGH_FAILED_BREAKOUT_SHORT",
        "price_side": "up",
        "funding_side": "positive_high",
        "direction_mode": "fixed_short",
        "failed_confirm_side": "up",
    },
    {
        "variant": "PRICE_DOWN_FUNDING_UP_CONTINUATION_SHORT",
        "price_side": "down",
        "funding_side": "up",
        "direction_mode": "fixed_short",
        "failed_confirm_side": "down",
    },
    {
        "variant": "PRICE_DOWN_FUNDING_NEG_FAILED_BREAKDOWN_LONG",
        "price_side": "down",
        "funding_side": "negative_high",
        "direction_mode": "fixed_long",
        "failed_confirm_side": "down",
    },
    {
        "variant": "FUNDING_SIGN_FLIP_CONTINUATION",
        "price_side": "either",
        "funding_side": "sign_flip",
        "direction_mode": "price_continuation",
        "failed_confirm_side": "price",
    },
    {
        "variant": "FUNDING_SIGN_FLIP_REVERSAL",
        "price_side": "either",
        "funding_side": "sign_flip",
        "direction_mode": "price_reversal",
        "failed_confirm_side": "price",
    },
)


def _cost_for_symbol(symbol: str, overrides: dict[str, float]) -> float:
    return float(
        overrides.get(symbol.upper(), DEFAULT_COST_BPS_BY_SYMBOL.get(symbol.upper(), 18.0))
    )


def _direction_mult(direction: str) -> float:
    side = str(direction).strip().lower()
    if side == "long":
        return 1.0
    if side == "short":
        return -1.0
    raise ValueError(f"unsupported direction: {direction}")


def _add_funding_divergence_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values(["symbol", "timestamp"]).reset_index(drop=True).copy()
    close = pd.to_numeric(out["close"], errors="coerce")
    high = pd.to_numeric(out["high"], errors="coerce")
    low = pd.to_numeric(out["low"], errors="coerce")
    open_ = pd.to_numeric(out["open"], errors="coerce")
    volume = pd.to_numeric(out.get("volume", 0.0), errors="coerce").fillna(0.0)
    oi = pd.to_numeric(out.get("oi_notional", np.nan), errors="coerce")
    funding = pd.to_numeric(out.get("funding_rate_scaled", 0.0), errors="coerce").fillna(0.0)
    grouped = out.groupby("symbol", sort=False)

    out["price_ret_12"] = grouped["close"].pct_change(12)
    out["price_ret_48"] = grouped["close"].pct_change(48)
    out["funding_level"] = funding
    out["funding_slope_3"] = grouped["funding_rate_scaled"].diff(3)
    out["funding_slope_6"] = grouped["funding_rate_scaled"].diff(6)
    out["funding_abs_percentile"] = (
        funding.abs()
        .groupby(out["symbol"], sort=False)
        .transform(lambda series: _rolling_pct_rank(series, window=2880, min_periods=288))
    )
    out["funding_sign"] = np.select(
        [funding > 0.0, funding < 0.0], ["positive", "negative"], default="zero"
    )
    prior_funding = grouped["funding_rate_scaled"].shift(1).fillna(0.0)
    out["funding_sign_flip"] = ((funding > 0.0) & (prior_funding < 0.0)) | (
        (funding < 0.0) & (prior_funding > 0.0)
    )
    out["oi_change_12"] = grouped["oi_notional"].transform(
        lambda series: np.log(pd.to_numeric(series, errors="coerce").replace(0.0, np.nan)).diff(12)
    )
    out["volume_z"] = grouped["volume"].transform(
        lambda series: (
            (
                pd.to_numeric(series, errors="coerce")
                - pd.to_numeric(series, errors="coerce").rolling(288, min_periods=48).mean()
            )
            / pd.to_numeric(series, errors="coerce").rolling(288, min_periods=48).std()
        )
    )
    prior_high_24 = grouped["high"].transform(
        lambda series: (
            pd.to_numeric(series, errors="coerce").shift(1).rolling(24, min_periods=12).max()
        )
    )
    prior_low_24 = grouped["low"].transform(
        lambda series: (
            pd.to_numeric(series, errors="coerce").shift(1).rolling(24, min_periods=12).min()
        )
    )
    out["failed_breakout_rejection_24"] = (high > prior_high_24) & (close < prior_high_24)
    out["failed_breakdown_reclaim_24"] = (low < prior_low_24) & (close > prior_low_24)

    ma_fast = grouped["close"].transform(
        lambda series: pd.to_numeric(series, errors="coerce").rolling(96, min_periods=24).mean()
    )
    ma_slow = grouped["close"].transform(
        lambda series: pd.to_numeric(series, errors="coerce").rolling(288, min_periods=72).mean()
    )
    trend_spread = (ma_fast / ma_slow - 1.0).replace([np.inf, -np.inf], np.nan)
    out["trend_regime"] = np.select(
        [trend_spread > 0.01, trend_spread < -0.01], ["uptrend", "downtrend"], default="chop"
    )
    rv_96 = (
        grouped["close"]
        .pct_change()
        .groupby(out["symbol"], sort=False)
        .transform(
            lambda series: (
                pd.to_numeric(series, errors="coerce").rolling(96, min_periods=24).std()
                * math.sqrt(96)
            )
        )
    )
    vol_rank = rv_96.groupby(out["symbol"], sort=False).transform(
        lambda series: _rolling_pct_rank(series, window=2880, min_periods=288)
    )
    out["vol_regime"] = np.select(
        [vol_rank >= 75.0, vol_rank >= 45.0], ["high_vol", "mid_vol"], default="low_vol"
    )
    out["shadow_year"] = out["timestamp"].dt.year.astype(str)
    out["shadow_month"] = out["timestamp"].dt.strftime("%Y-%m")
    out["close"] = close
    out["high"] = high
    out["low"] = low
    out["open"] = open_
    out["volume"] = volume
    out["oi_notional"] = oi
    out["funding_rate_scaled"] = funding
    return out


def _load_frames(
    repo_root: Path, symbols: list[str], years: list[int]
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    frames: list[pd.DataFrame] = []
    missing: dict[str, Any] = {}
    summary: dict[str, Any] = {}
    for symbol in symbols:
        try:
            frame = _prepare_symbol_frame(repo_root, symbol, years)
        except Exception as exc:
            missing[symbol] = str(exc)
            continue
        frame["symbol"] = symbol
        frames.append(frame)
        summary[symbol] = {
            "rows": len(frame),
            "start": str(frame["timestamp"].min()),
            "end": str(frame["timestamp"].max()),
            "years": sorted(frame["shadow_year"].dropna().unique().tolist()),
        }
    if not frames:
        raise RuntimeError("no symbols had complete OHLCV/OI/funding data")
    pooled = (
        pd.concat(frames, ignore_index=True)
        .sort_values(["symbol", "timestamp"])
        .reset_index(drop=True)
    )
    return _add_funding_divergence_features(pooled), missing, summary


def _array_context(df: pd.DataFrame, *, candidate_stride_bars: int) -> dict[str, Any]:
    symbols = df["symbol"].astype(str).to_numpy()
    boundaries = np.flatnonzero(symbols[1:] != symbols[:-1]) + 1
    starts = np.r_[0, boundaries]
    ends = np.r_[boundaries, len(symbols)]
    eval_indices = np.concatenate(
        [
            np.arange(start, end, candidate_stride_bars, dtype=int)
            for start, end in zip(starts, ends, strict=False)
        ]
    )
    price_ret = {
        12: pd.to_numeric(df["price_ret_12"], errors="coerce").to_numpy(dtype=float),
        48: pd.to_numeric(df["price_ret_48"], errors="coerce").to_numpy(dtype=float),
    }
    return {
        "close": pd.to_numeric(df["close"], errors="coerce").to_numpy(dtype=float),
        "price_ret": price_ret,
        "funding_level": pd.to_numeric(df["funding_level"], errors="coerce")
        .fillna(0.0)
        .to_numpy(dtype=float),
        "funding_slope_3": pd.to_numeric(df["funding_slope_3"], errors="coerce")
        .fillna(0.0)
        .to_numpy(dtype=float),
        "funding_slope_6": pd.to_numeric(df["funding_slope_6"], errors="coerce")
        .fillna(0.0)
        .to_numpy(dtype=float),
        "funding_abs_percentile": pd.to_numeric(df["funding_abs_percentile"], errors="coerce")
        .fillna(0.0)
        .to_numpy(dtype=float),
        "funding_sign_flip": df["funding_sign_flip"].fillna(False).to_numpy(dtype=bool),
        "oi_change": pd.to_numeric(df["oi_change_12"], errors="coerce")
        .fillna(0.0)
        .to_numpy(dtype=float),
        "volume_z": pd.to_numeric(df["volume_z"], errors="coerce")
        .fillna(0.0)
        .to_numpy(dtype=float),
        "failed_breakout": df["failed_breakout_rejection_24"].fillna(False).to_numpy(dtype=bool),
        "failed_breakdown": df["failed_breakdown_reclaim_24"].fillna(False).to_numpy(dtype=bool),
        "trend": df["trend_regime"].astype(str).to_numpy(),
        "vol_regime": df["vol_regime"].astype(str).to_numpy(),
        "symbols": symbols,
        "years": df["shadow_year"].astype(str).to_numpy(),
        "months": df["shadow_month"].astype(str).to_numpy(),
        "eval_indices": eval_indices,
        "symbol_slices": list(zip(starts.astype(int), ends.astype(int), strict=False)),
    }


def _cooldown_indices_from_mask(
    mask: np.ndarray, symbol_slices: list[tuple[int, int]], cooldown_bars: int
) -> np.ndarray:
    kept: list[int] = []
    for start, end in symbol_slices:
        raw = np.flatnonzero(mask[start:end]) + start
        if len(raw) == 0:
            continue
        blocks = (raw - start) // cooldown_bars
        _, first = np.unique(blocks, return_index=True)
        kept.extend(raw[np.sort(first)].astype(int).tolist())
    return np.asarray(kept, dtype=int)


def _trade_directions(spec: dict[str, str], price_ret: np.ndarray) -> np.ndarray:
    mode = spec["direction_mode"]
    if mode == "fixed_long":
        return np.full(len(price_ret), "long", dtype=object)
    if mode == "fixed_short":
        return np.full(len(price_ret), "short", dtype=object)
    if mode == "price_continuation":
        return np.where(price_ret >= 0.0, "long", "short").astype(object)
    if mode == "price_reversal":
        return np.where(price_ret >= 0.0, "short", "long").astype(object)
    raise ValueError(f"unsupported direction mode: {mode}")


def _walk_forward_pass(by_year: dict[str, Any]) -> bool:
    years = sorted(by_year)
    if len(years) < 2:
        return False
    oos_years = years[1:]
    if len(oos_years) < 2:
        return all((by_year[year].get("net_bps") or -(10**9)) > 0.0 for year in oos_years)
    positive = sum(1 for year in oos_years if (by_year[year].get("net_bps") or -(10**9)) > 0.0)
    return positive >= max(2, math.ceil(0.67 * len(oos_years)))


def _return_stats(values: np.ndarray) -> dict[str, Any]:
    clean = [float(value) for value in values if np.isfinite(float(value))]
    summary = _return_summary(clean)
    return {
        "event_count": len(clean),
        "net_bps": summary.get("mean_bps"),
        "t_stat": summary.get("t_stat"),
        "total_net_bps": float(np.sum(clean)) if clean else 0.0,
    }


def _group_return_stats_arrays(labels: np.ndarray, returns: np.ndarray) -> dict[str, Any]:
    labels_str = labels.astype(str)
    return {
        label: _return_stats(returns[labels_str == label])
        for label in sorted(set(labels_str.tolist()))
    }


def _max_abs_pnl_share_arrays(labels: np.ndarray, returns: np.ndarray) -> float | None:
    labels_str = labels.astype(str)
    totals = [
        abs(float(np.nansum(returns[labels_str == label])))
        for label in sorted(set(labels_str.tolist()))
    ]
    denom = float(sum(totals))
    if denom <= 0.0:
        return None
    return float(max(totals) / denom)


def _status(row: dict[str, Any]) -> str:
    best = row.get("best_exit") or {}
    if int(row.get("event_count") or 0) < 100:
        return "needs_sample_expansion"
    if (row.get("top_symbol_month_share") or 0.0) > 0.35 or (
        row.get("top_month_event_share") or 0.0
    ) > 0.35:
        return "regime_artifact_research_only"
    if (best.get("net_bps") or -(10**9)) <= 0.0:
        return "failed_net"
    if (best.get("t_stat") or -(10**9)) <= 2.0:
        return "failed_t_stat"
    if (best.get("cost_survival") or -(10**9)) < 0.8:
        return "failed_cost_survival"
    if len(row.get("positive_symbols") or []) < 3:
        return "symbol_scoped_research_only"
    if not bool(row.get("walk_forward", {}).get("pass")):
        return "walk_forward_failed"
    if not bool((best.get("slippage_plus_10_bps") or {}).get("survives")):
        return "failed_plus_10_bps_slippage"
    return "fresh_validation_candidate"


def _variant_mask(
    context: dict[str, Any],
    *,
    spec: dict[str, str],
    lookback: int,
    funding_pct: float,
    funding_slope_mode: str,
    oi_mode: str,
    confirm_mode: str,
    trend_filter: str,
) -> tuple[np.ndarray, np.ndarray]:
    eval_indices = context["eval_indices"]
    price_ret = context["price_ret"][lookback][eval_indices]
    funding_level = context["funding_level"][eval_indices]
    slope_3 = context["funding_slope_3"][eval_indices]
    slope_6 = context["funding_slope_6"][eval_indices]
    directions = _trade_directions(spec, price_ret)
    mask = np.isfinite(price_ret) & (context["funding_abs_percentile"][eval_indices] >= funding_pct)

    if spec["price_side"] == "up":
        mask &= price_ret > 0.0
    elif spec["price_side"] == "down":
        mask &= price_ret < 0.0
    elif spec["price_side"] == "either":
        mask &= price_ret != 0.0
    else:
        raise ValueError(f"unsupported price side: {spec['price_side']}")

    funding_side = spec["funding_side"]
    if funding_side == "down":
        mask &= slope_6 < 0.0
    elif funding_side == "up":
        mask &= slope_6 > 0.0
    elif funding_side == "positive_high":
        mask &= funding_level > 0.0
    elif funding_side == "negative_high":
        mask &= funding_level < 0.0
    elif funding_side == "sign_flip":
        mask &= context["funding_sign_flip"][eval_indices]
    else:
        raise ValueError(f"unsupported funding side: {funding_side}")

    if funding_slope_mode == "falling":
        mask &= (slope_3 < 0.0) & (slope_6 < 0.0)
    elif funding_slope_mode == "rising":
        mask &= (slope_3 > 0.0) & (slope_6 > 0.0)
    elif funding_slope_mode != "any":
        raise ValueError(f"unsupported funding slope mode: {funding_slope_mode}")

    if oi_mode == "aligned":
        mask &= context["oi_change"][eval_indices] > 0.0
    elif oi_mode == "divergent":
        mask &= context["oi_change"][eval_indices] < 0.0
    elif oi_mode != "optional":
        raise ValueError(f"unsupported OI mode: {oi_mode}")

    if confirm_mode == "failed_continuation_required":
        if spec["failed_confirm_side"] == "up":
            mask &= context["failed_breakout"][eval_indices]
        elif spec["failed_confirm_side"] == "down":
            mask &= context["failed_breakdown"][eval_indices]
        elif spec["failed_confirm_side"] == "price":
            mask &= ((price_ret > 0.0) & context["failed_breakout"][eval_indices]) | (
                (price_ret < 0.0) & context["failed_breakdown"][eval_indices]
            )
        else:
            raise ValueError(f"unsupported confirmation side: {spec['failed_confirm_side']}")
    elif confirm_mode != "optional":
        raise ValueError(f"unsupported confirmation mode: {confirm_mode}")

    if trend_filter in {"aligned", "countertrend"}:
        long_trade = directions == "long"
        aligned = ((long_trade) & (context["trend"][eval_indices] == "uptrend")) | (
            (~long_trade) & (context["trend"][eval_indices] == "downtrend")
        )
        mask &= aligned if trend_filter == "aligned" else ~aligned
    elif trend_filter != "any":
        raise ValueError(f"unsupported trend filter: {trend_filter}")
    return mask, directions


def _simulate_time_stop(
    context: dict[str, Any],
    indices: np.ndarray,
    *,
    directions: np.ndarray,
    exit_policy: str,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
) -> tuple[dict[str, Any], dict[str, np.ndarray]]:
    time_stop, max_hold = _parse_exit_policy(exit_policy)
    close = context["close"]
    symbols = context["symbols"]
    years = context["years"]
    months = context["months"]
    vol_regime = context["vol_regime"]
    empty_float = np.asarray([], dtype=float)
    empty_label = np.asarray([], dtype=object)

    def empty_result() -> tuple[dict[str, Any], dict[str, np.ndarray]]:
        return {
            "policy": exit_policy,
            "net_bps": None,
            "gross_bps": None,
            "t_stat": None,
            "hit_rate": None,
            "avg_hold_bars": None,
            "cost_survival": None,
            "slippage_plus_10_bps": {"net_bps": None, "t_stat": None, "survives": False},
            "n": 0,
        }, {
            "symbols": empty_label,
            "years": empty_label,
            "months": empty_label,
            "symbol_months": empty_label,
            "vol_regimes": empty_label,
            "net_bps": empty_float,
            "gross_bps": empty_float,
        }

    if len(indices) == 0:
        return empty_result()

    idx = indices.astype(int)
    check_idx = idx + time_stop
    end_idx = idx + max_hold
    in_bounds = end_idx < len(close)
    idx = idx[in_bounds]
    check_idx = check_idx[in_bounds]
    end_idx = end_idx[in_bounds]
    directions = directions[in_bounds]
    if len(idx) == 0:
        return empty_result()
    valid = (
        np.isfinite(close[idx])
        & (close[idx] > 0.0)
        & (symbols[check_idx] == symbols[idx])
        & (symbols[end_idx] == symbols[idx])
    )
    idx = idx[valid]
    check_idx = check_idx[valid]
    end_idx = end_idx[valid]
    if len(idx) == 0:
        return empty_result()

    directions = directions[valid]
    mult = np.where(directions == "long", 1.0, -1.0)
    entry = close[idx]
    check_bps = ((close[check_idx] / entry) - 1.0) * 10000.0 * mult
    exit_idx = np.where(check_bps <= 0.0, check_idx, end_idx)
    gross_returns = ((close[exit_idx] / entry) - 1.0) * 10000.0 * mult
    costs = np.asarray(
        [float(symbol_costs.get(str(symbol), 18.0)) for symbol in symbols[idx]], dtype=float
    )
    returns = gross_returns - costs
    plus_slippage_returns = returns - extra_slippage_bps
    holds = (exit_idx - idx).astype(float)
    net_summary = _return_summary(returns.tolist())
    gross_summary = _return_summary(gross_returns.tolist())
    plus_slippage_summary = _return_summary(plus_slippage_returns.tolist())
    net = net_summary.get("mean_bps")
    gross = gross_summary.get("mean_bps")
    symbol_labels = symbols[idx].astype(object)
    month_labels = months[idx].astype(object)
    details = {
        "symbols": symbol_labels,
        "years": years[idx].astype(object),
        "months": month_labels,
        "symbol_months": np.asarray(
            [
                f"{symbol}:{month}"
                for symbol, month in zip(symbol_labels, month_labels, strict=False)
            ],
            dtype=object,
        ),
        "vol_regimes": vol_regime[idx].astype(object),
        "net_bps": returns,
        "gross_bps": gross_returns,
    }
    return {
        "policy": exit_policy,
        "net_bps": net,
        "gross_bps": gross,
        "t_stat": net_summary.get("t_stat"),
        "hit_rate": float(np.sum(returns > 0.0) / len(returns)) if len(returns) else None,
        "avg_hold_bars": float(np.mean(holds)) if len(holds) else None,
        "cost_survival": float(net / gross)
        if net is not None and gross is not None and gross > 0.0
        else None,
        "slippage_plus_10_bps": {
            "net_bps": plus_slippage_summary.get("mean_bps"),
            "t_stat": plus_slippage_summary.get("t_stat"),
            "survives": (plus_slippage_summary.get("mean_bps") or -(10**9)) > 0.0,
        },
        "n": len(returns),
    }, details


def _evaluate_variant(
    context: dict[str, Any],
    *,
    spec: dict[str, str],
    lookback: int,
    funding_pct: float,
    funding_slope_mode: str,
    oi_mode: str,
    confirm_mode: str,
    trend_filter: str,
    exit_policy: str,
    cooldown_bars: int,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
) -> dict[str, Any]:
    mask, directions = _variant_mask(
        context,
        spec=spec,
        lookback=lookback,
        funding_pct=funding_pct,
        funding_slope_mode=funding_slope_mode,
        oi_mode=oi_mode,
        confirm_mode=confirm_mode,
        trend_filter=trend_filter,
    )
    indices = context["eval_indices"][mask]
    event_directions = directions[mask]
    best_exit, details = _simulate_time_stop(
        context,
        indices,
        directions=event_directions,
        exit_policy=exit_policy,
        symbol_costs=symbol_costs,
        extra_slippage_bps=extra_slippage_bps,
    )
    by_symbol = _group_return_stats_arrays(details["symbols"], details["net_bps"])
    by_year = _group_return_stats_arrays(details["years"], details["net_bps"])
    by_month = _group_return_stats_arrays(details["months"], details["net_bps"])
    by_vol_regime = _group_return_stats_arrays(details["vol_regimes"], details["net_bps"])
    symbol_month_counts = dict(Counter(details["symbol_months"].tolist()))
    month_counts = dict(Counter(details["months"].tolist()))
    symbol_counts = dict(Counter(details["symbols"].tolist()))
    row = {
        "variant_id": (
            f"{spec['variant']}__LB_{lookback}__FUNDING_PCT_{funding_pct:g}__"
            f"SLOPE_{funding_slope_mode.upper()}__OI_{oi_mode.upper()}__"
            f"CONFIRM_{confirm_mode.upper()}__TREND_{trend_filter.upper()}__{exit_policy.upper()}"
        ),
        "family": FAMILY,
        "base_variant": spec["variant"],
        "event_count": int(best_exit["n"]),
        "params": {
            "lookback_bars": lookback,
            "funding_pct": funding_pct,
            "funding_slope_mode": funding_slope_mode,
            "oi_mode": oi_mode,
            "confirm_mode": confirm_mode,
            "trend_filter": trend_filter,
            "cooldown_bars": cooldown_bars,
        },
        "best_exit": best_exit,
        "by_symbol": by_symbol,
        "by_year": by_year,
        "by_month": by_month,
        "by_vol_regime": by_vol_regime,
        "positive_symbols": sorted(
            symbol
            for symbol, stats in by_symbol.items()
            if (stats.get("net_bps") or -(10**9)) > 0.0
        ),
        "top_symbol_month_share": _max_share(symbol_month_counts, len(details["net_bps"])),
        "top_month_event_share": _max_share(month_counts, len(details["net_bps"])),
        "single_symbol_event_share": _max_share(symbol_counts, len(details["net_bps"])),
        "by_month_pnl_concentration": _max_abs_pnl_share_arrays(
            details["months"], details["net_bps"]
        ),
        "walk_forward": {"pass": _walk_forward_pass(by_year), "by_year": by_year},
        "slippage_plus_10_bps": best_exit["slippage_plus_10_bps"],
        "paper_approved": False,
        "live_approved": False,
    }
    row["status"] = _status(row)
    row["score"] = (
        max(0.0, best_exit.get("net_bps") or 0.0)
        + 10.0 * max(0.0, best_exit.get("t_stat") or 0.0)
        + 20.0 * max(0.0, best_exit.get("cost_survival") or 0.0)
        + 5.0 * len(row["positive_symbols"])
    )
    return row


def build_funding_divergence_report(
    *,
    repo_root: Path,
    symbols: list[str],
    years: list[int],
    lookbacks: list[int],
    funding_pcts: list[float],
    exit_policies: list[str],
    cooldown_bars: int,
    extra_slippage_bps: float,
    cost_overrides: dict[str, float],
    json_output: Path,
    csv_output: Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    raw, missing, input_summary = _load_frames(repo_root, symbols, years)
    context = _array_context(raw, candidate_stride_bars=cooldown_bars)
    symbol_costs = {symbol: _cost_for_symbol(symbol, cost_overrides) for symbol in symbols}
    rows: list[dict[str, Any]] = []
    for spec in VARIANT_SPECS:
        for lookback in lookbacks:
            for funding_pct in funding_pcts:
                for slope_mode in ("falling", "rising", "any"):
                    for oi_mode in ("optional", "aligned", "divergent"):
                        for confirm_mode in ("optional", "failed_continuation_required"):
                            for trend_filter in ("any", "aligned", "countertrend"):
                                for exit_policy in exit_policies:
                                    rows.append(  # noqa: PERF401
                                        _evaluate_variant(
                                            context,
                                            spec=spec,
                                            lookback=lookback,
                                            funding_pct=funding_pct,
                                            funding_slope_mode=slope_mode,
                                            oi_mode=oi_mode,
                                            confirm_mode=confirm_mode,
                                            trend_filter=trend_filter,
                                            exit_policy=exit_policy,
                                            cooldown_bars=cooldown_bars,
                                            symbol_costs=symbol_costs,
                                            extra_slippage_bps=extra_slippage_bps,
                                        )
                                    )
    rows.sort(key=lambda item: item["score"], reverse=True)
    top = rows[0] if rows else {}
    report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "scope": {
            "family": FAMILY,
            "symbols_requested": symbols,
            "symbols_evaluated": sorted(raw["symbol"].unique().tolist()),
            "years": years,
            "timeframe": "5m",
            "base_variants": [spec["variant"] for spec in VARIANT_SPECS],
            "lookback_bars": lookbacks,
            "funding_pcts": funding_pcts,
            "funding_slope_modes": ["falling", "rising", "any"],
            "oi_modes": ["optional", "aligned", "divergent"],
            "confirm_modes": ["optional", "failed_continuation_required"],
            "trend_filters": ["any", "aligned", "countertrend"],
            "exit_policies": exit_policies,
            "cooldown_bars": cooldown_bars,
            "candidate_sampling": f"first bar of each {cooldown_bars}-bar symbol block",
            "extra_slippage_bps": extra_slippage_bps,
            "cost_bps_by_symbol": symbol_costs,
            "approval_policy": "research_only_outputs_require_fresh_validation_no_paper_or_live",
            "regime_artifact_guard": "top_symbol_month_share_or_top_month_event_share_above_0.35",
            "new_edge_standard": {
                "event_count_min": 100,
                "net_bps_positive": True,
                "t_stat_min": 2.0,
                "cost_survival_min": 0.8,
                "positive_symbols_min": 3,
                "top_symbol_month_share_max": 0.35,
                "walk_forward_required": True,
                "extra_slippage_bps_survival_required": extra_slippage_bps,
            },
        },
        "input_summary": input_summary,
        "missing_symbol_data": missing,
        "candidate_count": len(rows),
        "status_counts": dict(Counter(row["status"] for row in rows)),
        "top_variants": rows[:50],
        "by_symbol": top.get("by_symbol", {}),
        "by_year": top.get("by_year", {}),
        "by_month": top.get("by_month", {}),
        "top_symbol_month_share": top.get("top_symbol_month_share"),
        "top_month_event_share": top.get("top_month_event_share"),
        "walk_forward": top.get("walk_forward", {}),
        "slippage_plus_10_bps": top.get("slippage_plus_10_bps", {}),
        "paper_approved_events": [],
        "live_approved_events": [],
    }
    csv = pd.DataFrame(
        [
            {
                "variant_id": row["variant_id"],
                "base_variant": row["base_variant"],
                "event_count": row["event_count"],
                "positive_symbols": ",".join(row["positive_symbols"]),
                "net_bps": row["best_exit"].get("net_bps"),
                "gross_bps": row["best_exit"].get("gross_bps"),
                "t_stat": row["best_exit"].get("t_stat"),
                "cost_survival": row["best_exit"].get("cost_survival"),
                "hit_rate": row["best_exit"].get("hit_rate"),
                "avg_hold_bars": row["best_exit"].get("avg_hold_bars"),
                "slippage_plus_10_bps_net_bps": row["slippage_plus_10_bps"].get("net_bps"),
                "slippage_plus_10_bps_t_stat": row["slippage_plus_10_bps"].get("t_stat"),
                "slippage_plus_10_bps_survives": row["slippage_plus_10_bps"].get("survives"),
                "top_symbol_month_share": row["top_symbol_month_share"],
                "top_month_event_share": row["top_month_event_share"],
                "single_symbol_event_share": row["single_symbol_event_share"],
                "by_month_pnl_concentration": row["by_month_pnl_concentration"],
                "walk_forward_pass": row["walk_forward"]["pass"],
                "lookback_bars": row["params"]["lookback_bars"],
                "funding_pct": row["params"]["funding_pct"],
                "funding_slope_mode": row["params"]["funding_slope_mode"],
                "oi_mode": row["params"]["oi_mode"],
                "confirm_mode": row["params"]["confirm_mode"],
                "trend_filter": row["params"]["trend_filter"],
                "exit_policy": row["best_exit"].get("policy"),
                "status": row["status"],
                "score": row["score"],
            }
            for row in rows
        ]
    )
    json_output.parent.mkdir(parents=True, exist_ok=True)
    csv_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8"
    )
    csv.to_csv(csv_output, index=False)
    return report, csv


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Funding divergence detector lab")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in DEFAULT_YEARS))
    parser.add_argument("--lookbacks", default=",".join(str(value) for value in DEFAULT_LOOKBACKS))
    parser.add_argument(
        "--funding-pcts", default=",".join(str(value) for value in DEFAULT_FUNDING_PCTS)
    )
    parser.add_argument("--exit-policies", default=",".join(DEFAULT_EXIT_POLICIES))
    parser.add_argument("--cooldown-bars", type=int, default=DEFAULT_COOLDOWN_BARS)
    parser.add_argument("--extra-slippage-bps", type=float, default=DEFAULT_EXTRA_SLIPPAGE_BPS)
    parser.add_argument("--cost-overrides", default="")
    parser.add_argument(
        "--json-output", default=str(DEFAULT_REPORT_DIR / "funding_divergence_lab_report.json")
    )
    parser.add_argument(
        "--csv-output", default=str(DEFAULT_REPORT_DIR / "top_funding_divergence_variants.csv")
    )
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    report, _ = build_funding_divergence_report(
        repo_root=repo_root,
        symbols=[item.upper() for item in _parse_csv(args.symbols)],
        years=_parse_ints(args.years),
        lookbacks=_parse_ints(args.lookbacks),
        funding_pcts=[float(item) for item in _parse_csv(args.funding_pcts)],
        exit_policies=_parse_csv(args.exit_policies),
        cooldown_bars=args.cooldown_bars,
        extra_slippage_bps=args.extra_slippage_bps,
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
