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
    _group_return_stats,
    _max_abs_pnl_share,
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
DEFAULT_COMPRESSION_PCTS = (10.0, 20.0)
DEFAULT_RANGE_PCTS = (10.0, 20.0)
DEFAULT_BREAKOUT_BUFFER_BPS = (0.0, 5.0, 10.0)
DEFAULT_EXIT_POLICIES = ("time_stop12_max48", "time_stop24_max96")
DEFAULT_COOLDOWN_BARS = 12
DEFAULT_EXTRA_SLIPPAGE_BPS = 10.0
FAMILY = "VOL_COMPRESSION_BREAKOUT"


VARIANT_SPECS = (
    {
        "variant": "COMPRESSION_UP_BREAKOUT_CONTINUATION",
        "direction": "long",
        "breakout_side": "up",
        "shape": "continuation",
    },
    {
        "variant": "COMPRESSION_DOWN_BREAKOUT_CONTINUATION",
        "direction": "short",
        "breakout_side": "down",
        "shape": "continuation",
    },
    {
        "variant": "COMPRESSION_UP_FAKEOUT_REVERSAL",
        "direction": "short",
        "breakout_side": "up",
        "shape": "fakeout_reversal",
    },
    {
        "variant": "COMPRESSION_DOWN_FAKEOUT_REVERSAL",
        "direction": "long",
        "breakout_side": "down",
        "shape": "fakeout_reversal",
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


def _add_vol_compression_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values(["symbol", "timestamp"]).reset_index(drop=True).copy()
    close = pd.to_numeric(out["close"], errors="coerce")
    high = pd.to_numeric(out["high"], errors="coerce")
    low = pd.to_numeric(out["low"], errors="coerce")
    volume = pd.to_numeric(out.get("volume", 0.0), errors="coerce").fillna(0.0)
    oi = pd.to_numeric(out.get("oi_notional", np.nan), errors="coerce")
    funding = pd.to_numeric(out.get("funding_rate_scaled", 0.0), errors="coerce").fillna(0.0)
    grouped = out.groupby("symbol", sort=False)

    ret = grouped["close"].pct_change()
    rv_96 = ret.groupby(out["symbol"], sort=False).transform(
        lambda series: (
            pd.to_numeric(series, errors="coerce").rolling(96, min_periods=24).std() * math.sqrt(96)
        )
    )
    bar_range = ((high - low) / close.replace(0.0, np.nan)).abs()
    range_96 = bar_range.groupby(out["symbol"], sort=False).transform(
        lambda series: pd.to_numeric(series, errors="coerce").rolling(96, min_periods=24).mean()
    )
    prev_close = grouped["close"].shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr_96 = (
        (true_range / close.replace(0.0, np.nan))
        .groupby(out["symbol"], sort=False)
        .transform(
            lambda series: pd.to_numeric(series, errors="coerce").rolling(96, min_periods=24).mean()
        )
    )

    out["rv_percentile_96"] = rv_96.groupby(out["symbol"], sort=False).transform(
        lambda series: _rolling_pct_rank(series, window=2880, min_periods=288)
    )
    out["range_percentile_96"] = range_96.groupby(out["symbol"], sort=False).transform(
        lambda series: _rolling_pct_rank(series, window=2880, min_periods=288)
    )
    out["atr_percentile_96"] = atr_96.groupby(out["symbol"], sort=False).transform(
        lambda series: _rolling_pct_rank(series, window=2880, min_periods=288)
    )
    out["rv_percentile_96"] = grouped["rv_percentile_96"].shift(1)
    out["range_percentile_96"] = grouped["range_percentile_96"].shift(1)
    out["atr_percentile_96"] = grouped["atr_percentile_96"].shift(1)
    out["donchian_high_96"] = grouped["high"].transform(
        lambda series: (
            pd.to_numeric(series, errors="coerce").shift(1).rolling(96, min_periods=48).max()
        )
    )
    out["donchian_low_96"] = grouped["low"].transform(
        lambda series: (
            pd.to_numeric(series, errors="coerce").shift(1).rolling(96, min_periods=48).min()
        )
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
    out["oi_change_12"] = grouped["oi_notional"].transform(
        lambda series: np.log(pd.to_numeric(series, errors="coerce").replace(0.0, np.nan)).diff(12)
    )
    out["funding_sign"] = np.select(
        [funding > 0.0, funding < 0.0], ["positive", "negative"], default="zero"
    )

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
    out["shadow_year"] = out["timestamp"].dt.year.astype(str)
    out["shadow_month"] = out["timestamp"].dt.strftime("%Y-%m")
    out["close"] = close
    out["high"] = high
    out["low"] = low
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
    return _add_vol_compression_features(pooled), missing, summary


def _cooldown_indices_by_symbol(
    df: pd.DataFrame, mask: pd.Series, cooldown_bars: int
) -> np.ndarray:
    kept: list[int] = []
    masked = mask.fillna(False)
    for _, group in df[masked].groupby("symbol", sort=False):
        last = -(10**9)
        for idx in group.index.to_numpy(dtype=int):
            if int(idx) - last >= cooldown_bars:
                kept.append(int(idx))
                last = int(idx)
    return np.asarray(sorted(kept), dtype=int)


def _array_context(df: pd.DataFrame) -> dict[str, Any]:
    symbols = df["symbol"].astype(str).to_numpy()
    boundaries = np.flatnonzero(symbols[1:] != symbols[:-1]) + 1
    starts = np.r_[0, boundaries]
    ends = np.r_[boundaries, len(symbols)]
    return {
        "close": pd.to_numeric(df["close"], errors="coerce").to_numpy(dtype=float),
        "high": pd.to_numeric(df["high"], errors="coerce").to_numpy(dtype=float),
        "low": pd.to_numeric(df["low"], errors="coerce").to_numpy(dtype=float),
        "donchian_high": pd.to_numeric(df["donchian_high_96"], errors="coerce").to_numpy(
            dtype=float
        ),
        "donchian_low": pd.to_numeric(df["donchian_low_96"], errors="coerce").to_numpy(dtype=float),
        "rv_pct": pd.to_numeric(df["rv_percentile_96"], errors="coerce").to_numpy(dtype=float),
        "range_pct": pd.to_numeric(df["range_percentile_96"], errors="coerce").to_numpy(
            dtype=float
        ),
        "atr_pct": pd.to_numeric(df["atr_percentile_96"], errors="coerce").to_numpy(dtype=float),
        "volume_z": pd.to_numeric(df["volume_z"], errors="coerce")
        .fillna(0.0)
        .to_numpy(dtype=float),
        "oi_change": pd.to_numeric(df["oi_change_12"], errors="coerce")
        .fillna(0.0)
        .to_numpy(dtype=float),
        "trend": df["trend_regime"].astype(str).to_numpy(),
        "symbols": symbols,
        "years": df["shadow_year"].astype(str).to_numpy(),
        "months": df["shadow_month"].astype(str).to_numpy(),
        "symbol_slices": list(zip(starts.astype(int), ends.astype(int), strict=False)),
    }


def _cooldown_indices_from_mask(
    mask: np.ndarray, symbol_slices: list[tuple[int, int]], cooldown_bars: int
) -> np.ndarray:
    kept: list[int] = []
    for start, end in symbol_slices:
        raw = np.flatnonzero(mask[start:end]) + start
        last = -(10**9)
        for idx in raw:
            if int(idx) - last >= cooldown_bars:
                kept.append(int(idx))
                last = int(idx)
    return np.asarray(kept, dtype=int)


def _walk_forward_pass(by_year: dict[str, Any]) -> bool:
    years = sorted(by_year)
    if len(years) < 2:
        return False
    oos_years = years[1:]
    if len(oos_years) < 2:
        return all((by_year[year].get("net_bps") or -(10**9)) > 0.0 for year in oos_years)
    positive = sum(1 for year in oos_years if (by_year[year].get("net_bps") or -(10**9)) > 0.0)
    return positive >= max(2, math.ceil(0.67 * len(oos_years)))


def _status(row: dict[str, Any]) -> str:
    best = row.get("best_exit") or {}
    if int(row.get("event_count") or 0) < 100:
        return "needs_sample_expansion"
    if (best.get("net_bps") or -(10**9)) <= 0.0:
        return "failed_net"
    if (best.get("t_stat") or -(10**9)) <= 2.0:
        return "failed_t_stat"
    if (best.get("cost_survival") or -(10**9)) < 0.8:
        return "failed_cost_survival"
    if len(row.get("positive_symbols") or []) < 3:
        return "symbol_scoped_research_only"
    if (row.get("top_symbol_month_share") or 0.0) > 0.35:
        return "symbol_month_concentrated_research_only"
    if not bool(row.get("walk_forward_pass")):
        return "walk_forward_failed"
    if not bool((best.get("slippage_plus_10_bps") or {}).get("survives")):
        return "failed_plus_10_bps_slippage"
    return "fresh_validation_candidate"


def _variant_mask(
    context: dict[str, Any],
    *,
    spec: dict[str, str],
    compression_pct: float,
    range_pct: float,
    breakout_buffer_bps: float,
    volume_mode: str,
    oi_mode: str,
    trend_filter: str,
) -> np.ndarray:
    close = context["close"]
    high = context["high"]
    low = context["low"]
    donchian_high = context["donchian_high"]
    donchian_low = context["donchian_low"]
    buffer = float(breakout_buffer_bps) / 10000.0
    compressed = (
        (context["rv_pct"] <= compression_pct)
        & (context["range_pct"] <= range_pct)
        & (context["atr_pct"] <= range_pct)
    )
    if spec["shape"] == "continuation" and spec["breakout_side"] == "up":
        trigger = close > donchian_high * (1.0 + buffer)
    elif spec["shape"] == "continuation" and spec["breakout_side"] == "down":
        trigger = close < donchian_low * (1.0 - buffer)
    elif spec["shape"] == "fakeout_reversal" and spec["breakout_side"] == "up":
        trigger = (high > donchian_high * (1.0 + buffer)) & (close < donchian_high)
    elif spec["shape"] == "fakeout_reversal" and spec["breakout_side"] == "down":
        trigger = (low < donchian_low * (1.0 - buffer)) & (close > donchian_low)
    else:
        raise ValueError(f"unsupported variant spec: {spec}")

    mask = compressed & trigger
    if volume_mode == "required":
        mask &= context["volume_z"] >= 1.0
    elif volume_mode != "optional":
        raise ValueError(f"unsupported volume mode: {volume_mode}")
    if oi_mode == "aligned":
        mask &= context["oi_change"] > 0.0
    elif oi_mode != "optional":
        raise ValueError(f"unsupported OI mode: {oi_mode}")
    if trend_filter == "aligned":
        wanted = "uptrend" if spec["direction"] == "long" else "downtrend"
        mask &= context["trend"] == wanted
    elif trend_filter != "any":
        raise ValueError(f"unsupported trend filter: {trend_filter}")
    return mask & np.isfinite(close) & np.isfinite(donchian_high) & np.isfinite(donchian_low)


def _simulate_time_stop(
    context: dict[str, Any],
    indices: np.ndarray,
    *,
    direction: str,
    exit_policy: str,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    time_stop, max_hold = _parse_exit_policy(exit_policy)
    close = context["close"]
    symbols = context["symbols"]
    years = context["years"]
    months = context["months"]
    mult = _direction_mult(direction)
    returns: list[float] = []
    gross_returns: list[float] = []
    plus_slippage_returns: list[float] = []
    holds: list[float] = []
    event_details: list[dict[str, Any]] = []
    for idx in indices:
        if idx + time_stop >= len(close) or not np.isfinite(close[idx]) or close[idx] <= 0.0:
            continue
        symbol = str(symbols[idx])
        if idx + 1 < len(symbols) and symbols[idx + time_stop] != symbol:
            continue
        entry = close[idx]
        check_idx = min(idx + time_stop, len(close) - 1)
        end_idx = min(idx + max_hold, len(close) - 1)
        if symbols[check_idx] != symbol or symbols[end_idx] != symbol:
            continue
        check_bps = ((close[check_idx] / entry) - 1.0) * 10000.0 * mult
        exit_idx = check_idx if check_bps <= 0.0 or check_idx >= end_idx else end_idx
        gross_bps = ((close[exit_idx] / entry) - 1.0) * 10000.0 * mult
        cost_bps = float(symbol_costs.get(symbol, 18.0))
        net_bps = gross_bps - cost_bps
        plus_slippage_bps = net_bps - extra_slippage_bps
        returns.append(net_bps)
        gross_returns.append(gross_bps)
        plus_slippage_returns.append(plus_slippage_bps)
        holds.append(float(exit_idx - idx))
        event_details.append(
            {
                "symbol": symbol,
                "year": str(years[idx]),
                "month": str(months[idx]),
                "symbol_month": f"{symbol}:{months[idx]}",
                "net_bps": net_bps,
                "gross_bps": gross_bps,
            }
        )
    net_summary = _return_summary(returns)
    gross_summary = _return_summary(gross_returns)
    plus_slippage_summary = _return_summary(plus_slippage_returns)
    net = net_summary.get("mean_bps")
    gross = gross_summary.get("mean_bps")
    return {
        "policy": exit_policy,
        "net_bps": net,
        "gross_bps": gross,
        "t_stat": net_summary.get("t_stat"),
        "hit_rate": float(sum(1 for value in returns if value > 0.0) / len(returns))
        if returns
        else None,
        "avg_hold_bars": float(np.mean(holds)) if holds else None,
        "cost_survival": float(net / gross)
        if net is not None and gross is not None and gross > 0.0
        else None,
        "slippage_plus_10_bps": {
            "net_bps": plus_slippage_summary.get("mean_bps"),
            "t_stat": plus_slippage_summary.get("t_stat"),
            "survives": (plus_slippage_summary.get("mean_bps") or -(10**9)) > 0.0,
        },
        "n": len(returns),
    }, event_details


def _evaluate_variant(
    context: dict[str, Any],
    *,
    spec: dict[str, str],
    compression_pct: float,
    range_pct: float,
    breakout_buffer_bps: float,
    volume_mode: str,
    oi_mode: str,
    trend_filter: str,
    exit_policy: str,
    cooldown_bars: int,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
) -> dict[str, Any]:
    mask = _variant_mask(
        context,
        spec=spec,
        compression_pct=compression_pct,
        range_pct=range_pct,
        breakout_buffer_bps=breakout_buffer_bps,
        volume_mode=volume_mode,
        oi_mode=oi_mode,
        trend_filter=trend_filter,
    )
    indices = _cooldown_indices_from_mask(mask, context["symbol_slices"], cooldown_bars)
    best_exit, event_details = _simulate_time_stop(
        context,
        indices,
        direction=spec["direction"],
        exit_policy=exit_policy,
        symbol_costs=symbol_costs,
        extra_slippage_bps=extra_slippage_bps,
    )
    by_symbol = _group_return_stats(event_details, "symbol")
    by_year = _group_return_stats(event_details, "year")
    by_month = _group_return_stats(event_details, "month")
    symbol_month_counts = dict(Counter(event["symbol_month"] for event in event_details))
    symbol_counts = dict(Counter(event["symbol"] for event in event_details))
    row = {
        "variant_id": (
            f"{spec['variant']}__COMP_{compression_pct:g}__RANGE_{range_pct:g}__"
            f"BUFFER_{breakout_buffer_bps:g}BPS__VOL_{volume_mode.upper()}__"
            f"OI_{oi_mode.upper()}__TREND_{trend_filter.upper()}__{exit_policy.upper()}"
        ),
        "family": FAMILY,
        "base_variant": spec["variant"],
        "direction": spec["direction"],
        "event_count": int(best_exit["n"]),
        "params": {
            "compression_pct": compression_pct,
            "range_pct": range_pct,
            "breakout_buffer_bps": breakout_buffer_bps,
            "volume_mode": volume_mode,
            "oi_mode": oi_mode,
            "trend_filter": trend_filter,
            "cooldown_bars": cooldown_bars,
        },
        "best_exit": best_exit,
        "by_symbol": by_symbol,
        "by_year": by_year,
        "by_month": by_month,
        "positive_symbols": sorted(
            symbol
            for symbol, stats in by_symbol.items()
            if (stats.get("net_bps") or -(10**9)) > 0.0
        ),
        "top_symbol_month_share": _max_share(symbol_month_counts, len(event_details)),
        "single_symbol_event_share": _max_share(symbol_counts, len(event_details)),
        "by_month_pnl_concentration": _max_abs_pnl_share(by_month),
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


def build_vol_compression_report(
    *,
    repo_root: Path,
    symbols: list[str],
    years: list[int],
    compression_pcts: list[float],
    range_pcts: list[float],
    breakout_buffer_bps: list[float],
    exit_policies: list[str],
    cooldown_bars: int,
    extra_slippage_bps: float,
    cost_overrides: dict[str, float],
    json_output: Path,
    csv_output: Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    raw, missing, input_summary = _load_frames(repo_root, symbols, years)
    context = _array_context(raw)
    symbol_costs = {symbol: _cost_for_symbol(symbol, cost_overrides) for symbol in symbols}
    rows: list[dict[str, Any]] = []
    for spec in VARIANT_SPECS:
        for compression_pct in compression_pcts:
            for range_pct in range_pcts:
                for buffer_bps in breakout_buffer_bps:
                    for volume_mode in ("optional", "required"):
                        for oi_mode in ("optional", "aligned"):
                            for trend_filter in ("any", "aligned"):
                                for exit_policy in exit_policies:
                                    rows.append(  # noqa: PERF401
                                        _evaluate_variant(
                                            context,
                                            spec=spec,
                                            compression_pct=compression_pct,
                                            range_pct=range_pct,
                                            breakout_buffer_bps=buffer_bps,
                                            volume_mode=volume_mode,
                                            oi_mode=oi_mode,
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
            "compression_pcts": compression_pcts,
            "range_pcts": range_pcts,
            "breakout_buffer_bps": breakout_buffer_bps,
            "volume_modes": ["optional", "required"],
            "oi_modes": ["optional", "aligned"],
            "trend_filters": ["any", "aligned"],
            "exit_policies": exit_policies,
            "cooldown_bars": cooldown_bars,
            "extra_slippage_bps": extra_slippage_bps,
            "cost_bps_by_symbol": symbol_costs,
            "approval_policy": "research_only_outputs_require_fresh_validation_no_paper_or_live",
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
        "walk_forward": top.get("walk_forward", {}),
        "slippage_plus_10_bps": top.get("slippage_plus_10_bps", {}),
        "paper_approved_events": [],
        "live_approved_events": [],
    }
    csv_rows = []
    for row in rows:
        best = row["best_exit"]
        csv_rows.append(
            {
                "variant_id": row["variant_id"],
                "base_variant": row["base_variant"],
                "direction": row["direction"],
                "event_count": row["event_count"],
                "positive_symbols": ",".join(row["positive_symbols"]),
                "net_bps": best.get("net_bps"),
                "gross_bps": best.get("gross_bps"),
                "t_stat": best.get("t_stat"),
                "cost_survival": best.get("cost_survival"),
                "hit_rate": best.get("hit_rate"),
                "avg_hold_bars": best.get("avg_hold_bars"),
                "slippage_plus_10_bps_net_bps": row["slippage_plus_10_bps"].get("net_bps"),
                "slippage_plus_10_bps_t_stat": row["slippage_plus_10_bps"].get("t_stat"),
                "slippage_plus_10_bps_survives": row["slippage_plus_10_bps"].get("survives"),
                "top_symbol_month_share": row["top_symbol_month_share"],
                "single_symbol_event_share": row["single_symbol_event_share"],
                "by_month_pnl_concentration": row["by_month_pnl_concentration"],
                "walk_forward_pass": row["walk_forward"]["pass"],
                "compression_pct": row["params"]["compression_pct"],
                "range_pct": row["params"]["range_pct"],
                "breakout_buffer_bps": row["params"]["breakout_buffer_bps"],
                "volume_mode": row["params"]["volume_mode"],
                "oi_mode": row["params"]["oi_mode"],
                "trend_filter": row["params"]["trend_filter"],
                "exit_policy": best.get("policy"),
                "status": row["status"],
                "score": row["score"],
            }
        )
    csv = pd.DataFrame(csv_rows)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    csv_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8"
    )
    csv.to_csv(csv_output, index=False)
    return report, csv


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Volatility compression breakout detector lab")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in DEFAULT_YEARS))
    parser.add_argument(
        "--compression-pcts", default=",".join(str(value) for value in DEFAULT_COMPRESSION_PCTS)
    )
    parser.add_argument(
        "--range-pcts", default=",".join(str(value) for value in DEFAULT_RANGE_PCTS)
    )
    parser.add_argument(
        "--breakout-buffer-bps",
        default=",".join(str(value) for value in DEFAULT_BREAKOUT_BUFFER_BPS),
    )
    parser.add_argument("--exit-policies", default=",".join(DEFAULT_EXIT_POLICIES))
    parser.add_argument("--cooldown-bars", type=int, default=DEFAULT_COOLDOWN_BARS)
    parser.add_argument("--extra-slippage-bps", type=float, default=DEFAULT_EXTRA_SLIPPAGE_BPS)
    parser.add_argument("--cost-overrides", default="")
    parser.add_argument(
        "--json-output", default=str(DEFAULT_REPORT_DIR / "vol_compression_lab_report.json")
    )
    parser.add_argument(
        "--csv-output", default=str(DEFAULT_REPORT_DIR / "top_vol_compression_variants.csv")
    )
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    report, _ = build_vol_compression_report(
        repo_root=repo_root,
        symbols=[item.upper() for item in _parse_csv(args.symbols)],
        years=_parse_ints(args.years),
        compression_pcts=[float(item) for item in _parse_csv(args.compression_pcts)],
        range_pcts=[float(item) for item in _parse_csv(args.range_pcts)],
        breakout_buffer_bps=[float(item) for item in _parse_csv(args.breakout_buffer_bps)],
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
