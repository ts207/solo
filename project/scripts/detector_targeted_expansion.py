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

from project.scripts.detector_exit_lab import _family_robustness
from project.scripts.detector_shadow_report import _prepare_symbol_frame, _return_summary
from project.scripts.detector_tuning_lab import (
    _add_features,
    _base_filter,
    _cooldown_indices_by_symbol,
    _direction_mult,
    _make_variant_id,
    _parse_csv,
    _parse_ints,
    _path_metrics,
)


DEFAULT_FAMILY = "SHORT_BUILD_CONTINUATION_STRICT"
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
DEFAULT_PRICE_THRESHOLDS = (80.0, 90.0, 95.0)
DEFAULT_OI_THRESHOLDS = (95.0, 97.5, 99.0)
DEFAULT_FAILURE_LOOKBACKS = (12, 24, 48, 96)
DEFAULT_HORIZONS = (48, 96)
DEFAULT_COOLDOWNS = (6, 12, 24)
DEFAULT_EXIT_POLICIES = ("time_stop12_max96", "time_stop24_max96")
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


def _safe_mean(values: list[float]) -> float | None:
    clean = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    return float(np.mean(clean)) if clean else None


def _event_indices_for_short_build(
    df: pd.DataFrame,
    *,
    price_pct: float,
    oi_pct: float,
    failure_lookback: int,
    cooldown_bars: int,
) -> np.ndarray:
    base = _base_filter(df, price_pct, oi_pct, lookback=12)
    failed_down = df[f"failed_breakdown_reclaim_{failure_lookback}"]
    mask = (
        base["price_down_oi_up"]
        & ((df["funding_sign"] == "negative") | df["funding_falling"])
        & df["close_near_low"]
        & ~failed_down
    )
    return _cooldown_indices_by_symbol(df, mask, cooldown_bars)


def _simulate_time_stop(
    close: np.ndarray,
    indices: np.ndarray,
    direction: str,
    *,
    time_stop_bars: int,
    max_hold_bars: int,
    cost_bps: float,
) -> dict[str, Any]:
    mult = _direction_mult(direction)
    returns: list[float] = []
    gross_returns: list[float] = []
    holds: list[float] = []
    first_profit_times: list[float] = []
    for idx in indices:
        if idx + 1 >= len(close) or not np.isfinite(close[idx]) or close[idx] <= 0.0:
            continue
        entry = close[idx]
        first_profit: float | None = None
        for step in range(1, min(max_hold_bars, len(close) - idx - 1) + 1):
            step_bps = ((close[idx + step] / entry) - 1.0) * 10000.0 * mult
            if step_bps > 0.0:
                first_profit = float(step)
                break
        check_idx = min(idx + time_stop_bars, len(close) - 1)
        check_bps = ((close[check_idx] / entry) - 1.0) * 10000.0 * mult
        if check_bps <= 0.0 or check_idx >= idx + max_hold_bars:
            exit_bps = check_bps
            hold = check_idx - idx
        else:
            end_idx = min(idx + max_hold_bars, len(close) - 1)
            exit_bps = ((close[end_idx] / entry) - 1.0) * 10000.0 * mult
            hold = end_idx - idx
        gross_returns.append(float(exit_bps))
        returns.append(float(exit_bps - cost_bps))
        holds.append(float(hold))
        if first_profit is not None:
            first_profit_times.append(first_profit)
    summary = _return_summary(returns)
    gross = _return_summary(gross_returns)
    gross_mean = gross.get("mean_bps")
    net_mean = summary.get("mean_bps")
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in returns:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = min(max_drawdown, cumulative - peak)
    return {
        "net_bps": net_mean,
        "gross_bps": gross_mean,
        "t_stat": summary.get("t_stat"),
        "hit_rate": float(sum(1 for value in returns if value > 0.0) / len(returns)) if returns else None,
        "avg_hold_bars": _safe_mean(holds),
        "max_drawdown_bps": float(max_drawdown),
        "cost_survival": float(net_mean / gross_mean) if net_mean is not None and gross_mean is not None and gross_mean > 0.0 else None,
        "time_to_first_profit": _safe_mean(first_profit_times),
        "first_profit_hit_rate": float(len(first_profit_times) / len(returns)) if returns else None,
        "n": len(returns),
    }


def _parse_exit_policy(policy: str) -> tuple[int, int]:
    token = policy.strip().lower()
    if not token.startswith("time_stop") or "_max" not in token:
        raise ValueError(f"unsupported targeted expansion exit policy: {policy}")
    left, right = token.removeprefix("time_stop").split("_max", 1)
    return int(left), int(right)


def _cost_for_symbol(symbol: str, overrides: dict[str, float]) -> float:
    return float(overrides.get(symbol.upper(), DEFAULT_COST_BPS_BY_SYMBOL.get(symbol.upper(), 18.0)))


def _parse_cost_overrides(raw: str | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for item in _parse_csv(raw or ""):
        if ":" not in item:
            continue
        symbol, value = item.split(":", 1)
        out[symbol.strip().upper()] = float(value)
    return out


def _slice_summary(df: pd.DataFrame, indices: np.ndarray) -> dict[str, Any]:
    rows = df.iloc[indices] if len(indices) else df.iloc[[]]
    return {
        "by_symbol": dict(Counter(rows["symbol"])),
        "by_year": dict(Counter(rows["shadow_year"])),
        "by_month_top": dict(Counter(rows["shadow_month"]).most_common(10)),
        "by_vol_regime": dict(Counter(rows["shadow_vol_regime"])),
        "by_trend_regime": dict(Counter(rows["trend_regime"])),
        "by_funding_regime": dict(Counter(rows["funding_sign"])),
        "by_oi_regime": dict(Counter(rows["oi_regime"])),
        "by_session": dict(Counter(rows["session"])),
    }


def _return_stats(values: list[float]) -> dict[str, Any]:
    summary = _return_summary(values)
    return {
        "event_count": int(len(values)),
        "net_bps": summary.get("mean_bps"),
        "t_stat": summary.get("t_stat"),
        "total_net_bps": float(np.sum(values)) if values else 0.0,
    }


def _group_return_stats(event_details: list[dict[str, Any]], key: str) -> dict[str, Any]:
    grouped: dict[str, list[float]] = {}
    for event in event_details:
        label = str(event.get(key, "unknown"))
        value = event.get("net_bps")
        if value is None or not math.isfinite(float(value)):
            continue
        grouped.setdefault(label, []).append(float(value))
    return {label: _return_stats(values) for label, values in sorted(grouped.items())}


def _max_share(counts: dict[str, int], total: int) -> float | None:
    if total <= 0 or not counts:
        return None
    return float(max(counts.values()) / total)


def _max_abs_pnl_share(group_stats: dict[str, Any]) -> float | None:
    totals = [abs(float(row.get("total_net_bps") or 0.0)) for row in group_stats.values()]
    denom = float(sum(totals))
    if denom <= 0.0:
        return None
    return float(max(totals) / denom)


def _month_concentration(slice_summary: dict[str, Any], count: int) -> float | None:
    months = slice_summary.get("by_month_top") or {}
    if count <= 0 or not months:
        return None
    return float(max(months.values()) / count)


def _positive_symbols(symbol_rows: list[dict[str, Any]]) -> list[str]:
    return sorted(
        {
            row["symbol"]
            for row in symbol_rows
            if row.get("event_count", 0) > 0
            and (row.get("best_exit") or {}).get("net_bps") is not None
            and (row.get("best_exit") or {}).get("net_bps") > 0.0
        }
    )


def _positive_symbols_from_stats(by_symbol: dict[str, Any]) -> list[str]:
    return sorted(
        symbol
        for symbol, stats in by_symbol.items()
        if int(stats.get("event_count") or 0) > 0
        and stats.get("net_bps") is not None
        and float(stats["net_bps"]) > 0.0
    )


def _status(row: dict[str, Any], family_stats: dict[str, Any]) -> str:
    best = row.get("best_exit") or {}
    count = int(row.get("event_count") or 0)
    positive_symbols = row.get("positive_symbols") or []
    month_conc = row.get("month_concentration")
    single_symbol_event_share = row.get("single_symbol_event_share")
    single_symbol_pnl_share = row.get("single_symbol_pnl_share")
    by_month_pnl_concentration = row.get("by_month_pnl_concentration")
    if count < 100:
        return "needs_sample_expansion"
    if (best.get("net_bps") or -10**9) <= 0.0:
        return "failed_net"
    if (best.get("t_stat") or -10**9) <= 2.0:
        return "failed_t_stat"
    if (best.get("cost_survival") or -10**9) < 0.8:
        return "failed_cost_survival"
    if len(positive_symbols) < 3:
        return "symbol_scoped_research_only"
    if single_symbol_event_share is not None and single_symbol_event_share > 0.5:
        return "single_symbol_event_dominated_research_only"
    if single_symbol_pnl_share is not None and single_symbol_pnl_share > 0.5:
        return "single_symbol_pnl_dominated_research_only"
    if month_conc is not None and month_conc > 0.35:
        return "month_concentrated_research_only"
    if by_month_pnl_concentration is not None and by_month_pnl_concentration > 0.4:
        return "month_pnl_concentrated_research_only"
    if (family_stats.get("adjacent_positive_rate") or 0.0) < 0.5:
        return "isolated_threshold_research_only"
    if bool(family_stats.get("best_variant_is_isolated")):
        return "isolated_threshold_research_only"
    return "fresh_validation_candidate"


def _load_frames(repo_root: Path, symbols: list[str], years: list[int]) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    frames: dict[str, pd.DataFrame] = {}
    missing: dict[str, Any] = {}
    for symbol in symbols:
        try:
            frame = _add_features(_prepare_symbol_frame(repo_root, symbol, years))
        except Exception as exc:
            missing[symbol] = str(exc)
            continue
        frame["symbol"] = symbol
        frames[symbol] = frame
    return frames, missing


def _evaluate_scope(
    df: pd.DataFrame,
    *,
    symbol_costs: dict[str, float],
    price_pct: float,
    oi_pct: float,
    failure_lookback: int,
    cooldown_bars: int,
    exit_policy: str,
    horizon: int,
) -> dict[str, Any]:
    direction = "short"
    close = pd.to_numeric(df["close"], errors="coerce").to_numpy()
    open_ = pd.to_numeric(df["open"], errors="coerce").to_numpy()
    high = pd.to_numeric(df["high"], errors="coerce").to_numpy()
    low = pd.to_numeric(df["low"], errors="coerce").to_numpy()
    indices = _event_indices_for_short_build(
        df,
        price_pct=price_pct,
        oi_pct=oi_pct,
        failure_lookback=failure_lookback,
        cooldown_bars=cooldown_bars,
    )
    costs = np.asarray([symbol_costs.get(str(df.iloc[idx]["symbol"]), 18.0) for idx in indices], dtype=float)
    # Path diagnostics use the mean cost for aggregate MFE/MAE only; return simulation below applies per-symbol costs.
    path = _path_metrics(close, high, low, indices, direction, horizon, float(np.nanmean(costs)) if len(costs) else 18.0)
    time_stop, max_hold = _parse_exit_policy(exit_policy)
    returns: list[float] = []
    gross_returns: list[float] = []
    holds: list[float] = []
    first_profit_times: list[float] = []
    mae_before_mfe_values: list[float] = []
    gap_against_count = 0
    event_details: list[dict[str, Any]] = []
    mult = _direction_mult(direction)
    for event_number, idx in enumerate(indices):
        if idx + 1 >= len(close) or not np.isfinite(close[idx]) or close[idx] <= 0.0:
            continue
        entry = close[idx]
        cost_bps = float(costs[event_number])
        first_profit: float | None = None
        if idx + 1 < len(open_) and np.isfinite(open_[idx + 1]):
            gap_bps = ((open_[idx + 1] / entry) - 1.0) * 10000.0 * mult
            if gap_bps < 0.0:
                gap_against_count += 1
        fav_path: list[float] = []
        adv_path: list[float] = []
        for step in range(1, min(max_hold, len(close) - idx - 1) + 1):
            step_bps = ((close[idx + step] / entry) - 1.0) * 10000.0 * mult
            if step_bps > 0.0:
                first_profit = float(step)
                break
        for step in range(1, min(max_hold, len(close) - idx - 1) + 1):
            if mult > 0:
                fav_path.append(float((high[idx + step] / entry - 1.0) * 10000.0))
                adv_path.append(float((low[idx + step] / entry - 1.0) * 10000.0))
            else:
                fav_path.append(float((entry / low[idx + step] - 1.0) * 10000.0))
                adv_path.append(float((entry / high[idx + step] - 1.0) * 10000.0))
        if fav_path and adv_path:
            mfe_step = int(np.nanargmax(fav_path))
            adverse_before = adv_path[: mfe_step + 1]
            if adverse_before:
                mae_before_mfe_values.append(float(np.nanmin(adverse_before)))
        check_idx = min(idx + time_stop, len(close) - 1)
        check_bps = ((close[check_idx] / entry) - 1.0) * 10000.0 * mult
        if check_bps <= 0.0 or check_idx >= idx + max_hold:
            exit_bps = check_bps
            hold = check_idx - idx
        else:
            end_idx = min(idx + max_hold, len(close) - 1)
            exit_bps = ((close[end_idx] / entry) - 1.0) * 10000.0 * mult
            hold = end_idx - idx
        gross_returns.append(float(exit_bps))
        returns.append(float(exit_bps - cost_bps))
        holds.append(float(hold))
        if first_profit is not None:
            first_profit_times.append(first_profit)
        state = df.iloc[idx]
        event_details.append(
            {
                "symbol": str(state.get("symbol", "unknown")),
                "year": str(state.get("shadow_year", "unknown")),
                "month": str(state.get("shadow_month", "unknown")),
                "net_bps": float(exit_bps - cost_bps),
                "gross_bps": float(exit_bps),
            }
        )
    net_summary = _return_summary(returns)
    gross_summary = _return_summary(gross_returns)
    net = net_summary.get("mean_bps")
    gross = gross_summary.get("mean_bps")
    best_exit = {
        "policy": exit_policy,
        "net_bps": net,
        "gross_bps": gross,
        "t_stat": net_summary.get("t_stat"),
        "hit_rate": float(sum(1 for value in returns if value > 0.0) / len(returns)) if returns else None,
        "avg_hold_bars": _safe_mean(holds),
        "cost_survival": float(net / gross) if net is not None and gross is not None and gross > 0.0 else None,
        "time_to_first_profit": _safe_mean(first_profit_times),
        "first_profit_hit_rate": float(len(first_profit_times) / len(returns)) if returns else None,
        "mae_before_mfe_bps": _safe_mean(mae_before_mfe_values),
        "gap_against_entry_rate": float(gap_against_count / len(returns)) if returns else None,
        "n": len(returns),
    }
    slices = _slice_summary(df, indices)
    by_symbol = _group_return_stats(event_details, "symbol")
    by_year = _group_return_stats(event_details, "year")
    by_month = _group_return_stats(event_details, "month")
    single_symbol_event_share = _max_share({k: int(v) for k, v in slices.get("by_symbol", {}).items()}, int(len(indices)))
    single_symbol_pnl_share = _max_abs_pnl_share(by_symbol)
    by_month_pnl_concentration = _max_abs_pnl_share(by_month)
    return {
        "variant_id": _make_variant_id([DEFAULT_FAMILY, price_pct, oi_pct, failure_lookback, exit_policy, "H", horizon, "CD", cooldown_bars]),
        "base_variant_family": DEFAULT_FAMILY,
        "family": "oi_expansion",
        "direction": direction,
        "event_count": int(len(indices)),
        "params": {
            "price_pct": price_pct,
            "oi_pct": oi_pct,
            "failure_lookback": failure_lookback,
            "exit_policy": exit_policy,
            "horizon_bars": horizon,
            "cooldown_bars": cooldown_bars,
        },
        "best_horizon_bars": horizon,
        "best_path": {
            "mfe_bps": path.get("max_favorable_bps"),
            "mae_bps": path.get("max_adverse_bps"),
            "edge_ratio": path.get("edge_ratio"),
            "time_to_mfe": path.get("time_to_mfe"),
            "time_to_mae": path.get("time_to_mae"),
            "mfe_hit_rate_after_cost": path.get("mfe_hit_rate_after_cost"),
            "mae_exceeds_cost_rate": path.get("mae_exceeds_cost_rate"),
        },
        "best_exit": best_exit,
        "regime_slices": slices,
        "month_concentration": _month_concentration(slices, int(len(indices))),
        "by_symbol_net_bps": by_symbol,
        "by_year_net_bps": by_year,
        "by_month_net_bps": by_month,
        "single_symbol_event_share": single_symbol_event_share,
        "single_symbol_pnl_share": single_symbol_pnl_share,
        "by_month_pnl_concentration": by_month_pnl_concentration,
        "symbol_scope": sorted(set(df["symbol"].astype(str))),
        "paper_approved": False,
        "live_approved": False,
    }


def build_targeted_expansion_report(
    *,
    repo_root: Path,
    family: str,
    symbols: list[str],
    years: list[int],
    price_thresholds: list[float],
    oi_thresholds: list[float],
    failure_lookbacks: list[int],
    horizons: list[int],
    cooldowns: list[int],
    exit_policies: list[str],
    cost_overrides: dict[str, float],
    json_output: Path,
    csv_output: Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    if family != DEFAULT_FAMILY:
        raise ValueError(f"targeted expansion currently supports {DEFAULT_FAMILY}; got {family}")
    frames_by_symbol, missing = _load_frames(repo_root, symbols, years)
    symbol_costs = {symbol: _cost_for_symbol(symbol, cost_overrides) for symbol in symbols}
    rows: list[dict[str, Any]] = []
    if frames_by_symbol:
        pooled = pd.concat(frames_by_symbol.values(), ignore_index=True).sort_values(["symbol", "timestamp"]).reset_index(drop=True)
        for price_pct in price_thresholds:
            for oi_pct in oi_thresholds:
                for failure_lookback in failure_lookbacks:
                    for cooldown in cooldowns:
                        for exit_policy in exit_policies:
                            for horizon in horizons:
                                pooled_row = _evaluate_scope(
                                    pooled,
                                    symbol_costs=symbol_costs,
                                    price_pct=price_pct,
                                    oi_pct=oi_pct,
                                    failure_lookback=failure_lookback,
                                    cooldown_bars=cooldown,
                                    exit_policy=exit_policy,
                                    horizon=horizon,
                                )
                                pooled_row["positive_symbols"] = _positive_symbols_from_stats(
                                    pooled_row.get("by_symbol_net_bps", {})
                                )
                                rows.append(pooled_row)
    family_stats = _family_robustness(rows)
    family_stat = family_stats.get(DEFAULT_FAMILY, {})
    for row in rows:
        row["status"] = _status(row, family_stat)
        best = row.get("best_exit") or {}
        row["score"] = (
            max(0.0, best.get("net_bps") or 0.0)
            + 10.0 * max(0.0, best.get("t_stat") or 0.0)
            + 25.0 * max(0.0, best.get("cost_survival") or 0.0)
            + 6.0 * max(0.0, row.get("best_path", {}).get("edge_ratio") or 0.0)
            + 5.0 * len(row.get("positive_symbols") or [])
        )
    rows.sort(key=lambda item: item.get("score", 0.0), reverse=True)
    status_counts = dict(Counter(row.get("status", "unknown") for row in rows))
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "family": family,
            "symbols_requested": symbols,
            "symbols_evaluated": sorted(frames_by_symbol),
            "years": years,
            "timeframe": "5m",
            "price_thresholds": price_thresholds,
            "oi_thresholds": oi_thresholds,
            "failure_lookbacks": failure_lookbacks,
            "horizons": horizons,
            "cooldowns": cooldowns,
            "exit_policies": exit_policies,
            "cost_bps_by_symbol": symbol_costs,
            "approval_policy": "research_only_outputs_require_fresh_validation",
        },
        "missing_symbol_data": missing,
        "candidate_count": len(rows),
        "status_counts": status_counts,
        "family_robustness": family_stats,
        "top_variants": rows[:50],
        "paper_approved_events": [],
        "live_approved_events": [],
    }
    csv_rows = []
    for row in rows:
        best = row.get("best_exit") or {}
        path = row.get("best_path") or {}
        csv_rows.append(
            {
                "variant_id": row["variant_id"],
                "event_count": row["event_count"],
                "positive_symbols": ",".join(row.get("positive_symbols") or []),
                "symbol_scope": ",".join(row.get("symbol_scope") or []),
                "by_symbol_event_count": json.dumps(
                    {symbol: stats.get("event_count") for symbol, stats in row.get("by_symbol_net_bps", {}).items()},
                    sort_keys=True,
                ),
                "by_symbol_net_bps": json.dumps(
                    {symbol: stats.get("net_bps") for symbol, stats in row.get("by_symbol_net_bps", {}).items()},
                    sort_keys=True,
                ),
                "by_symbol_t_stat": json.dumps(
                    {symbol: stats.get("t_stat") for symbol, stats in row.get("by_symbol_net_bps", {}).items()},
                    sort_keys=True,
                ),
                "by_year_net_bps": json.dumps(
                    {year: stats.get("net_bps") for year, stats in row.get("by_year_net_bps", {}).items()},
                    sort_keys=True,
                ),
                "price_pct": row["params"]["price_pct"],
                "oi_pct": row["params"]["oi_pct"],
                "failure_lookback": row["params"]["failure_lookback"],
                "cooldown_bars": row["params"]["cooldown_bars"],
                "exit_policy": best.get("policy"),
                "horizon_bars": row["params"]["horizon_bars"],
                "net_bps": best.get("net_bps"),
                "gross_bps": best.get("gross_bps"),
                "t_stat": best.get("t_stat"),
                "hit_rate": best.get("hit_rate"),
                "avg_hold_bars": best.get("avg_hold_bars"),
                "cost_survival": best.get("cost_survival"),
                "mfe_bps": path.get("mfe_bps"),
                "mae_bps": path.get("mae_bps"),
                "edge_ratio": path.get("edge_ratio"),
                "time_to_first_profit": best.get("time_to_first_profit"),
                "first_profit_hit_rate": best.get("first_profit_hit_rate"),
                "mae_before_mfe_bps": best.get("mae_before_mfe_bps"),
                "gap_against_entry_rate": best.get("gap_against_entry_rate"),
                "month_concentration": row.get("month_concentration"),
                "by_month_pnl_concentration": row.get("by_month_pnl_concentration"),
                "single_symbol_event_share": row.get("single_symbol_event_share"),
                "single_symbol_pnl_share": row.get("single_symbol_pnl_share"),
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
    parser = argparse.ArgumentParser(description="Targeted local-neighborhood expansion for a detector lead family")
    parser.add_argument("--family", default=DEFAULT_FAMILY)
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in DEFAULT_YEARS))
    parser.add_argument("--price-thresholds", default=",".join(str(value) for value in DEFAULT_PRICE_THRESHOLDS))
    parser.add_argument("--oi-thresholds", default=",".join(str(value) for value in DEFAULT_OI_THRESHOLDS))
    parser.add_argument("--failure-lookbacks", default=",".join(str(value) for value in DEFAULT_FAILURE_LOOKBACKS))
    parser.add_argument("--horizons", default=",".join(str(value) for value in DEFAULT_HORIZONS))
    parser.add_argument("--cooldowns", default=",".join(str(value) for value in DEFAULT_COOLDOWNS))
    parser.add_argument("--exit-policies", default=",".join(DEFAULT_EXIT_POLICIES))
    parser.add_argument("--cost-overrides", default="")
    parser.add_argument("--json-output", default=str(DEFAULT_REPORT_DIR / "targeted_expansion_short_build.json"))
    parser.add_argument("--csv-output", default=str(DEFAULT_REPORT_DIR / "targeted_expansion_short_build.csv"))
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[2]
    symbols = [item.upper() for item in _parse_csv(args.symbols)]
    years = _parse_ints(args.years)
    report, _ = build_targeted_expansion_report(
        repo_root=repo_root,
        family=str(args.family).strip().upper(),
        symbols=symbols,
        years=years,
        price_thresholds=[float(item) for item in _parse_csv(args.price_thresholds)],
        oi_thresholds=[float(item) for item in _parse_csv(args.oi_thresholds)],
        failure_lookbacks=_parse_ints(args.failure_lookbacks),
        horizons=_parse_ints(args.horizons),
        cooldowns=_parse_ints(args.cooldowns),
        exit_policies=_parse_csv(args.exit_policies),
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
