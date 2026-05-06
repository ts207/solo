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

from project.scripts.detector_shadow_report import _prepare_symbol_frame, _return_summary, _rolling_pct_rank
from project.scripts.detector_targeted_expansion import (
    DEFAULT_COST_BPS_BY_SYMBOL,
    DEFAULT_SYMBOLS,
    _group_return_stats,
    _max_abs_pnl_share,
    _max_share,
    _parse_cost_overrides,
    _parse_exit_policy,
    _safe_mean,
)
from project.scripts.detector_tuning_lab import _add_features, _direction_mult, _parse_csv, _parse_ints, _path_metrics


DEFAULT_YEARS = (2022, 2023, 2024, 2025)
DEFAULT_REPORT_DIR = Path("data/reports/detectors/no_liquidations_v1")
DEFAULT_EXIT_POLICIES = ("time_stop12_max96", "time_stop24_max96")
DEFAULT_HORIZONS = (48, 96)


def _cost_for_symbol(symbol: str, overrides: dict[str, float]) -> float:
    return float(overrides.get(symbol.upper(), DEFAULT_COST_BPS_BY_SYMBOL.get(symbol.upper(), 18.0)))


def _load_frames(repo_root: Path, symbols: list[str], years: list[int]) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    frames = []
    missing: dict[str, Any] = {}
    summary: dict[str, Any] = {}
    for symbol in symbols:
        try:
            frame = _add_features(_prepare_symbol_frame(repo_root, symbol, years))
        except Exception as exc:
            missing[symbol] = str(exc)
            continue
        frame["symbol"] = symbol
        frames.append(frame)
        summary[symbol] = {
            "rows": int(len(frame)),
            "start": str(frame["timestamp"].min()),
            "end": str(frame["timestamp"].max()),
            "years": sorted(frame["shadow_year"].dropna().unique().tolist()),
        }
    if not frames:
        raise RuntimeError("no symbols had complete OHLCV/OI/funding data")
    return pd.concat(frames, ignore_index=True).sort_values(["symbol", "timestamp"]).reset_index(drop=True), missing, summary


def _add_1h_setups(df: pd.DataFrame) -> pd.DataFrame:
    out_frames = []
    for symbol, group in df.groupby("symbol", sort=False):
        g = group.sort_values("timestamp").set_index("timestamp")
        hourly = pd.DataFrame(
            {
                "open": g["open"].resample("1h", label="right", closed="right").first(),
                "high": g["high"].resample("1h", label="right", closed="right").max(),
                "low": g["low"].resample("1h", label="right", closed="right").min(),
                "close": g["close"].resample("1h", label="right", closed="right").last(),
                "volume": g["volume"].resample("1h", label="right", closed="right").sum(),
                "oi_notional": g["oi_notional"].resample("1h", label="right", closed="right").last(),
                "funding_rate_scaled": g["funding_rate_scaled"].resample("1h", label="right", closed="right").last(),
            }
        ).dropna(subset=["close", "oi_notional"])
        hourly = hourly.reset_index()
        hourly["symbol"] = symbol
        close = pd.to_numeric(hourly["close"], errors="coerce")
        oi = pd.to_numeric(hourly["oi_notional"], errors="coerce")
        funding = pd.to_numeric(hourly["funding_rate_scaled"], errors="coerce").fillna(0.0)
        hourly["h1_ret_1"] = close.pct_change()
        hourly["h1_ret_6"] = close.pct_change(6)
        hourly["h1_price_abs_pct"] = _rolling_pct_rank(hourly["h1_ret_6"].abs(), window=720, min_periods=72)
        hourly["h1_oi_chg_6"] = np.log(oi.replace(0.0, np.nan)).diff(6)
        hourly["h1_oi_abs_pct"] = _rolling_pct_rank(hourly["h1_oi_chg_6"].abs(), window=720, min_periods=72)
        hourly["h1_funding_abs_pct"] = _rolling_pct_rank(funding.abs(), window=720, min_periods=72)
        hourly["h1_funding_slope_6"] = funding.abs().diff(6)
        ma_fast = close.rolling(24, min_periods=8).mean()
        ma_slow = close.rolling(96, min_periods=24).mean()
        spread = (ma_fast / ma_slow - 1.0).replace([np.inf, -np.inf], np.nan)
        hourly["h1_trend_regime"] = np.select([spread > 0.01, spread < -0.01], ["uptrend", "downtrend"], default="chop")
        hourly["h1_vol_rank"] = _rolling_pct_rank(close.pct_change().rolling(24, min_periods=8).std(), window=720, min_periods=72)
        hourly["h1_short_build_setup"] = (
            (hourly["h1_ret_6"] < 0.0)
            & (hourly["h1_oi_chg_6"] > 0.0)
            & (hourly["h1_price_abs_pct"] >= 80.0)
            & (hourly["h1_oi_abs_pct"] >= 90.0)
        )
        hourly["h1_neg_funding_oi_expansion"] = (
            (funding < 0.0)
            & (hourly["h1_oi_chg_6"] > 0.0)
            & (hourly["h1_oi_abs_pct"] >= 90.0)
            & (hourly["h1_funding_abs_pct"] >= 80.0)
        )
        hourly["h1_pos_funding_oi_expansion"] = (
            (funding > 0.0)
            & (hourly["h1_oi_chg_6"] > 0.0)
            & (hourly["h1_oi_abs_pct"] >= 90.0)
            & (hourly["h1_funding_abs_pct"] >= 80.0)
        )
        hourly["h1_oi_flush"] = (hourly["h1_oi_chg_6"] < 0.0) & (hourly["h1_oi_abs_pct"] >= 90.0)
        out_frames.append(hourly[[
            "timestamp",
            "symbol",
            "h1_short_build_setup",
            "h1_neg_funding_oi_expansion",
            "h1_pos_funding_oi_expansion",
            "h1_oi_flush",
            "h1_trend_regime",
            "h1_vol_rank",
        ]])
    hourly_all = pd.concat(out_frames, ignore_index=True).sort_values(["symbol", "timestamp"])
    merged = []
    for symbol, group in df.groupby("symbol", sort=False):
        h = hourly_all[hourly_all["symbol"] == symbol].sort_values("timestamp")
        merged.append(pd.merge_asof(group.sort_values("timestamp"), h, on="timestamp", by="symbol", direction="backward"))
    return pd.concat(merged, ignore_index=True).sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def _variant_masks(df: pd.DataFrame) -> dict[str, tuple[str, pd.Series]]:
    lower_low_hold = df["price_down_12"] & df["close_near_low"] & ~df["failed_breakdown_reclaim_24"]
    breakdown_continuation = (df["price_move_abs_pct_12"] >= 70.0) & lower_low_hold
    failed_breakout = df["failed_breakout_rejection_24"] | df["failed_breakout_wick_24"]
    reclaim = df["failed_breakdown_reclaim_24"] | df["failed_breakdown_wick_24"]
    return {
        "1H_SHORT_BUILD_SETUP__5M_BREAKDOWN_CONTINUATION": (
            "short",
            df["h1_short_build_setup"].fillna(False) & breakdown_continuation,
        ),
        "1H_NEG_FUNDING_OI_EXPANSION__5M_LOWER_LOW_HOLD": (
            "short",
            df["h1_neg_funding_oi_expansion"].fillna(False) & lower_low_hold,
        ),
        "1H_POS_FUNDING_OI_EXPANSION__5M_FAILED_BREAKOUT": (
            "short",
            df["h1_pos_funding_oi_expansion"].fillna(False) & failed_breakout,
        ),
        "1H_OI_FLUSH__5M_RECLAIM": (
            "long",
            df["h1_oi_flush"].fillna(False) & reclaim,
        ),
    }


def _cooldown_indices_by_symbol(df: pd.DataFrame, mask: pd.Series, cooldown: int = 12) -> np.ndarray:
    kept: list[int] = []
    masked = mask.fillna(False)
    for _, rows in df[masked].groupby("symbol", sort=False):
        last = -10**9
        for idx in rows.index.to_numpy(dtype=int):
            if int(idx) - last >= cooldown:
                kept.append(int(idx))
                last = int(idx)
    return np.asarray(sorted(kept), dtype=int)


def _simulate_time_stop_with_costs(
    df: pd.DataFrame,
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    indices: np.ndarray,
    direction: str,
    policy: str,
    symbol_costs: dict[str, float],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    time_stop, max_hold = _parse_exit_policy(policy)
    mult = _direction_mult(direction)
    returns: list[float] = []
    gross_returns: list[float] = []
    holds: list[float] = []
    first_profit_times: list[float] = []
    event_details: list[dict[str, Any]] = []
    for idx in indices:
        if idx + 1 >= len(close) or not np.isfinite(close[idx]) or close[idx] <= 0.0:
            continue
        entry = close[idx]
        state = df.iloc[idx]
        cost_bps = float(symbol_costs.get(str(state["symbol"]), 18.0))
        first_profit = None
        for step in range(1, min(max_hold, len(close) - idx - 1) + 1):
            step_bps = ((close[idx + step] / entry) - 1.0) * 10000.0 * mult
            if step_bps > 0.0:
                first_profit = float(step)
                break
        check_idx = min(idx + time_stop, len(close) - 1)
        check_bps = ((close[check_idx] / entry) - 1.0) * 10000.0 * mult
        if check_bps <= 0.0 or check_idx >= idx + max_hold:
            exit_bps = check_bps
            hold = check_idx - idx
        else:
            end_idx = min(idx + max_hold, len(close) - 1)
            exit_bps = ((close[end_idx] / entry) - 1.0) * 10000.0 * mult
            hold = end_idx - idx
        net = float(exit_bps - cost_bps)
        gross_returns.append(float(exit_bps))
        returns.append(net)
        holds.append(float(hold))
        if first_profit is not None:
            first_profit_times.append(first_profit)
        event_details.append({"symbol": str(state["symbol"]), "year": str(state["shadow_year"]), "month": str(state["shadow_month"]), "net_bps": net})
    net_summary = _return_summary(returns)
    gross_summary = _return_summary(gross_returns)
    net = net_summary.get("mean_bps")
    gross = gross_summary.get("mean_bps")
    return {
        "policy": policy,
        "net_bps": net,
        "gross_bps": gross,
        "t_stat": net_summary.get("t_stat"),
        "hit_rate": float(sum(1 for value in returns if value > 0.0) / len(returns)) if returns else None,
        "avg_hold_bars": _safe_mean(holds),
        "cost_survival": float(net / gross) if net is not None and gross is not None and gross > 0.0 else None,
        "time_to_first_profit": _safe_mean(first_profit_times),
        "first_profit_hit_rate": float(len(first_profit_times) / len(returns)) if returns else None,
        "n": len(returns),
    }, event_details


def _status(row: dict[str, Any], baseline_best_net: float | None) -> str:
    if row["event_count"] < 80:
        return "needs_sample_expansion"
    best = row["best_exit"]
    if (best.get("net_bps") or -10**9) <= 0.0:
        return "failed_net"
    if (best.get("t_stat") or -10**9) <= 2.0:
        return "failed_t_stat"
    if (best.get("cost_survival") or -10**9) < 0.8:
        return "failed_cost_survival"
    if len(row.get("positive_symbols") or []) < 3:
        return "symbol_scoped_research_only"
    if (row.get("single_symbol_event_share") or 0.0) > 0.5:
        return "single_symbol_event_dominated_research_only"
    if (row.get("by_month_pnl_concentration") or 0.0) > 0.4:
        return "month_pnl_concentrated_research_only"
    if baseline_best_net is not None and (best.get("net_bps") or -10**9) <= baseline_best_net:
        return "no_material_improvement_over_5m_baseline"
    return "fresh_validation_candidate"


def _load_baseline(repo_root: Path) -> dict[str, Any]:
    path = repo_root / DEFAULT_REPORT_DIR / "targeted_expansion_short_build.json"
    if not path.exists():
        return {"available": False}
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("top_variants") or []
    best_sample = None
    for row in rows:
        if int(row.get("event_count") or 0) < 80:
            continue
        net = (row.get("best_exit") or {}).get("net_bps")
        if net is None:
            continue
        if best_sample is None or float(net) > float((best_sample.get("best_exit") or {}).get("net_bps") or -10**9):
            best_sample = row
    if best_sample is None:
        csv_path = repo_root / DEFAULT_REPORT_DIR / "targeted_expansion_short_build.csv"
        if csv_path.exists():
            csv = pd.read_csv(csv_path)
            if not csv.empty and {"event_count", "net_bps", "t_stat"}.issubset(csv.columns):
                csv["_event_count"] = pd.to_numeric(csv["event_count"], errors="coerce")
                csv["_net_bps"] = pd.to_numeric(csv["net_bps"], errors="coerce")
                csv["_t_stat"] = pd.to_numeric(csv["t_stat"], errors="coerce")
                sample = csv[csv["_event_count"] >= 100].sort_values("_net_bps", ascending=False)
                if not sample.empty:
                    top = sample.iloc[0]
                    best_sample = {"best_exit": {"net_bps": float(top["_net_bps"]), "t_stat": float(top["_t_stat"])}}
    return {
        "available": True,
        "baseline_status_counts": payload.get("status_counts", {}),
        "best_sample_net_bps": None if best_sample is None else (best_sample.get("best_exit") or {}).get("net_bps"),
        "best_sample_t_stat": None if best_sample is None else (best_sample.get("best_exit") or {}).get("t_stat"),
        "baseline_decision": "generic_5m_short_build_rejected",
    }


def build_mtf_report(
    *,
    repo_root: Path,
    symbols: list[str],
    years: list[int],
    exit_policies: list[str],
    horizons: list[int],
    cost_overrides: dict[str, float],
    json_output: Path,
    csv_output: Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    raw, missing, input_summary = _load_frames(repo_root, symbols, years)
    df = _add_1h_setups(raw)
    close = pd.to_numeric(df["close"], errors="coerce").to_numpy()
    high = pd.to_numeric(df["high"], errors="coerce").to_numpy()
    low = pd.to_numeric(df["low"], errors="coerce").to_numpy()
    symbol_costs = {symbol: _cost_for_symbol(symbol, cost_overrides) for symbol in symbols}
    baseline = _load_baseline(repo_root)
    baseline_net = baseline.get("best_sample_net_bps")
    rows: list[dict[str, Any]] = []
    for variant_id, (direction, mask) in _variant_masks(df).items():
        indices = _cooldown_indices_by_symbol(df, mask, cooldown=12)
        if len(indices) == 0:
            continue
        path_by_horizon = {
            f"{horizon}b": _path_metrics(close, high, low, indices, direction, horizon, np.mean(list(symbol_costs.values())))
            for horizon in horizons
        }
        for policy in exit_policies:
            best_exit, event_details = _simulate_time_stop_with_costs(df, close, high, low, indices, direction, policy, symbol_costs)
            by_symbol = _group_return_stats(event_details, "symbol")
            by_year = _group_return_stats(event_details, "year")
            by_month = _group_return_stats(event_details, "month")
            event_count = int(len(indices))
            symbol_counts = Counter(event["symbol"] for event in event_details)
            positive_symbols = sorted(symbol for symbol, stats in by_symbol.items() if stats.get("net_bps") is not None and stats["net_bps"] > 0.0)
            row = {
                "variant_id": f"{variant_id}__{policy}",
                "setup_timeframe": "1h",
                "trigger_timeframe": "5m",
                "direction": direction,
                "event_count": event_count,
                "best_exit": best_exit,
                "positive_symbols": positive_symbols,
                "by_symbol_net_bps": by_symbol,
                "by_year_net_bps": by_year,
                "by_month_net_bps": by_month,
                "single_symbol_event_share": _max_share(dict(symbol_counts), event_count),
                "by_month_pnl_concentration": _max_abs_pnl_share(by_month),
                "path_diagnostics": path_by_horizon,
                "execution_quality": {
                    "spread_at_entry_bps": None,
                    "spread_percentile": None,
                    "next_bar_gap_bps": None,
                    "adverse_selection_1bar_bps": None,
                    "entry_fill_penalty_bps": None,
                    "data_available": False,
                    "missing_fields": ["best_bid", "best_ask", "spread_bps", "depth_usd"],
                },
                "paper_approved": False,
                "live_approved": False,
            }
            row["status"] = _status(row, baseline_net)
            row["score"] = max(0.0, best_exit.get("net_bps") or 0.0) + 10.0 * max(0.0, best_exit.get("t_stat") or 0.0)
            rows.append(row)
    rows.sort(key=lambda item: item["score"], reverse=True)
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "symbols_requested": symbols,
            "symbols_evaluated": sorted(raw["symbol"].unique().tolist()),
            "years": years,
            "setup_timeframe": "1h",
            "trigger_timeframe": "5m",
            "exit_timeframe": "5m",
            "exit_policies": exit_policies,
            "horizons": horizons,
            "cost_bps_by_symbol": symbol_costs,
            "approval_policy": "research_only_outputs_require_fresh_validation",
        },
        "input_summary": input_summary,
        "missing_symbol_data": missing,
        "baseline_5m": baseline,
        "candidate_count": len(rows),
        "status_counts": dict(Counter(row["status"] for row in rows)),
        "top_variants": rows[:50],
        "paper_approved_events": [],
        "live_approved_events": [],
    }
    csv = pd.DataFrame([
        {
            "variant_id": row["variant_id"],
            "direction": row["direction"],
            "event_count": row["event_count"],
            "positive_symbols": ",".join(row["positive_symbols"]),
            "net_bps": row["best_exit"].get("net_bps"),
            "t_stat": row["best_exit"].get("t_stat"),
            "cost_survival": row["best_exit"].get("cost_survival"),
            "hit_rate": row["best_exit"].get("hit_rate"),
            "avg_hold_bars": row["best_exit"].get("avg_hold_bars"),
            "time_to_first_profit": row["best_exit"].get("time_to_first_profit"),
            "single_symbol_event_share": row["single_symbol_event_share"],
            "by_month_pnl_concentration": row["by_month_pnl_concentration"],
            "status": row["status"],
            "score": row["score"],
        }
        for row in rows
    ])
    json_output.parent.mkdir(parents=True, exist_ok=True)
    csv_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    csv.to_csv(csv_output, index=False)
    return report, csv


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Multi-timeframe detector lab: 1h setup plus 5m trigger and exits")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in DEFAULT_YEARS))
    parser.add_argument("--exit-policies", default=",".join(DEFAULT_EXIT_POLICIES))
    parser.add_argument("--horizons", default=",".join(str(horizon) for horizon in DEFAULT_HORIZONS))
    parser.add_argument("--cost-overrides", default="")
    parser.add_argument("--json-output", default=str(DEFAULT_REPORT_DIR / "mtf_lab_report.json"))
    parser.add_argument("--csv-output", default=str(DEFAULT_REPORT_DIR / "top_mtf_variants.csv"))
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    report, _ = build_mtf_report(
        repo_root=repo_root,
        symbols=[item.upper() for item in _parse_csv(args.symbols)],
        years=_parse_ints(args.years),
        exit_policies=_parse_csv(args.exit_policies),
        horizons=_parse_ints(args.horizons),
        cost_overrides=_parse_cost_overrides(args.cost_overrides),
        json_output=repo_root / args.json_output,
        csv_output=repo_root / args.csv_output,
    )
    print(json.dumps({
        "status": "pass",
        "json_output": str(repo_root / args.json_output),
        "csv_output": str(repo_root / args.csv_output),
        "candidate_count": report["candidate_count"],
        "status_counts": report["status_counts"],
        "paper_approved_events": report["paper_approved_events"],
        "live_approved_events": report["live_approved_events"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
