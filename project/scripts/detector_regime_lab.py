from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.scripts.detector_mtf_lab import (
    DEFAULT_REPORT_DIR,
    DEFAULT_SYMBOLS,
    DEFAULT_YEARS,
    _add_1h_setups,
    _cooldown_indices_by_symbol,
    _load_frames,
    _variant_masks,
)
from project.scripts.detector_mtf_diagnose import _parse_exit_policy
from project.scripts.detector_shadow_report import _return_summary, _rolling_pct_rank
from project.scripts.detector_targeted_expansion import DEFAULT_COST_BPS_BY_SYMBOL, _parse_cost_overrides
from project.scripts.detector_tuning_lab import _direction_mult, _parse_csv, _parse_ints


DEFAULT_VARIANT = "1H_SHORT_BUILD_SETUP__5M_BREAKDOWN_CONTINUATION"
DEFAULT_EXIT_POLICY = "time_stop24_max96"
VOL_PCTS = (80.0, 90.0, 95.0)
BREADTH_PCTS = (60.0, 70.0, 80.0)
TREND_FILTERS = ("h1_down", "h4_down", "both_down")
FUNDING_FILTERS = ("negative", "falling", "not_positive_extreme")


def _safe_mean(values: list[float]) -> float | None:
    clean = [float(value) for value in values if value is not None and math.isfinite(float(value))]
    return float(np.mean(clean)) if clean else None


def _stats(rows: list[dict[str, Any]], *, extra_slippage_bps: float = 0.0) -> dict[str, Any]:
    net_values = [float(row["net_bps"]) - extra_slippage_bps for row in rows]
    gross_values = [float(row["gross_bps"]) for row in rows]
    net = _return_summary(net_values)
    gross = _return_summary(gross_values)
    net_mean = net.get("mean_bps")
    gross_mean = gross.get("mean_bps")
    return {
        "events": len(rows),
        "gross_bps": gross_mean,
        "net_bps": net_mean,
        "t_stat": net.get("t_stat"),
        "hit_rate": float(sum(1 for value in net_values if value > 0.0) / len(net_values)) if net_values else None,
        "total_net_bps": float(np.sum(net_values)) if net_values else 0.0,
        "cost_survival": float(net_mean / gross_mean) if net_mean is not None and gross_mean is not None and gross_mean > 0.0 else None,
    }


def _group_stats(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key, "unknown"))].append(row)
    total_abs = sum(abs(_stats(group)["total_net_bps"]) for group in grouped.values())
    out = {}
    for label, group in sorted(grouped.items()):
        stats = _stats(group)
        stats["pnl_share"] = float(abs(stats["total_net_bps"]) / total_abs) if total_abs > 0.0 else None
        out[label] = stats
    return out


def _top_share(grouped: dict[str, Any]) -> float | None:
    shares = [row.get("pnl_share") for row in grouped.values() if row.get("pnl_share") is not None]
    return float(max(shares)) if shares else None


def _add_market_regime(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy().sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    per_symbol = []
    for _, group in out.groupby("symbol", sort=False):
        g = group.sort_values("timestamp").copy()
        close = pd.to_numeric(g["close"], errors="coerce")
        g["h1_ma"] = close.rolling(12, min_periods=6).mean()
        g["h4_ma"] = close.rolling(48, min_periods=24).mean()
        g["below_h1_ma"] = close < g["h1_ma"]
        g["below_h4_ma"] = close < g["h4_ma"]
        g["rv_1h"] = close.pct_change().rolling(12, min_periods=6).std()
        g["rv_4h"] = close.pct_change().rolling(48, min_periods=24).std()
        g["symbol_vol_pct"] = _rolling_pct_rank(g["rv_4h"], window=2880, min_periods=288)
        per_symbol.append(g)
    out = pd.concat(per_symbol, ignore_index=True).sort_values(["timestamp", "symbol"]).reset_index(drop=True)
    breadth = (
        out.groupby("timestamp")
        .agg(
            market_breadth_below_h1=("below_h1_ma", "mean"),
            market_breadth_below_h4=("below_h4_ma", "mean"),
            market_vol_median=("symbol_vol_pct", "median"),
        )
        .reset_index()
    )
    btc = out[out["symbol"] == "BTCUSDT"][["timestamp", "below_h1_ma", "below_h4_ma"]].rename(
        columns={"below_h1_ma": "btc_below_h1_ma", "below_h4_ma": "btc_below_h4_ma"}
    )
    breadth = breadth.merge(btc, on="timestamp", how="left")
    return out.merge(breadth, on="timestamp", how="left").sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def _add_symbol_bounds(df: pd.DataFrame) -> pd.DataFrame:
    out = df.sort_values(["symbol", "timestamp"]).reset_index(drop=True).copy()
    out["_symbol_end_idx"] = 0
    for _, group in out.groupby("symbol", sort=False):
        end_idx = int(group.index.max())
        out.loc[group.index, "_symbol_end_idx"] = end_idx
    return out


def _regime_mask(df: pd.DataFrame, *, vol_pct: float, trend_filter: str, breadth_pct: float, funding_filter: str) -> pd.Series:
    vol = pd.to_numeric(df["market_vol_median"], errors="coerce") >= vol_pct
    breadth_h1 = pd.to_numeric(df["market_breadth_below_h1"], errors="coerce") >= breadth_pct / 100.0
    breadth_h4 = pd.to_numeric(df["market_breadth_below_h4"], errors="coerce") >= breadth_pct / 100.0
    if trend_filter == "h1_down":
        trend = breadth_h1 & df["btc_below_h1_ma"].fillna(False)
    elif trend_filter == "h4_down":
        trend = breadth_h4 & df["btc_below_h4_ma"].fillna(False)
    elif trend_filter == "both_down":
        trend = breadth_h1 & breadth_h4 & df["btc_below_h1_ma"].fillna(False) & df["btc_below_h4_ma"].fillna(False)
    else:
        raise ValueError(f"unknown trend filter: {trend_filter}")
    if funding_filter == "negative":
        funding = df["funding_sign"] == "negative"
    elif funding_filter == "falling":
        funding = df["funding_falling"].fillna(False)
    elif funding_filter == "not_positive_extreme":
        funding = ~((df["funding_sign"] == "positive") & (pd.to_numeric(df["funding_abs_pct"], errors="coerce") >= 95.0))
    else:
        raise ValueError(f"unknown funding filter: {funding_filter}")
    return (vol & trend & funding).fillna(False)


def _simulate_events(
    df: pd.DataFrame,
    indices: np.ndarray,
    direction: str,
    exit_policy: str,
    symbol_costs: dict[str, float],
) -> list[dict[str, Any]]:
    close = pd.to_numeric(df["close"], errors="coerce").to_numpy()
    time_stop, max_hold = _parse_exit_policy(exit_policy)
    mult = _direction_mult(direction)
    rows = []
    for idx in indices:
        if idx + 1 >= len(close) or not np.isfinite(close[idx]) or close[idx] <= 0.0:
            continue
        state = df.iloc[idx]
        symbol_end_idx = int(state.get("_symbol_end_idx", len(close) - 1))
        if idx >= symbol_end_idx:
            continue
        entry = float(close[idx])
        cost = float(symbol_costs.get(str(state["symbol"]), 18.0))
        check_idx = min(idx + time_stop, symbol_end_idx)
        check_bps = ((close[check_idx] / entry) - 1.0) * 10000.0 * mult
        if check_bps <= 0.0 or check_idx >= idx + max_hold:
            exit_bps = check_bps
        else:
            end_idx = min(idx + max_hold, symbol_end_idx)
            exit_bps = ((close[end_idx] / entry) - 1.0) * 10000.0 * mult
        rows.append(
            {
                "symbol": str(state["symbol"]),
                "year": str(state["shadow_year"]),
                "month": str(state["shadow_month"]),
                "symbol_month": f"{state['symbol']}:{state['shadow_month']}",
                "vol_regime": str(state["shadow_vol_regime"]),
                "h1_trend_regime": str(state["h1_trend_regime"]),
                "session": str(state["session"]),
                "gross_bps": float(exit_bps),
                "net_bps": float(exit_bps - cost),
            }
        )
    return rows


def _without_top(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    grouped = _group_stats(rows, key)
    if not grouped:
        return _stats([])
    top = max(grouped, key=lambda item: grouped[item].get("pnl_share") or -10**9)
    return _stats([row for row in rows if row[key] != top])


def _walk_forward(rows: list[dict[str, Any]]) -> dict[str, Any]:
    windows = {
        "train_2022_2023_validate_2024": [row for row in rows if row["year"] == "2024"],
        "train_2022_2024_validate_2025": [row for row in rows if row["year"] == "2025"],
    }
    out = {}
    for name, validation_rows in windows.items():
        validation = _stats(validation_rows)
        out[name] = {
            "validation": validation,
            "validation_pass": (
                validation["events"] >= 20
                and (validation.get("net_bps") or -10**9) > 0.0
                and (validation.get("t_stat") or -10**9) > 1.5
                and (validation.get("cost_survival") or -10**9) >= 0.8
            ),
        }
    return out


def _status(row: dict[str, Any]) -> str:
    base = row["inside_regime"]
    if base["events"] < 80:
        return "needs_sample_expansion"
    if (base.get("net_bps") or -10**9) <= 0.0:
        return "failed_net"
    if (base.get("t_stat") or -10**9) <= 2.0:
        return "failed_t_stat"
    if (base.get("cost_survival") or -10**9) < 0.8:
        return "failed_cost_survival"
    if (row.get("top_symbol_month_pnl_share") or 0.0) > 0.35:
        return "symbol_month_concentrated_research_only"
    without_month = row["without_top_month"]
    if (without_month.get("net_bps") or -10**9) <= 0.0 or (without_month.get("cost_survival") or -10**9) < 0.8:
        return "collapses_without_top_month_research_only"
    if (row["slippage_sensitivity"].get("extra_10bps", {}).get("net_bps") or -10**9) <= 0.0:
        return "fails_slippage_research_only"
    if not all(payload["validation_pass"] for payload in row["walk_forward"].values()):
        return "walk_forward_failed_research_only"
    return "fresh_validation_candidate"


def build_regime_report(
    *,
    repo_root: Path,
    symbols: list[str],
    years: list[int],
    cost_overrides: dict[str, float],
    json_output: Path,
    csv_output: Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    raw, missing, input_summary = _load_frames(repo_root, symbols, years)
    df = _add_symbol_bounds(_add_market_regime(_add_1h_setups(raw)))
    direction, base_mask = _variant_masks(df)[DEFAULT_VARIANT]
    symbol_costs = {symbol: float(cost_overrides.get(symbol, DEFAULT_COST_BPS_BY_SYMBOL.get(symbol, 18.0))) for symbol in symbols}
    rows = []
    for vol_pct in VOL_PCTS:
        for trend_filter in TREND_FILTERS:
            for breadth_pct in BREADTH_PCTS:
                for funding_filter in FUNDING_FILTERS:
                    regime = _regime_mask(df, vol_pct=vol_pct, trend_filter=trend_filter, breadth_pct=breadth_pct, funding_filter=funding_filter)
                    inside_mask = base_mask & regime
                    outside_mask = base_mask & ~regime
                    inside_indices = _cooldown_indices_by_symbol(df, inside_mask, cooldown=12)
                    outside_indices = _cooldown_indices_by_symbol(df, outside_mask, cooldown=12)
                    inside_events = _simulate_events(df, inside_indices, direction, DEFAULT_EXIT_POLICY, symbol_costs)
                    outside_events = _simulate_events(df, outside_indices, direction, DEFAULT_EXIT_POLICY, symbol_costs)
                    by_symbol_month = _group_stats(inside_events, "symbol_month")
                    row = {
                        "variant_id": f"CRISIS_DOWNTREND_REGIME_V{int(vol_pct)}_B{int(breadth_pct)}_{trend_filter}_{funding_filter}",
                        "base_family": "MTF_SHORT_BUILD_BREAKDOWN_CONTINUATION",
                        "regime": {
                            "vol_percentile_min": vol_pct,
                            "trend_filter": trend_filter,
                            "market_breadth_below_ma_pct": breadth_pct,
                            "funding_filter": funding_filter,
                        },
                        "inside_regime": _stats(inside_events),
                        "outside_regime": _stats(outside_events),
                        "by_symbol": _group_stats(inside_events, "symbol"),
                        "by_year": _group_stats(inside_events, "year"),
                        "by_month": _group_stats(inside_events, "month"),
                        "by_symbol_month": by_symbol_month,
                        "top_symbol_month_pnl_share": _top_share(by_symbol_month),
                        "without_top_month": _without_top(inside_events, "month"),
                        "without_top_symbol_month": _without_top(inside_events, "symbol_month"),
                        "by_regime": {
                            "volatility_regime": _group_stats(inside_events, "vol_regime"),
                            "h1_trend_regime": _group_stats(inside_events, "h1_trend_regime"),
                            "session": _group_stats(inside_events, "session"),
                        },
                        "slippage_sensitivity": {f"extra_{int(extra)}bps": _stats(inside_events, extra_slippage_bps=extra) for extra in (0.0, 2.0, 5.0, 10.0, 15.0, 25.0)},
                        "walk_forward": _walk_forward(inside_events),
                        "paper_approved": False,
                        "live_approved": False,
                    }
                    row["status"] = _status(row)
                    row["score"] = max(0.0, row["inside_regime"].get("net_bps") or 0.0) + 10.0 * max(0.0, row["inside_regime"].get("t_stat") or 0.0)
                    rows.append(row)
    rows.sort(key=lambda item: item["score"], reverse=True)
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "family": "MTF_SHORT_BUILD_BREAKDOWN_CONTINUATION",
            "symbols": symbols,
            "years": years,
            "setup_timeframe": "1h",
            "trigger_timeframe": "5m",
            "exit_policy": DEFAULT_EXIT_POLICY,
            "cost_bps_by_symbol": symbol_costs,
            "approval_policy": "research_only_outputs_require_fresh_validation",
        },
        "input_summary": input_summary,
        "missing_symbol_data": missing,
        "candidate_count": len(rows),
        "status_counts": dict(Counter(row["status"] for row in rows)),
        "top_variants": rows[:50],
        "paper_approved_events": [],
        "live_approved_events": [],
    }
    csv = pd.DataFrame([
        {
            "variant_id": row["variant_id"],
            "events": row["inside_regime"]["events"],
            "net_bps": row["inside_regime"]["net_bps"],
            "t_stat": row["inside_regime"]["t_stat"],
            "cost_survival": row["inside_regime"]["cost_survival"],
            "outside_net_bps": row["outside_regime"]["net_bps"],
            "top_symbol_month_pnl_share": row["top_symbol_month_pnl_share"],
            "without_top_month_net_bps": row["without_top_month"]["net_bps"],
            "without_top_month_t_stat": row["without_top_month"]["t_stat"],
            "slippage_10_net_bps": row["slippage_sensitivity"]["extra_10bps"]["net_bps"],
            "walk_forward_2024_pass": row["walk_forward"]["train_2022_2023_validate_2024"]["validation_pass"],
            "walk_forward_2025_pass": row["walk_forward"]["train_2022_2024_validate_2025"]["validation_pass"],
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
    parser = argparse.ArgumentParser(description="Regime-gated lab for MTF short-build breakdown continuation")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in DEFAULT_YEARS))
    parser.add_argument("--cost-overrides", default="")
    parser.add_argument("--json-output", default=str(DEFAULT_REPORT_DIR / "regime_lab_report.json"))
    parser.add_argument("--csv-output", default=str(DEFAULT_REPORT_DIR / "top_regime_variants.csv"))
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    report, _ = build_regime_report(
        repo_root=repo_root,
        symbols=[item.upper() for item in _parse_csv(args.symbols)],
        years=_parse_ints(args.years),
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
