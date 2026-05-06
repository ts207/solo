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
    _cost_for_symbol,
    _load_frames,
    _parse_cost_overrides,
    _variant_masks,
)
from project.scripts.detector_targeted_expansion import DEFAULT_COST_BPS_BY_SYMBOL
from project.scripts.detector_tuning_lab import _direction_mult, _parse_csv, _parse_ints
from project.scripts.detector_shadow_report import _return_summary


DEFAULT_VARIANT = "1H_SHORT_BUILD_SETUP__5M_BREAKDOWN_CONTINUATION"
DEFAULT_EXIT_POLICY = "time_stop24_max96"
SLIPPAGE_GRID = (0.0, 2.0, 5.0, 10.0, 15.0, 25.0)


def _parse_exit_policy(policy: str) -> tuple[int, int]:
    token = policy.strip().lower()
    if not token.startswith("time_stop") or "_max" not in token:
        raise ValueError(f"unsupported MTF diagnosis exit policy: {policy}")
    left, right = token.removeprefix("time_stop").split("_max", 1)
    return int(left), int(right)


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
        "avg_trade_bps": net_mean,
        "total_net_bps": float(np.sum(net_values)) if net_values else 0.0,
        "cost_survival": float(net_mean / gross_mean) if net_mean is not None and gross_mean is not None and gross_mean > 0.0 else None,
    }


def _group_stats(rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key, "unknown"))].append(row)
    total_abs_pnl = sum(abs(_stats(group)["total_net_bps"]) for group in grouped.values())
    out = {}
    for label, group in sorted(grouped.items()):
        stats = _stats(group)
        stats["cumulative_pnl_share"] = (
            float(abs(stats["total_net_bps"]) / total_abs_pnl) if total_abs_pnl > 0.0 else None
        )
        out[label] = stats
    return out


def _symbol_breakdown(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["symbol"])].append(row)
    return {symbol: _stats(group) for symbol, group in sorted(grouped.items())}


def _symbol_month_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[f"{row['symbol']}:{row['month']}"].append(row)
    total_abs_pnl = sum(abs(_stats(group)["total_net_bps"]) for group in grouped.values())
    out = {}
    for key, group in sorted(grouped.items()):
        stats = _stats(group)
        stats["pnl_share"] = float(abs(stats["total_net_bps"]) / total_abs_pnl) if total_abs_pnl > 0.0 else None
        out[key] = stats
    return out


def _top_key(stats: dict[str, Any], share_key: str = "cumulative_pnl_share") -> str | None:
    if not stats:
        return None
    return max(stats, key=lambda key: stats[key].get(share_key) or -10**9)


def _filter_rows(rows: list[dict[str, Any]], key: str, value: str | None) -> list[dict[str, Any]]:
    if value is None:
        return rows
    if key == "symbol_month":
        symbol, month = value.split(":", 1)
        return [row for row in rows if not (row["symbol"] == symbol and row["month"] == month)]
    return [row for row in rows if str(row.get(key)) != value]


def _simulate_events(
    df: pd.DataFrame,
    indices: np.ndarray,
    direction: str,
    exit_policy: str,
    symbol_costs: dict[str, float],
) -> list[dict[str, Any]]:
    close = pd.to_numeric(df["close"], errors="coerce").to_numpy()
    open_ = pd.to_numeric(df["open"], errors="coerce").to_numpy()
    high = pd.to_numeric(df["high"], errors="coerce").to_numpy()
    low = pd.to_numeric(df["low"], errors="coerce").to_numpy()
    time_stop, max_hold = _parse_exit_policy(exit_policy)
    mult = _direction_mult(direction)
    rows: list[dict[str, Any]] = []
    for idx in indices:
        if idx + 1 >= len(close) or not np.isfinite(close[idx]) or close[idx] <= 0.0:
            continue
        state = df.iloc[idx]
        entry = float(close[idx])
        cost_bps = float(symbol_costs.get(str(state["symbol"]), 18.0))
        first_profit = None
        mae_before_mfe = None
        fav_path: list[float] = []
        adv_path: list[float] = []
        for step in range(1, min(max_hold, len(close) - idx - 1) + 1):
            step_bps = ((close[idx + step] / entry) - 1.0) * 10000.0 * mult
            if first_profit is None and step_bps > 0.0:
                first_profit = float(step)
            if mult > 0:
                fav_path.append(float((high[idx + step] / entry - 1.0) * 10000.0))
                adv_path.append(float((low[idx + step] / entry - 1.0) * 10000.0))
            else:
                fav_path.append(float((entry / low[idx + step] - 1.0) * 10000.0))
                adv_path.append(float((entry / high[idx + step] - 1.0) * 10000.0))
        if fav_path and adv_path:
            mfe_step = int(np.nanargmax(fav_path))
            mae_before_mfe = float(np.nanmin(adv_path[: mfe_step + 1]))
        check_idx = min(idx + time_stop, len(close) - 1)
        check_bps = ((close[check_idx] / entry) - 1.0) * 10000.0 * mult
        if check_bps <= 0.0 or check_idx >= idx + max_hold:
            exit_idx = check_idx
            exit_bps = check_bps
        else:
            exit_idx = min(idx + max_hold, len(close) - 1)
            exit_bps = ((close[exit_idx] / entry) - 1.0) * 10000.0 * mult
        next_gap = None
        adverse_1bar = None
        if idx + 1 < len(close):
            next_gap = ((open_[idx + 1] / entry) - 1.0) * 10000.0 * mult if np.isfinite(open_[idx + 1]) else None
            adverse_1bar = ((entry / high[idx + 1]) - 1.0) * 10000.0 if mult < 0 else ((low[idx + 1] / entry) - 1.0) * 10000.0
        rows.append(
            {
                "timestamp": str(state["timestamp"]),
                "symbol": str(state["symbol"]),
                "year": str(state["shadow_year"]),
                "month": str(state["shadow_month"]),
                "session": str(state["session"]),
                "vol_regime": str(state["shadow_vol_regime"]),
                "trend_regime": str(state["h1_trend_regime"]),
                "funding_sign": str(state["funding_sign"]),
                "funding_slope": "rising" if bool(state.get("funding_rising", False)) else "falling",
                "oi_abs_pct_12": float(state["oi_abs_pct_12"]) if np.isfinite(state["oi_abs_pct_12"]) else None,
                "h1_vol_rank": float(state["h1_vol_rank"]) if np.isfinite(state["h1_vol_rank"]) else None,
                "gross_bps": float(exit_bps),
                "net_bps": float(exit_bps - cost_bps),
                "cost_bps": cost_bps,
                "hold_bars": int(exit_idx - idx),
                "time_to_first_profit": first_profit,
                "mae_before_mfe_bps": mae_before_mfe,
                "next_bar_gap_bps": next_gap,
                "adverse_selection_1bar_bps": adverse_1bar,
            }
        )
    return rows


def _walk_forward(rows: list[dict[str, Any]]) -> dict[str, Any]:
    windows = {
        "train_2022_2023_validate_2024": {
            "train": [row for row in rows if row["year"] in {"2022", "2023"}],
            "validation": [row for row in rows if row["year"] == "2024"],
        },
        "train_2022_2024_validate_2025": {
            "train": [row for row in rows if row["year"] in {"2022", "2023", "2024"}],
            "validation": [row for row in rows if row["year"] == "2025"],
        },
    }
    out = {}
    for name, payload in windows.items():
        train = _stats(payload["train"])
        validation = _stats(payload["validation"])
        validation_pass = (
            validation["events"] >= 20
            and (validation.get("net_bps") or -10**9) > 0.0
            and (validation.get("t_stat") or -10**9) > 1.5
            and (validation.get("cost_survival") or -10**9) >= 0.8
        )
        out[name] = {"train": train, "validation": validation, "validation_pass": validation_pass}
    return out


def _slippage_sensitivity(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {f"extra_{int(extra)}bps": _stats(rows, extra_slippage_bps=extra) for extra in SLIPPAGE_GRID}


def _candidate_decision(
    *,
    base: dict[str, Any],
    without_top_month: dict[str, Any],
    without_top_symbol_month: dict[str, Any],
    top_symbol_month_share: float | None,
    walk_forward: dict[str, Any],
    slippage: dict[str, Any],
) -> dict[str, Any]:
    if (base.get("net_bps") or -10**9) <= 0.0:
        return {"status": "kill", "reason": "base_net_not_positive"}
    if (base.get("t_stat") or -10**9) <= 2.0:
        return {"status": "research_only", "reason": "base_t_stat_below_2"}
    if top_symbol_month_share is not None and top_symbol_month_share > 0.35:
        return {"status": "regime_specific_research", "reason": "top_symbol_month_pnl_share_above_0_35"}
    if (without_top_month.get("net_bps") or -10**9) <= 0.0 or (without_top_month.get("cost_survival") or -10**9) < 0.8:
        return {"status": "regime_specific_research", "reason": "collapses_without_top_month"}
    if (without_top_symbol_month.get("net_bps") or -10**9) <= 0.0:
        return {"status": "regime_specific_research", "reason": "collapses_without_top_symbol_month"}
    extra_5 = slippage.get("extra_5bps", {})
    extra_10 = slippage.get("extra_10bps", {})
    if (extra_5.get("net_bps") or -10**9) <= 0.0:
        return {"status": "research_only", "reason": "dies_with_plus_5bps_slippage"}
    if (extra_10.get("net_bps") or -10**9) <= 0.0:
        return {"status": "needs_execution_data", "reason": "dies_with_plus_10bps_slippage"}
    if not all(payload.get("validation_pass") for payload in walk_forward.values()):
        return {"status": "needs_execution_data", "reason": "walk_forward_validation_not_passed"}
    return {"status": "fresh_validation_candidate", "reason": "diagnostic_gates_passed_requires_book_data_before_paper"}


def build_diagnosis_report(
    *,
    repo_root: Path,
    variant: str,
    symbols: list[str],
    years: list[int],
    exit_policy: str,
    cost_overrides: dict[str, float],
    json_output: Path,
) -> dict[str, Any]:
    raw, missing, input_summary = _load_frames(repo_root, symbols, years)
    df = _add_1h_setups(raw)
    masks = _variant_masks(df)
    if variant not in masks:
        normalized = variant.replace("_5M_", "__5M_").replace("1H_", "1H_")
        if normalized in masks:
            variant = normalized
        else:
            raise KeyError(f"unknown MTF variant {variant}; available={sorted(masks)}")
    direction, mask = masks[variant]
    indices = _cooldown_indices_by_symbol(df, mask, cooldown=12)
    symbol_costs = {symbol: float(cost_overrides.get(symbol, DEFAULT_COST_BPS_BY_SYMBOL.get(symbol, 18.0))) for symbol in symbols}
    events = _simulate_events(df, indices, direction, exit_policy, symbol_costs)
    base = _stats(events)
    by_month = _group_stats(events, "month")
    by_symbol = _group_stats(events, "symbol")
    by_year = _group_stats(events, "year")
    by_regime = {
        "volatility_regime": _group_stats(events, "vol_regime"),
        "trend_regime": _group_stats(events, "trend_regime"),
        "funding_sign": _group_stats(events, "funding_sign"),
        "funding_slope": _group_stats(events, "funding_slope"),
        "session": _group_stats(events, "session"),
    }
    symbol_month = _symbol_month_stats(events)
    top_profit_month = max(by_month, key=lambda key: by_month[key]["total_net_bps"]) if by_month else None
    top_loss_month = min(by_month, key=lambda key: by_month[key]["total_net_bps"]) if by_month else None
    top_month = _top_key(by_month)
    top_symbol_month = _top_key(symbol_month, share_key="pnl_share")
    without_top_month = _stats(_filter_rows(events, "month", top_month))
    without_top_symbol_month = _stats(_filter_rows(events, "symbol_month", top_symbol_month))
    walk_forward = _walk_forward(events)
    slippage = _slippage_sensitivity(events)
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "variant": variant,
            "exit_policy": exit_policy,
            "setup_timeframe": "1h",
            "trigger_timeframe": "5m",
            "symbols": symbols,
            "years": years,
            "cost_bps_by_symbol": symbol_costs,
        },
        "input_summary": input_summary,
        "missing_symbol_data": missing,
        "base_result": base,
        "by_month": {month: {**stats, "symbol_breakdown": _symbol_breakdown([row for row in events if row["month"] == month])} for month, stats in by_month.items()},
        "by_symbol": by_symbol,
        "by_symbol_month": symbol_month,
        "top_profit_month": {"month": top_profit_month, "stats": by_month.get(top_profit_month)} if top_profit_month else None,
        "top_loss_month": {"month": top_loss_month, "stats": by_month.get(top_loss_month)} if top_loss_month else None,
        "top_symbol_month_pnl_share": None if top_symbol_month is None else symbol_month[top_symbol_month].get("pnl_share"),
        "top_symbol_month": {"symbol_month": top_symbol_month, "stats": symbol_month.get(top_symbol_month)} if top_symbol_month else None,
        "without_top_month": without_top_month,
        "without_top_symbol_month": without_top_symbol_month,
        "by_year": by_year,
        "by_regime": by_regime,
        "path_execution_diagnostics": {
            "time_to_first_profit": _safe_mean([row["time_to_first_profit"] for row in events if row.get("time_to_first_profit") is not None]),
            "mae_before_mfe_bps": _safe_mean([row["mae_before_mfe_bps"] for row in events if row.get("mae_before_mfe_bps") is not None]),
            "next_bar_gap_bps": _safe_mean([row["next_bar_gap_bps"] for row in events if row.get("next_bar_gap_bps") is not None]),
            "adverse_selection_1bar_bps": _safe_mean([row["adverse_selection_1bar_bps"] for row in events if row.get("adverse_selection_1bar_bps") is not None]),
            "spread_at_entry_bps": None,
            "spread_percentile": None,
            "entry_fill_penalty_bps": None,
            "book_data_available": False,
            "missing_fields": ["best_bid", "best_ask", "spread_bps", "bid_depth_usd", "ask_depth_usd", "depth_usd"],
        },
        "slippage_sensitivity": slippage,
        "walk_forward": walk_forward,
        "paper_approved_events": [],
        "live_approved_events": [],
    }
    report["candidate_decision"] = _candidate_decision(
        base=base,
        without_top_month=without_top_month,
        without_top_symbol_month=without_top_symbol_month,
        top_symbol_month_share=report["top_symbol_month_pnl_share"],
        walk_forward=walk_forward,
        slippage=slippage,
    )
    json_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose concentration and execution realism for a selected MTF detector variant")
    parser.add_argument("--variant", default=DEFAULT_VARIANT)
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in DEFAULT_YEARS))
    parser.add_argument("--exit-policy", default=DEFAULT_EXIT_POLICY)
    parser.add_argument("--cost-overrides", default="")
    parser.add_argument("--json-output", default=str(DEFAULT_REPORT_DIR / "mtf_diagnosis_short_build.json"))
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    report = build_diagnosis_report(
        repo_root=repo_root,
        variant=str(args.variant),
        symbols=[item.upper() for item in _parse_csv(args.symbols)],
        years=_parse_ints(args.years),
        exit_policy=str(args.exit_policy),
        cost_overrides=_parse_cost_overrides(args.cost_overrides),
        json_output=repo_root / args.json_output,
    )
    print(json.dumps({
        "status": "pass",
        "json_output": str(repo_root / args.json_output),
        "base_result": report["base_result"],
        "top_symbol_month_pnl_share": report["top_symbol_month_pnl_share"],
        "without_top_month": report["without_top_month"],
        "without_top_symbol_month": report["without_top_symbol_month"],
        "candidate_decision": report["candidate_decision"],
        "paper_approved_events": report["paper_approved_events"],
        "live_approved_events": report["live_approved_events"],
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
