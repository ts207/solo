from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from datetime import UTC, datetime
from itertools import product
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.scripts import detector_daily_carry_trend_lab as daily_lab
from project.scripts.detector_targeted_expansion import _parse_cost_overrides
from project.scripts.detector_tuning_lab import _parse_csv, _parse_ints

DEFAULT_REPORT_DIR = Path("data/reports/detectors/no_liquidations_v1")
FAMILY = "DAILY_CARRY_TREND_DIAGNOSIS"
BASE_FAMILY = daily_lab.FAMILY
TARGET_YEARS = ("2023", "2024", "2025")


def _finite_mean(values: list[float]) -> float | None:
    if not values:
        return None
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return None
    return float(np.mean(arr))


def _max_drawdown(values: list[float]) -> float | None:
    if not values:
        return None
    curve = np.cumsum(np.asarray(values, dtype=float))
    peaks = np.maximum.accumulate(curve)
    drawdowns = curve - peaks
    return float(np.min(drawdowns))


def _annualized_sharpe(values: list[float], *, rebalance_days: int) -> float | None:
    if len(values) < 2:
        return None
    arr = np.asarray(values, dtype=float)
    std = float(np.std(arr, ddof=1))
    if std <= 0.0 or not math.isfinite(std):
        return None
    return float(np.mean(arr) / std * math.sqrt(365.0 / float(rebalance_days)))


def _status(row: dict[str, Any]) -> str:
    if int(row.get("event_count") or 0) < 100:
        return "needs_sample_expansion"
    if (row.get("net_pnl") or -1e9) <= 0.0:
        return "failed_net"
    if (row.get("t_stat") or -1e9) <= 2.0:
        return "failed_t_stat"
    if (row.get("cost_survival") or -1e9) < 0.8:
        return "failed_cost_survival"
    if (row.get("plus_10_bps_net_pnl") or -1e9) <= 0.0:
        return "failed_plus_10_bps_slippage"
    if not row.get("positive_target_years_pass", False):
        return "failed_year_split"
    if (row.get("top_symbol_month_share") or 0.0) > 0.35:
        return "symbol_month_concentrated_research_only"
    return "diagnostic_research_candidate"


def _leg_return(
    row: pd.Series,
    *,
    direction: str,
    hold_days: int,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
) -> dict[str, Any]:
    symbol = str(row["symbol"])
    price_bps = (
        float(row[f"fwd_price_ret_{hold_days}d"]) * 10000.0 * daily_lab._direction_mult(direction)
    )
    funding_bps = float(
        row[f"fwd_funding_long_bps_{hold_days}d"]
        if direction == "long"
        else row[f"fwd_funding_short_bps_{hold_days}d"]
    )
    gross_bps = price_bps + funding_bps
    cost_bps = float(symbol_costs.get(symbol, 18.0))
    net_bps = gross_bps - cost_bps
    return {
        "symbol": symbol,
        "direction": direction,
        "price_bps": price_bps,
        "funding_bps": funding_bps,
        "gross_bps": gross_bps,
        "cost_bps": cost_bps,
        "net_bps": net_bps,
        "plus_10_bps": net_bps - extra_slippage_bps,
    }


def _variant_id(
    *,
    rebalance_days: int,
    hold_days: int,
    basket_size: int,
    rank_signal: str,
    funding_filter: str,
) -> str:
    return (
        f"{BASE_FAMILY}__DIAGNOSE__TREND_FOLLOW__RANK_{rank_signal.upper()}__"
        f"REBALANCE_{rebalance_days}D__HOLD_{hold_days}D__TOP{basket_size}_BOTTOM{basket_size}__"
        f"FUNDING_FILTER_{funding_filter.upper()}"
    )


def _evaluate_diagnostic_variant(
    df: pd.DataFrame,
    *,
    rebalance_days: int,
    hold_days: int,
    basket_size: int,
    rank_signal: str,
    funding_filter: str,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
) -> dict[str, Any]:
    basket_net: list[float] = []
    basket_gross: list[float] = []
    basket_price: list[float] = []
    basket_funding: list[float] = []
    basket_cost: list[float] = []
    basket_plus_10: list[float] = []
    long_net: list[float] = []
    short_net: list[float] = []
    long_only_net: list[float] = []
    short_only_net: list[float] = []
    turnover_fractions: list[float] = []
    leg_details: list[dict[str, Any]] = []
    basket_details: list[dict[str, Any]] = []
    previous_keys: set[tuple[str, str]] | None = None

    timestamps = sorted(df["timestamp"].dropna().unique())
    for idx, ts in enumerate(timestamps):
        if idx % rebalance_days != 0:
            continue
        group = df[df["timestamp"] == ts].copy()
        group["_score"] = daily_lab._rank_score(group, rank_signal)
        valid_mask = (
            np.isfinite(pd.to_numeric(group["_score"], errors="coerce"))
            & np.isfinite(pd.to_numeric(group[f"fwd_price_ret_{hold_days}d"], errors="coerce"))
            & np.isfinite(
                pd.to_numeric(group[f"fwd_funding_long_bps_{hold_days}d"], errors="coerce")
            )
            & group["crash_filter_ok"].fillna(False)
        )
        if funding_filter == "on":
            valid_mask &= (
                pd.to_numeric(group["funding_abs_pct"], errors="coerce")
                <= daily_lab.FUNDING_ABS_PCT_MAX
            )
        valid = group[valid_mask].copy()
        if len(valid) < max(daily_lab.MIN_CROSS_SECTION, basket_size * 2):
            continue

        ranked = valid.sort_values("_score", ascending=True)
        bottom = ranked.head(basket_size)
        top = ranked.tail(basket_size).iloc[::-1]
        legs: list[dict[str, Any]] = []
        for _, row in top.iterrows():
            leg = _leg_return(
                row,
                direction="long",
                hold_days=hold_days,
                symbol_costs=symbol_costs,
                extra_slippage_bps=extra_slippage_bps,
            )
            leg["rank_bucket"] = "top"
            leg["year"] = str(row["shadow_year"])
            leg["month"] = str(row["shadow_month"])
            legs.append(leg)
        for _, row in bottom.iterrows():
            leg = _leg_return(
                row,
                direction="short",
                hold_days=hold_days,
                symbol_costs=symbol_costs,
                extra_slippage_bps=extra_slippage_bps,
            )
            leg["rank_bucket"] = "bottom"
            leg["year"] = str(row["shadow_year"])
            leg["month"] = str(row["shadow_month"])
            legs.append(leg)
        if len(legs) != basket_size * 2:
            continue

        keys = {(str(leg["symbol"]), str(leg["direction"])) for leg in legs}
        if previous_keys is None:
            turnover = 1.0
        else:
            changed = len(keys - previous_keys) + len(previous_keys - keys)
            turnover = float(changed) / float(len(keys) * 2)
        previous_keys = keys
        turnover_fractions.append(turnover)

        long_legs = [leg for leg in legs if leg["direction"] == "long"]
        short_legs = [leg for leg in legs if leg["direction"] == "short"]
        net_value = float(np.mean([leg["net_bps"] for leg in legs]))
        gross_value = float(np.mean([leg["gross_bps"] for leg in legs]))
        price_value = float(np.mean([leg["price_bps"] for leg in legs]))
        funding_value = float(np.mean([leg["funding_bps"] for leg in legs]))
        cost_value = float(np.mean([leg["cost_bps"] for leg in legs]))
        plus_value = float(np.mean([leg["plus_10_bps"] for leg in legs]))
        long_value = float(np.mean([leg["net_bps"] for leg in long_legs]))
        short_value = float(np.mean([leg["net_bps"] for leg in short_legs]))
        basket_net.append(net_value)
        basket_gross.append(gross_value)
        basket_price.append(price_value)
        basket_funding.append(funding_value)
        basket_cost.append(cost_value)
        basket_plus_10.append(plus_value)
        long_net.append(long_value)
        short_net.append(short_value)
        long_only_net.append(long_value)
        short_only_net.append(short_value)
        leg_details.extend(
            [
                {
                    "timestamp": str(ts),
                    "symbol": leg["symbol"],
                    "year": leg["year"],
                    "month": leg["month"],
                    "symbol_month": f"{leg['symbol']}:{leg['month']}",
                    "direction": leg["direction"],
                    "rank_bucket": leg["rank_bucket"],
                    "net_bps": leg["net_bps"],
                    "gross_bps": leg["gross_bps"],
                    "price_bps": leg["price_bps"],
                    "funding_bps": leg["funding_bps"],
                    "cost_bps": leg["cost_bps"],
                }
                for leg in legs
            ]
        )
        basket_details.append(
            {
                "timestamp": str(ts),
                "year": str(pd.Timestamp(ts).year),
                "month": pd.Timestamp(ts).strftime("%Y-%m"),
                "net_bps": net_value,
                "gross_bps": gross_value,
                "price_bps": price_value,
                "funding_bps": funding_value,
                "cost_bps": cost_value,
            }
        )

    net_summary = daily_lab._return_summary(basket_net)
    gross_summary = daily_lab._return_summary(basket_gross)
    plus_summary = daily_lab._return_summary(basket_plus_10)
    by_symbol = daily_lab._group_return_stats(leg_details, "symbol")
    by_year = daily_lab._group_return_stats(basket_details, "year")
    by_month = daily_lab._group_return_stats(basket_details, "month")
    symbol_month_counts = dict(Counter(event["symbol_month"] for event in leg_details))
    target_year_positive = {
        year: (by_year.get(year, {}).get("net_bps") or -1e9) > 0.0 for year in TARGET_YEARS
    }
    gross_pnl = gross_summary.get("mean_bps")
    net_pnl = net_summary.get("mean_bps")
    row = {
        "variant_id": _variant_id(
            rebalance_days=rebalance_days,
            hold_days=hold_days,
            basket_size=basket_size,
            rank_signal=rank_signal,
            funding_filter=funding_filter,
        ),
        "family": FAMILY,
        "base_family": BASE_FAMILY,
        "direction_mode": "trend_follow",
        "rank_signal": rank_signal,
        "rebalance_days": rebalance_days,
        "hold_days": hold_days,
        "basket_size_per_side": basket_size,
        "funding_filter": funding_filter,
        "event_count": len(basket_net),
        "leg_count": len(leg_details),
        "gross_pnl": gross_pnl,
        "net_pnl": net_pnl,
        "cost_paid_bps": _finite_mean(basket_cost),
        "turnover": _finite_mean(turnover_fractions),
        "annualized_turnover": (_finite_mean(turnover_fractions) or 0.0)
        * (365.0 / float(rebalance_days)),
        "long_leg_pnl": _finite_mean(long_net),
        "short_leg_pnl": _finite_mean(short_net),
        "long_only_net_pnl": _finite_mean(long_only_net),
        "short_only_net_pnl": _finite_mean(short_only_net),
        "price_pnl": _finite_mean(basket_price),
        "funding_pnl": _finite_mean(basket_funding),
        "t_stat": net_summary.get("t_stat"),
        "gross_t_stat": gross_summary.get("t_stat"),
        "cost_survival": float(net_pnl / gross_pnl)
        if net_pnl is not None and gross_pnl is not None and gross_pnl > 0.0
        else None,
        "plus_10_bps_net_pnl": plus_summary.get("mean_bps"),
        "plus_10_bps_t_stat": plus_summary.get("t_stat"),
        "plus_10_bps_survives": (plus_summary.get("mean_bps") or -1e9) > 0.0,
        "by_year": by_year,
        "by_symbol": by_symbol,
        "by_month": by_month,
        "max_drawdown": _max_drawdown(basket_net),
        "sharpe": _annualized_sharpe(basket_net, rebalance_days=rebalance_days),
        "positive_target_years": target_year_positive,
        "positive_target_years_pass": all(target_year_positive.values()),
        "top_symbol_month_share": daily_lab._max_share(symbol_month_counts, len(leg_details)),
        "paper_approved": False,
        "live_approved": False,
    }
    row["status"] = _status(row)
    row["score"] = (
        max(0.0, row.get("net_pnl") or 0.0)
        + 10.0 * max(0.0, row.get("t_stat") or 0.0)
        + 20.0 * max(0.0, row.get("cost_survival") or 0.0)
        - 1000.0 * float(row["status"] != "diagnostic_research_candidate")
    )
    return row


def _best(rows: list[dict[str, Any]], predicate: Any) -> dict[str, Any]:
    filtered = [row for row in rows if predicate(row)]
    if not filtered:
        return {}
    return max(filtered, key=lambda row: (row.get("net_pnl") or -1e9, row.get("t_stat") or -1e9))


def _diagnostic_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    best_daily = _best(rows, lambda row: row["rebalance_days"] == 1)
    best_weekly = _best(rows, lambda row: row["rebalance_days"] == 7)
    best_overall = _best(rows, lambda row: True)
    best_long_only = max(rows, key=lambda row: row.get("long_only_net_pnl") or -1e9) if rows else {}
    weekly_cost = best_weekly.get("cost_survival")
    daily_cost = best_daily.get("cost_survival")
    return {
        "best_daily_rebalance": best_daily.get("variant_id"),
        "best_daily_net_pnl": best_daily.get("net_pnl"),
        "best_daily_cost_survival": daily_cost,
        "best_weekly_rebalance": best_weekly.get("variant_id"),
        "best_weekly_net_pnl": best_weekly.get("net_pnl"),
        "best_weekly_cost_survival": weekly_cost,
        "weekly_improves_cost_survival": bool(
            weekly_cost is not None and daily_cost is not None and weekly_cost > daily_cost
        ),
        "weekly_cost_survival_above_0_8": bool(weekly_cost is not None and weekly_cost >= 0.8),
        "best_overall": best_overall.get("variant_id"),
        "best_overall_status": best_overall.get("status"),
        "best_overall_positive_2023_2024_2025": best_overall.get("positive_target_years", {}),
        "best_long_only_variant": best_long_only.get("variant_id"),
        "best_long_only_net_pnl": best_long_only.get("long_only_net_pnl"),
        "best_long_short_net_pnl": best_overall.get("net_pnl"),
        "long_only_better_than_long_short": bool(
            (best_long_only.get("long_only_net_pnl") or -1e9)
            > (best_overall.get("net_pnl") or -1e9)
        ),
        "shorts_killing_best_overall": bool(
            (best_overall.get("long_leg_pnl") or -1e9) > 0.0
            and (best_overall.get("short_leg_pnl") or 1e9) < 0.0
        ),
    }


def build_daily_carry_trend_diagnosis(
    *,
    repo_root: Path,
    symbols: list[str],
    years: list[int],
    extra_slippage_bps: float,
    cost_overrides: dict[str, float],
    json_output: Path,
    csv_output: Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    df, missing, input_summary = daily_lab._load_frames(repo_root, symbols, years)
    symbol_costs = {
        symbol: daily_lab._cost_for_symbol(symbol, cost_overrides) for symbol in symbols
    }
    rows = [
        _evaluate_diagnostic_variant(
            df,
            rebalance_days=rebalance_days,
            hold_days=hold_days,
            basket_size=basket_size,
            rank_signal=rank_signal,
            funding_filter=funding_filter,
            symbol_costs=symbol_costs,
            extra_slippage_bps=extra_slippage_bps,
        )
        for rebalance_days, hold_days, basket_size, rank_signal, funding_filter in product(
            (1, 7), (5, 10, 20), (1, 2, 3), ("ret_14d", "ret_30d"), ("on", "off")
        )
    ]
    rows.sort(key=lambda row: row["score"], reverse=True)
    top = rows[0] if rows else {}
    report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "scope": {
            "family": FAMILY,
            "base_family": BASE_FAMILY,
            "symbols_requested": symbols,
            "symbols_evaluated": sorted(df["symbol"].unique().tolist()),
            "years": years,
            "bar_interval": "1d",
            "direction_modes": ["trend_follow"],
            "rank_signals": ["ret_14d", "ret_30d"],
            "rebalance_days": [1, 7],
            "hold_days": [5, 10, 20],
            "basket_size_per_side": [1, 2, 3],
            "funding_filter": ["on", "off"],
            "returns_include": ["price_pnl", "funding_pnl"],
            "cost_model": "symbol_cost_once_per_rebalance",
            "data_scope": ["OHLCV", "open_interest", "funding"],
            "historical_book_data": "deferred_not_used",
            "extra_slippage_bps": extra_slippage_bps,
            "approval_policy": "diagnostic_research_only_no_paper_or_live",
        },
        "input_summary": input_summary,
        "missing_symbol_data": missing,
        "candidate_count": len(rows),
        "status_counts": dict(Counter(row["status"] for row in rows)),
        "diagnostic_summary": _diagnostic_summary(rows),
        "top_variants": rows[:50],
        "by_year": top.get("by_year", {}),
        "by_symbol": top.get("by_symbol", {}),
        "by_month": top.get("by_month", {}),
        "paper_approved_events": [],
        "live_approved_events": [],
    }
    csv = pd.DataFrame(
        [
            {
                "variant_id": row["variant_id"],
                "rebalance_days": row["rebalance_days"],
                "hold_days": row["hold_days"],
                "basket_size_per_side": row["basket_size_per_side"],
                "direction_mode": row["direction_mode"],
                "rank_signal": row["rank_signal"],
                "funding_filter": row["funding_filter"],
                "event_count": row["event_count"],
                "leg_count": row["leg_count"],
                "gross_pnl": row["gross_pnl"],
                "net_pnl": row["net_pnl"],
                "cost_paid_bps": row["cost_paid_bps"],
                "turnover": row["turnover"],
                "annualized_turnover": row["annualized_turnover"],
                "long_leg_pnl": row["long_leg_pnl"],
                "short_leg_pnl": row["short_leg_pnl"],
                "long_only_net_pnl": row["long_only_net_pnl"],
                "short_only_net_pnl": row["short_only_net_pnl"],
                "price_pnl": row["price_pnl"],
                "funding_pnl": row["funding_pnl"],
                "t_stat": row["t_stat"],
                "gross_t_stat": row["gross_t_stat"],
                "cost_survival": row["cost_survival"],
                "plus_10_bps_net_pnl": row["plus_10_bps_net_pnl"],
                "plus_10_bps_t_stat": row["plus_10_bps_t_stat"],
                "plus_10_bps_survives": row["plus_10_bps_survives"],
                "max_drawdown": row["max_drawdown"],
                "sharpe": row["sharpe"],
                "positive_target_years_pass": row["positive_target_years_pass"],
                "top_symbol_month_share": row["top_symbol_month_share"],
                "by_year": json.dumps(row["by_year"], sort_keys=True),
                "by_symbol": json.dumps(row["by_symbol"], sort_keys=True),
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
    parser = argparse.ArgumentParser(description="Daily carry + trend turnover diagnosis")
    parser.add_argument("--symbols", default=",".join(daily_lab.DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in daily_lab.DEFAULT_YEARS))
    parser.add_argument(
        "--extra-slippage-bps", type=float, default=daily_lab.DEFAULT_EXTRA_SLIPPAGE_BPS
    )
    parser.add_argument("--cost-overrides", default="")
    parser.add_argument(
        "--json-output",
        default=str(DEFAULT_REPORT_DIR / "daily_carry_trend_diagnosis.json"),
    )
    parser.add_argument(
        "--csv-output",
        default=str(DEFAULT_REPORT_DIR / "daily_carry_trend_diagnosis.csv"),
    )
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    report, _ = build_daily_carry_trend_diagnosis(
        repo_root=repo_root,
        symbols=[item.upper() for item in _parse_csv(args.symbols)],
        years=_parse_ints(args.years),
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
                "diagnostic_summary": report["diagnostic_summary"],
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
