from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.scripts import detector_slow_timeframe_lab as slow_lab
from project.scripts.detector_shadow_report import _return_summary
from project.scripts.detector_targeted_expansion import (
    _group_return_stats,
    _max_share,
    _parse_cost_overrides,
)
from project.scripts.detector_tuning_lab import _parse_csv, _parse_ints

DEFAULT_REPORT_DIR = Path("data/reports/detectors/no_liquidations_v1")
DEFAULT_EXTRA_SLIPPAGE_BPS = 10.0
FAMILY = "SLOW_RELATIVE_STRENGTH_DIAGNOSIS"


def _direction_mult(direction: str) -> float:
    return 1.0 if direction == "long" else -1.0


def _variant_id(direction_mode: str, rebalance_bars: int, hold_bars: int, basket_size: int) -> str:
    return (
        f"{FAMILY}__{direction_mode.upper()}__REBALANCE_{rebalance_bars * 4}H__"
        f"HOLD_{hold_bars * 4}H__TOP_BOTTOM_{basket_size}"
    )


def _summarize(values: list[float]) -> dict[str, Any]:
    summary = _return_summary(values)
    return {
        "event_count": len(values),
        "net_bps": summary.get("mean_bps"),
        "t_stat": summary.get("t_stat"),
        "total_bps": float(np.sum(values)) if values else 0.0,
    }


def _empty_summary() -> dict[str, Any]:
    return {"event_count": 0, "net_bps": None, "t_stat": None, "total_bps": 0.0}


def _gate_pass(row: dict[str, Any]) -> bool:
    return (
        int(row.get("event_count") or 0) >= 100
        and (row.get("basket_net") or -1e9) > 0.0
        and (row.get("basket_t_stat") or -1e9) > 2.0
        and (row.get("cost_survival") or -1e9) >= 0.8
        and (row.get("plus_10_bps_net") or -1e9) > 0.0
        and bool(row.get("positive_2023_2024_2025"))
        and (row.get("top_symbol_share") or 1.0) <= 0.35
        and (row.get("top_month_share") or 1.0) <= 0.35
    )


def _status(row: dict[str, Any]) -> str:
    if int(row.get("event_count") or 0) < 100:
        return "needs_sample_expansion"
    if (row.get("basket_net") or -1e9) <= 0.0:
        return "failed_net"
    if (row.get("basket_t_stat") or -1e9) <= 2.0:
        return "failed_t_stat"
    if (row.get("cost_survival") or -1e9) < 0.8:
        return "failed_cost_survival"
    if (row.get("plus_10_bps_net") or -1e9) <= 0.0:
        return "failed_plus_10_bps_slippage"
    if not bool(row.get("positive_2023_2024_2025")):
        return "failed_year_split"
    if (row.get("top_symbol_share") or 1.0) > 0.35:
        return "symbol_dominated_research_only"
    if (row.get("top_month_share") or 1.0) > 0.35:
        return "month_dominated_research_only"
    return "diagnostic_candidate"


def _select_legs(
    ranked: pd.DataFrame, direction_mode: str, basket_size: int
) -> list[tuple[pd.Series, str, str]]:
    bottom = ranked.head(basket_size)
    top = ranked.tail(basket_size).iloc[::-1]
    if direction_mode == "momentum":
        return [(row, "long", "top") for _, row in top.iterrows()] + [
            (row, "short", "bottom") for _, row in bottom.iterrows()
        ]
    if direction_mode == "reversal":
        return [(row, "short", "top") for _, row in top.iterrows()] + [
            (row, "long", "bottom") for _, row in bottom.iterrows()
        ]
    raise ValueError(f"unsupported direction mode: {direction_mode}")


def _evaluate_variant(
    df: pd.DataFrame,
    *,
    direction_mode: str,
    rebalance_bars: int,
    hold_bars: int,
    basket_size: int,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    basket_net: list[float] = []
    basket_gross: list[float] = []
    basket_plus_10: list[float] = []
    long_gross: list[float] = []
    long_net: list[float] = []
    short_gross: list[float] = []
    short_net: list[float] = []
    top_forward: list[float] = []
    bottom_forward: list[float] = []
    leg_details: list[dict[str, Any]] = []
    basket_details: list[dict[str, Any]] = []
    ranking_rows: list[dict[str, Any]] = []
    fwd_column = f"fwd_ret_{hold_bars}x4h"
    for rebalance_idx, (ts, group) in enumerate(df.groupby("timestamp", sort=True)):
        if rebalance_idx % rebalance_bars != 0:
            continue
        valid = group[
            np.isfinite(pd.to_numeric(group["score"], errors="coerce"))
            & np.isfinite(pd.to_numeric(group[fwd_column], errors="coerce"))
            & (
                pd.to_numeric(group["funding_abs_pct"], errors="coerce")
                <= slow_lab.FUNDING_ABS_PCT_MAX
            )
        ].copy()
        if len(valid) < max(slow_lab.MIN_CROSS_SECTION, basket_size * 2):
            continue
        ranked = valid.sort_values("score", ascending=True)
        bottom = ranked.head(basket_size)
        top = ranked.tail(basket_size).iloc[::-1]
        legs = _select_legs(ranked, direction_mode, basket_size)
        leg_gross_values: list[float] = []
        leg_net_values: list[float] = []
        leg_plus_values: list[float] = []
        for row, direction, rank_bucket in legs:
            symbol = str(row["symbol"])
            gross_bps = float(row[fwd_column]) * 10000.0 * _direction_mult(direction)
            cost_bps = float(symbol_costs.get(symbol, 18.0))
            net_bps = gross_bps - cost_bps
            plus_bps = net_bps - extra_slippage_bps
            leg_gross_values.append(gross_bps)
            leg_net_values.append(net_bps)
            leg_plus_values.append(plus_bps)
            if direction == "long":
                long_gross.append(gross_bps)
                long_net.append(net_bps)
            else:
                short_gross.append(gross_bps)
                short_net.append(net_bps)
            leg_details.append(
                {
                    "timestamp": str(ts),
                    "symbol": symbol,
                    "year": str(row["shadow_year"]),
                    "month": str(row["shadow_month"]),
                    "symbol_month": f"{symbol}:{row['shadow_month']}",
                    "direction": direction,
                    "rank_bucket": rank_bucket,
                    "net_bps": net_bps,
                    "gross_bps": gross_bps,
                }
            )
        if len(leg_net_values) != basket_size * 2:
            continue
        basket_net_value = float(np.mean(leg_net_values))
        basket_gross_value = float(np.mean(leg_gross_values))
        basket_plus_value = float(np.mean(leg_plus_values))
        basket_net.append(basket_net_value)
        basket_gross.append(basket_gross_value)
        basket_plus_10.append(basket_plus_value)
        top_fwd = float(pd.to_numeric(top[fwd_column], errors="coerce").mean() * 10000.0)
        bottom_fwd = float(pd.to_numeric(bottom[fwd_column], errors="coerce").mean() * 10000.0)
        top_forward.append(top_fwd)
        bottom_forward.append(bottom_fwd)
        basket_details.append(
            {
                "timestamp": str(ts),
                "year": str(pd.Timestamp(ts).year),
                "month": pd.Timestamp(ts).strftime("%Y-%m"),
                "net_bps": basket_net_value,
                "gross_bps": basket_gross_value,
            }
        )
        ranking_rows.append(
            {
                "timestamp": str(ts),
                "direction_mode": direction_mode,
                "rebalance_hours": rebalance_bars * 4,
                "hold_hours": hold_bars * 4,
                "basket_size": basket_size,
                "top_symbols": ",".join(top["symbol"].astype(str).tolist()),
                "bottom_symbols": ",".join(bottom["symbol"].astype(str).tolist()),
                "top_score_avg": float(pd.to_numeric(top["score"], errors="coerce").mean()),
                "bottom_score_avg": float(pd.to_numeric(bottom["score"], errors="coerce").mean()),
                "top_forward_return": top_fwd,
                "bottom_forward_return": bottom_fwd,
                "basket_forward_return": basket_gross_value,
            }
        )
    row = _row_from_variant(
        direction_mode=direction_mode,
        rebalance_bars=rebalance_bars,
        hold_bars=hold_bars,
        basket_size=basket_size,
        basket_net=basket_net,
        basket_gross=basket_gross,
        basket_plus_10=basket_plus_10,
        long_gross=long_gross,
        long_net=long_net,
        short_gross=short_gross,
        short_net=short_net,
        top_forward=top_forward,
        bottom_forward=bottom_forward,
        leg_details=leg_details,
        basket_details=basket_details,
    )
    return row, ranking_rows


def _row_from_variant(
    *,
    direction_mode: str,
    rebalance_bars: int,
    hold_bars: int,
    basket_size: int,
    basket_net: list[float],
    basket_gross: list[float],
    basket_plus_10: list[float],
    long_gross: list[float],
    long_net: list[float],
    short_gross: list[float],
    short_net: list[float],
    top_forward: list[float],
    bottom_forward: list[float],
    leg_details: list[dict[str, Any]],
    basket_details: list[dict[str, Any]],
) -> dict[str, Any]:
    net_summary = _return_summary(basket_net)
    gross_summary = _return_summary(basket_gross)
    plus_summary = _return_summary(basket_plus_10)
    by_year = _group_return_stats(basket_details, "year")
    by_symbol = _group_return_stats(leg_details, "symbol")
    symbol_counts = dict(Counter(event["symbol"] for event in leg_details))
    month_counts = dict(Counter(event["month"] for event in basket_details))
    net = net_summary.get("mean_bps")
    gross = gross_summary.get("mean_bps")
    row = {
        "variant_id": _variant_id(direction_mode, rebalance_bars, hold_bars, basket_size),
        "family": FAMILY,
        "direction_mode": direction_mode,
        "rebalance_hours": rebalance_bars * 4,
        "hold_hours": hold_bars * 4,
        "basket_size": basket_size,
        "event_count": len(basket_net),
        "leg_count": len(leg_details),
        "basket_gross": gross,
        "basket_net": net,
        "basket_t_stat": net_summary.get("t_stat"),
        "no_cost_gross": gross,
        "with_cost_net": net,
        "plus_10_bps_net": plus_summary.get("mean_bps"),
        "plus_10_bps_t_stat": plus_summary.get("t_stat"),
        "cost_survival": float(net / gross)
        if net is not None and gross is not None and gross > 0.0
        else None,
        "long_leg_gross": _summarize(long_gross),
        "long_leg_net": _summarize(long_net),
        "short_leg_gross": _summarize(short_gross),
        "short_leg_net": _summarize(short_net),
        "top_forward_return": _summarize(top_forward),
        "bottom_forward_return": _summarize(bottom_forward),
        "by_year": by_year,
        "by_symbol": by_symbol,
        "positive_2023_2024_2025": all(
            (by_year.get(year, {}).get("net_bps") or -1e9) > 0.0
            for year in ("2023", "2024", "2025")
        ),
        "top_symbol_share": _max_share(symbol_counts, len(leg_details)),
        "top_month_share": _max_share(month_counts, len(basket_details)),
    }
    row["passes_gates"] = _gate_pass(row)
    row["status"] = _status(row)
    row["score"] = (
        max(0.0, row.get("basket_net") or 0.0)
        + 10.0 * max(0.0, row.get("basket_t_stat") or 0.0)
        + 20.0 * max(0.0, row.get("cost_survival") or 0.0)
        - 1000.0 * float(not row["passes_gates"])
    )
    return row


def build_slow_timeframe_diagnosis(
    *,
    repo_root: Path,
    symbols: list[str],
    years: list[int],
    extra_slippage_bps: float,
    cost_overrides: dict[str, float],
    json_output: Path,
    csv_output: Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    df, missing, input_summary = slow_lab._load_frames(repo_root, symbols, years)
    symbol_costs = {symbol: slow_lab._cost_for_symbol(symbol, cost_overrides) for symbol in symbols}
    rows: list[dict[str, Any]] = []
    ranking_rows: list[dict[str, Any]] = []
    for direction_mode in ("momentum", "reversal"):
        for rebalance_bars in (1, 3, 6):
            for hold_bars in (1, 3, 6):
                for basket_size in (2, 3):
                    row, ranking = _evaluate_variant(
                        df,
                        direction_mode=direction_mode,
                        rebalance_bars=rebalance_bars,
                        hold_bars=hold_bars,
                        basket_size=basket_size,
                        symbol_costs=symbol_costs,
                        extra_slippage_bps=extra_slippage_bps,
                    )
                    rows.append(row)
                    ranking_rows.extend(ranking)
    rows.sort(key=lambda item: item["score"], reverse=True)
    ranking_df = pd.DataFrame(ranking_rows)
    best_momentum = max(
        (row for row in rows if row["direction_mode"] == "momentum"),
        key=lambda row: row.get("basket_net") or -1e9,
        default={},
    )
    best_reversal = max(
        (row for row in rows if row["direction_mode"] == "reversal"),
        key=lambda row: row.get("basket_net") or -1e9,
        default={},
    )
    positive_gross_count = sum(1 for row in rows if (row.get("basket_gross") or -1e9) > 0.0)
    positive_net_count = sum(1 for row in rows if (row.get("basket_net") or -1e9) > 0.0)
    conclusion = (
        "weak_positive_gross_costs_kill"
        if positive_gross_count > 0 and positive_net_count == 0
        else "no_positive_gross_signal"
        if positive_gross_count == 0
        else "positive_net_requires_gate_review"
    )
    report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "scope": {
            "family": FAMILY,
            "source_signal": slow_lab.FAMILY,
            "symbols_requested": symbols,
            "symbols_evaluated": sorted(df["symbol"].unique().tolist()),
            "years": years,
            "bar_interval": slow_lab.BAR_INTERVAL,
            "direction_modes": ["momentum", "reversal"],
            "rebalance_hours": [4, 12, 24],
            "hold_hours": [4, 12, 24],
            "basket_sizes": [2, 3],
            "data_scope": ["OHLCV", "open_interest", "funding"],
            "historical_book_data": "deferred_not_used",
            "extra_slippage_bps": extra_slippage_bps,
            "paper_live_policy": "paper/live approvals remain empty",
        },
        "input_summary": input_summary,
        "missing_symbol_data": missing,
        "candidate_count": len(rows),
        "status_counts": dict(Counter(row["status"] for row in rows)),
        "diagnostic_summary": {
            "positive_gross_count": positive_gross_count,
            "positive_net_count": positive_net_count,
            "best_momentum_variant": best_momentum.get("variant_id"),
            "best_momentum_net_bps": best_momentum.get("basket_net"),
            "best_momentum_gross_bps": best_momentum.get("basket_gross"),
            "best_reversal_variant": best_reversal.get("variant_id"),
            "best_reversal_net_bps": best_reversal.get("basket_net"),
            "best_reversal_gross_bps": best_reversal.get("basket_gross"),
            "conclusion": conclusion,
        },
        "top_variants": rows[:20],
        "ranking_sanity_sample": ranking_df.head(200).to_dict(orient="records")
        if not ranking_df.empty
        else [],
        "paper_approved_events": [],
        "live_approved_events": [],
    }
    csv = pd.DataFrame(
        [
            {
                "variant_id": row["variant_id"],
                "direction_mode": row["direction_mode"],
                "rebalance_hours": row["rebalance_hours"],
                "hold_hours": row["hold_hours"],
                "basket_size": row["basket_size"],
                "event_count": row["event_count"],
                "basket_gross": row["basket_gross"],
                "basket_net": row["basket_net"],
                "basket_t_stat": row["basket_t_stat"],
                "no_cost_gross": row["no_cost_gross"],
                "with_cost_net": row["with_cost_net"],
                "plus_10_bps_net": row["plus_10_bps_net"],
                "plus_10_bps_t_stat": row["plus_10_bps_t_stat"],
                "cost_survival": row["cost_survival"],
                "long_leg_gross": row["long_leg_gross"].get("net_bps"),
                "long_leg_net": row["long_leg_net"].get("net_bps"),
                "short_leg_gross": row["short_leg_gross"].get("net_bps"),
                "short_leg_net": row["short_leg_net"].get("net_bps"),
                "top_forward_return": row["top_forward_return"].get("net_bps"),
                "bottom_forward_return": row["bottom_forward_return"].get("net_bps"),
                "positive_2023_2024_2025": row["positive_2023_2024_2025"],
                "top_symbol_share": row["top_symbol_share"],
                "top_month_share": row["top_month_share"],
                "passes_gates": row["passes_gates"],
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
    ranking_output = csv_output.with_name("slow_relative_strength_ranking_sanity.csv")
    ranking_df.to_csv(ranking_output, index=False)
    return report, csv


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose slow relative-strength rotation")
    parser.add_argument("--symbols", default=",".join(slow_lab.DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in slow_lab.DEFAULT_YEARS))
    parser.add_argument("--extra-slippage-bps", type=float, default=DEFAULT_EXTRA_SLIPPAGE_BPS)
    parser.add_argument("--cost-overrides", default="")
    parser.add_argument(
        "--json-output", default=str(DEFAULT_REPORT_DIR / "slow_timeframe_diagnosis.json")
    )
    parser.add_argument(
        "--csv-output",
        default=str(DEFAULT_REPORT_DIR / "slow_relative_strength_diagnosis.csv"),
    )
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    report, _ = build_slow_timeframe_diagnosis(
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
