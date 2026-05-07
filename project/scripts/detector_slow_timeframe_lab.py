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

from project.scripts.detector_shadow_report import _prepare_symbol_frame, _return_summary
from project.scripts.detector_targeted_expansion import (
    DEFAULT_COST_BPS_BY_SYMBOL,
    _group_return_stats,
    _max_share,
    _parse_cost_overrides,
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
DEFAULT_EXTRA_SLIPPAGE_BPS = 10.0
FAMILY = "SLOW_RELATIVE_STRENGTH_ROTATION"
BAR_INTERVAL = "4h"
MOMENTUM_24H_BARS = 6
MOMENTUM_72H_BARS = 18
VOL_LOOKBACK_BARS = 18
HORIZON_BARS = 1
BASKET_SIZE_PER_SIDE = 2
MIN_CROSS_SECTION = 6
FUNDING_ABS_PCT_MAX = 90.0


def _cost_for_symbol(symbol: str, overrides: dict[str, float]) -> float:
    return float(
        overrides.get(symbol.upper(), DEFAULT_COST_BPS_BY_SYMBOL.get(symbol.upper(), 18.0))
    )


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
            "rows_5m": len(frame),
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
    return _add_slow_features(_to_4h_bars(pooled)), missing, summary


def _to_4h_bars(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values(["symbol", "timestamp"]).reset_index(drop=True).copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out["_bar_ts"] = out["timestamp"].dt.floor(BAR_INTERVAL)
    for column in ("open", "high", "low", "close", "volume", "oi_notional", "funding_rate_scaled"):
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    bars = (
        out.groupby(["symbol", "_bar_ts"], sort=True)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            oi_notional=("oi_notional", "last"),
            funding_rate_scaled=("funding_rate_scaled", "last"),
        )
        .reset_index()
        .rename(columns={"_bar_ts": "timestamp"})
    )
    bars = bars.dropna(subset=["timestamp", "close"]).sort_values(["symbol", "timestamp"])
    bars["shadow_year"] = bars["timestamp"].dt.year.astype(str)
    bars["shadow_month"] = bars["timestamp"].dt.strftime("%Y-%m")
    return bars.reset_index(drop=True)


def _add_slow_features(frame_4h: pd.DataFrame) -> pd.DataFrame:
    out = frame_4h.sort_values(["symbol", "timestamp"]).reset_index(drop=True).copy()
    grouped = out.groupby("symbol", sort=False)
    close = pd.to_numeric(out["close"], errors="coerce")
    out["ret_24h"] = grouped["close"].pct_change(MOMENTUM_24H_BARS)
    out["ret_72h"] = grouped["close"].pct_change(MOMENTUM_72H_BARS)
    out["realized_vol_72h"] = grouped["close"].transform(
        lambda series: (
            pd.to_numeric(series, errors="coerce")
            .pct_change()
            .rolling(VOL_LOOKBACK_BARS, min_periods=6)
            .std()
            * math.sqrt(VOL_LOOKBACK_BARS)
        )
    )
    out["score"] = (out["ret_24h"] + out["ret_72h"]) / out["realized_vol_72h"].replace(0.0, np.nan)
    out["funding_abs_pct"] = grouped["funding_rate_scaled"].transform(
        lambda series: (
            pd.to_numeric(series, errors="coerce")
            .abs()
            .rolling(180, min_periods=30)
            .rank(pct=True)
            .fillna(0.0)
            * 100.0
        )
    )
    out["fwd_ret_4h"] = grouped["close"].shift(-HORIZON_BARS) / close - 1.0
    return out


def _walk_forward_pass(by_year: dict[str, Any]) -> bool:
    years = sorted(by_year)
    if len(years) < 2:
        return False
    oos_years = years[1:]
    if len(oos_years) < 2:
        return all((by_year[year].get("net_bps") or -1e9) > 0.0 for year in oos_years)
    positive = sum(1 for year in oos_years if (by_year[year].get("net_bps") or -1e9) > 0.0)
    return positive >= max(2, math.ceil(0.67 * len(oos_years)))


def _evaluate_rotation(
    df: pd.DataFrame, *, symbol_costs: dict[str, float], extra_slippage_bps: float
) -> dict[str, Any]:
    returns: list[float] = []
    gross_returns: list[float] = []
    plus_slippage_returns: list[float] = []
    basket_details: list[dict[str, Any]] = []
    leg_details: list[dict[str, Any]] = []
    for ts, group in df.groupby("timestamp", sort=True):
        valid = group[
            np.isfinite(pd.to_numeric(group["score"], errors="coerce"))
            & np.isfinite(pd.to_numeric(group["fwd_ret_4h"], errors="coerce"))
            & (pd.to_numeric(group["funding_abs_pct"], errors="coerce") <= FUNDING_ABS_PCT_MAX)
        ].copy()
        if len(valid) < max(MIN_CROSS_SECTION, BASKET_SIZE_PER_SIDE * 2):
            continue
        ranked = valid.sort_values("score", ascending=True)
        shorts = ranked.head(BASKET_SIZE_PER_SIDE)
        longs = ranked.tail(BASKET_SIZE_PER_SIDE).iloc[::-1]
        legs = [(row, "long", "top_2") for _, row in longs.iterrows()] + [
            (row, "short", "bottom_2") for _, row in shorts.iterrows()
        ]
        leg_net: list[float] = []
        leg_gross: list[float] = []
        leg_plus_slippage: list[float] = []
        basket_symbols: list[str] = []
        for row, direction, rank_bucket in legs:
            symbol = str(row["symbol"])
            fwd_ret = float(row["fwd_ret_4h"])
            direction_mult = 1.0 if direction == "long" else -1.0
            gross_bps = fwd_ret * 10000.0 * direction_mult
            cost_bps = float(symbol_costs.get(symbol, 18.0))
            net_bps = gross_bps - cost_bps
            plus_10_bps = net_bps - extra_slippage_bps
            leg_gross.append(gross_bps)
            leg_net.append(net_bps)
            leg_plus_slippage.append(plus_10_bps)
            basket_symbols.append(symbol)
            leg_details.append(
                {
                    "timestamp": str(ts),
                    "symbol": symbol,
                    "year": str(row["shadow_year"]),
                    "month": str(row["shadow_month"]),
                    "symbol_month": f"{symbol}:{row['shadow_month']}",
                    "direction": direction,
                    "rank_bucket": rank_bucket,
                    "score": float(row["score"]),
                    "net_bps": net_bps,
                    "gross_bps": gross_bps,
                }
            )
        if len(leg_net) != BASKET_SIZE_PER_SIDE * 2:
            continue
        basket_net = float(np.mean(leg_net))
        basket_gross = float(np.mean(leg_gross))
        basket_plus_slippage = float(np.mean(leg_plus_slippage))
        returns.append(basket_net)
        gross_returns.append(basket_gross)
        plus_slippage_returns.append(basket_plus_slippage)
        basket_details.append(
            {
                "timestamp": str(ts),
                "year": str(pd.Timestamp(ts).year),
                "month": pd.Timestamp(ts).strftime("%Y-%m"),
                "symbols": ",".join(sorted(basket_symbols)),
                "net_bps": basket_net,
                "gross_bps": basket_gross,
            }
        )
    return _row_from_events(
        returns=returns,
        gross_returns=gross_returns,
        plus_slippage_returns=plus_slippage_returns,
        basket_details=basket_details,
        leg_details=leg_details,
        extra_slippage_bps=extra_slippage_bps,
    )


def _row_from_events(
    *,
    returns: list[float],
    gross_returns: list[float],
    plus_slippage_returns: list[float],
    basket_details: list[dict[str, Any]],
    leg_details: list[dict[str, Any]],
    extra_slippage_bps: float,
) -> dict[str, Any]:
    del extra_slippage_bps
    net_summary = _return_summary(returns)
    gross_summary = _return_summary(gross_returns)
    plus_slippage_summary = _return_summary(plus_slippage_returns)
    net = net_summary.get("mean_bps")
    gross = gross_summary.get("mean_bps")
    by_symbol = _group_return_stats(leg_details, "symbol")
    by_year = _group_return_stats(basket_details, "year")
    by_month = _group_return_stats(basket_details, "month")
    symbol_month_counts = dict(Counter(event["symbol_month"] for event in leg_details))
    positive_symbols = sorted(
        symbol for symbol, stats in by_symbol.items() if (stats.get("net_bps") or -1e9) > 0.0
    )
    row = {
        "variant_id": "SLOW_RELATIVE_STRENGTH_ROTATION__4H__TOP2_BOTTOM2__MOM_24H_72H_VOL_ADJ__NO_EXTREME_FUNDING",
        "family": FAMILY,
        "event_count": len(returns),
        "leg_count": len(leg_details),
        "params": {
            "bar_interval": BAR_INTERVAL,
            "momentum_lookbacks": ["24h", "72h"],
            "score": "(ret_24h + ret_72h) / realized_vol_72h",
            "basket": "long_top_2_short_bottom_2",
            "rebalance": "every_4h",
            "horizon": "next_4h",
            "funding_filter": f"funding_abs_percentile <= {FUNDING_ABS_PCT_MAX:g}",
        },
        "best_exit": {
            "policy": "rebalance_every_4h_hold_next_4h",
            "net_bps": net,
            "gross_bps": gross,
            "t_stat": net_summary.get("t_stat"),
            "hit_rate": float(np.mean(np.asarray(returns) > 0.0)) if returns else None,
            "cost_survival": float(net / gross)
            if net is not None and gross is not None and gross > 0.0
            else None,
            "slippage_plus_10_bps": {
                "net_bps": plus_slippage_summary.get("mean_bps"),
                "t_stat": plus_slippage_summary.get("t_stat"),
                "survives": (plus_slippage_summary.get("mean_bps") or -1e9) > 0.0,
            },
            "n": len(returns),
        },
        "by_symbol": by_symbol,
        "by_year": by_year,
        "by_month": by_month,
        "positive_symbols": positive_symbols,
        "top_symbol_month_share": _max_share(symbol_month_counts, len(leg_details)),
        "walk_forward": {"pass": _walk_forward_pass(by_year), "by_year": by_year},
        "paper_approved": False,
        "live_approved": False,
    }
    row["status"] = _status(row)
    row["score"] = (
        max(0.0, row["best_exit"].get("net_bps") or 0.0)
        + 10.0 * max(0.0, row["best_exit"].get("t_stat") or 0.0)
        + 20.0 * max(0.0, row["best_exit"].get("cost_survival") or 0.0)
        + 5.0 * len(row["positive_symbols"])
    )
    return row


def _status(row: dict[str, Any]) -> str:
    best = row.get("best_exit") or {}
    if int(row.get("event_count") or 0) < 100:
        return "needs_sample_expansion"
    if (best.get("net_bps") or -1e9) <= 0.0:
        return "failed_net"
    if (best.get("t_stat") or -1e9) <= 2.0:
        return "failed_t_stat"
    if (best.get("cost_survival") or -1e9) < 0.8:
        return "failed_cost_survival"
    if len(row.get("positive_symbols") or []) < 3:
        return "symbol_scoped_research_only"
    if (row.get("top_symbol_month_share") or 0.0) > 0.35:
        return "symbol_month_concentrated_research_only"
    if not bool((row.get("walk_forward") or {}).get("pass")):
        return "walk_forward_failed"
    if not bool((best.get("slippage_plus_10_bps") or {}).get("survives")):
        return "failed_plus_10_bps_slippage"
    return "fresh_slow_timeframe_candidate"


def build_slow_timeframe_report(
    *,
    repo_root: Path,
    symbols: list[str],
    years: list[int],
    extra_slippage_bps: float,
    cost_overrides: dict[str, float],
    json_output: Path,
    csv_output: Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    df, missing, input_summary = _load_frames(repo_root, symbols, years)
    symbol_costs = {symbol: _cost_for_symbol(symbol, cost_overrides) for symbol in symbols}
    row = _evaluate_rotation(
        df,
        symbol_costs=symbol_costs,
        extra_slippage_bps=extra_slippage_bps,
    )
    rows = [row]
    report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "scope": {
            "family": FAMILY,
            "symbols_requested": symbols,
            "symbols_evaluated": sorted(df["symbol"].unique().tolist()),
            "years": years,
            "bar_interval": BAR_INTERVAL,
            "signal_count": 1,
            "data_scope": ["OHLCV", "open_interest", "funding"],
            "historical_book_data": "deferred_not_used",
            "cost_bps_by_symbol": symbol_costs,
            "extra_slippage_bps": extra_slippage_bps,
            "approval_policy": "research_only_outputs_require_fresh_validation_no_paper_or_live",
        },
        "input_summary": input_summary,
        "missing_symbol_data": missing,
        "candidate_count": len(rows),
        "status_counts": dict(Counter(item["status"] for item in rows)),
        "top_variants": rows,
        "by_symbol": row["by_symbol"],
        "by_year": row["by_year"],
        "by_month": row["by_month"],
        "top_symbol_month_share": row["top_symbol_month_share"],
        "walk_forward": row["walk_forward"],
        "slippage_plus_10_bps": row["best_exit"]["slippage_plus_10_bps"],
        "paper_approved_events": [],
        "live_approved_events": [],
    }
    csv = pd.DataFrame(
        [
            {
                "variant_id": item["variant_id"],
                "event_count": item["event_count"],
                "leg_count": item["leg_count"],
                "net_bps": item["best_exit"].get("net_bps"),
                "gross_bps": item["best_exit"].get("gross_bps"),
                "t_stat": item["best_exit"].get("t_stat"),
                "cost_survival": item["best_exit"].get("cost_survival"),
                "hit_rate": item["best_exit"].get("hit_rate"),
                "positive_symbols": ",".join(item["positive_symbols"]),
                "top_symbol_month_share": item["top_symbol_month_share"],
                "walk_forward_pass": item["walk_forward"]["pass"],
                "slippage_plus_10_bps_net_bps": item["best_exit"]["slippage_plus_10_bps"].get(
                    "net_bps"
                ),
                "slippage_plus_10_bps_t_stat": item["best_exit"]["slippage_plus_10_bps"].get(
                    "t_stat"
                ),
                "slippage_plus_10_bps_survives": item["best_exit"]["slippage_plus_10_bps"].get(
                    "survives"
                ),
                "status": item["status"],
                "score": item["score"],
            }
            for item in rows
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
    parser = argparse.ArgumentParser(description="Slow 4h relative-strength rotation lab")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in DEFAULT_YEARS))
    parser.add_argument("--extra-slippage-bps", type=float, default=DEFAULT_EXTRA_SLIPPAGE_BPS)
    parser.add_argument("--cost-overrides", default="")
    parser.add_argument(
        "--json-output", default=str(DEFAULT_REPORT_DIR / "slow_timeframe_lab_report.json")
    )
    parser.add_argument(
        "--csv-output", default=str(DEFAULT_REPORT_DIR / "top_slow_timeframe_variants.csv")
    )
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    report, _ = build_slow_timeframe_report(
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
