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
FAMILY = "DAILY_CARRY_TREND"
BASKET_SIZE_PER_SIDE = 2
MIN_CROSS_SECTION = 6
FUNDING_ABS_PCT_MAX = 95.0
CRASH_VOL_PCT_MAX = 95.0
CRASH_ABS_RET_MAX = 0.20


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
    return _add_daily_features(_to_daily_bars(pooled)), missing, summary


def _to_daily_bars(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values(["symbol", "timestamp"]).reset_index(drop=True).copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out["_day"] = out["timestamp"].dt.floor("1D")
    for column in ("open", "high", "low", "close", "volume", "oi_notional", "funding_rate_scaled"):
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    funding_events = out[
        out["timestamp"].dt.hour.isin([0, 8, 16]) & (out["timestamp"].dt.minute == 0)
    ].copy()
    funding_daily = (
        funding_events.groupby(["symbol", "_day"], sort=True)["funding_rate_scaled"]
        .sum()
        .rename("funding_sum")
        .reset_index()
    )
    bars = (
        out.groupby(["symbol", "_day"], sort=True)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            oi_notional=("oi_notional", "last"),
        )
        .reset_index()
        .merge(funding_daily, on=["symbol", "_day"], how="left")
        .rename(columns={"_day": "timestamp"})
    )
    bars["funding_sum"] = pd.to_numeric(bars["funding_sum"], errors="coerce").fillna(0.0)
    bars["funding_long_bps"] = -bars["funding_sum"] * 10000.0
    bars["funding_short_bps"] = bars["funding_sum"] * 10000.0
    bars = bars.dropna(subset=["timestamp", "close"]).sort_values(["symbol", "timestamp"])
    bars["shadow_year"] = bars["timestamp"].dt.year.astype(str)
    bars["shadow_month"] = bars["timestamp"].dt.strftime("%Y-%m")
    return bars.reset_index(drop=True)


def _rolling_pct_rank(series: pd.Series, *, window: int, min_periods: int) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.rolling(window, min_periods=min_periods).rank(pct=True).fillna(0.0) * 100.0


def _add_daily_features(frame_1d: pd.DataFrame) -> pd.DataFrame:
    out = frame_1d.sort_values(["symbol", "timestamp"]).reset_index(drop=True).copy()
    grouped = out.groupby("symbol", sort=False)
    close = pd.to_numeric(out["close"], errors="coerce")
    oi = pd.to_numeric(out["oi_notional"], errors="coerce")
    out["ret_1d"] = grouped["close"].pct_change()
    out["ret_7d"] = grouped["close"].pct_change(7)
    out["ret_14d"] = grouped["close"].pct_change(14)
    out["ret_30d"] = grouped["close"].pct_change(30)
    out["close_ma_30d"] = grouped["close"].transform(
        lambda series: pd.to_numeric(series, errors="coerce").rolling(30, min_periods=20).mean()
    )
    out["oi_trend"] = grouped["oi_notional"].transform(
        lambda series: np.log(pd.to_numeric(series, errors="coerce").replace(0.0, np.nan)).diff(7)
    )
    out["funding_carry"] = pd.to_numeric(out["funding_long_bps"], errors="coerce")
    out["funding_abs_pct"] = grouped["funding_sum"].transform(
        lambda series: _rolling_pct_rank(series.abs(), window=180, min_periods=30)
    )
    realized_vol_14d = grouped["ret_1d"].transform(
        lambda series: (
            pd.to_numeric(series, errors="coerce").rolling(14, min_periods=7).std() * math.sqrt(14)
        )
    )
    realized_vol_30d = grouped["ret_1d"].transform(
        lambda series: (
            pd.to_numeric(series, errors="coerce").rolling(30, min_periods=15).std() * math.sqrt(30)
        )
    )
    out["realized_vol_14d"] = realized_vol_14d
    out["realized_vol_30d"] = realized_vol_30d
    out["crash_vol_pct"] = realized_vol_14d.groupby(out["symbol"], sort=False).transform(
        lambda series: _rolling_pct_rank(series, window=180, min_periods=30)
    )
    out["crash_filter_ok"] = (
        pd.to_numeric(out["crash_vol_pct"], errors="coerce").fillna(0.0) <= CRASH_VOL_PCT_MAX
    ) & (pd.to_numeric(out["ret_1d"], errors="coerce").abs().fillna(0.0) <= CRASH_ABS_RET_MAX)
    for hold_days in (1, 2, 3, 5, 10, 20):
        out[f"fwd_price_ret_{hold_days}d"] = grouped["close"].shift(-hold_days) / close - 1.0
        out[f"fwd_funding_long_bps_{hold_days}d"] = grouped["funding_long_bps"].transform(
            lambda series, h=hold_days: (
                pd.to_numeric(series, errors="coerce")
                .shift(-1)
                .rolling(h, min_periods=h)
                .sum()
                .shift(-(h - 1))
            )
        )
        out[f"fwd_funding_short_bps_{hold_days}d"] = -out[f"fwd_funding_long_bps_{hold_days}d"]
    out["close"] = close
    out["oi_notional"] = oi
    return out


def _direction_mult(direction: str) -> float:
    return 1.0 if direction == "long" else -1.0


def _rank_score(df: pd.DataFrame, rank_signal: str) -> pd.Series:
    if rank_signal == "ret_7d":
        return pd.to_numeric(df["ret_7d"], errors="coerce")
    if rank_signal == "ret_14d":
        return pd.to_numeric(df["ret_14d"], errors="coerce")
    if rank_signal == "ret_30d":
        return pd.to_numeric(df["ret_30d"], errors="coerce")
    if rank_signal == "ret_14d_vol_adj":
        vol = pd.to_numeric(df["realized_vol_14d"], errors="coerce").replace(0.0, np.nan)
        return pd.to_numeric(df["ret_14d"], errors="coerce") / vol
    if rank_signal == "ret_30d_vol_adj":
        vol = pd.to_numeric(df["realized_vol_30d"], errors="coerce").replace(0.0, np.nan)
        return pd.to_numeric(df["ret_30d"], errors="coerce") / vol
    if rank_signal == "funding_carry":
        return pd.to_numeric(df["funding_carry"], errors="coerce")
    if rank_signal == "oi_trend":
        return pd.to_numeric(df["oi_trend"], errors="coerce")
    raise ValueError(f"unsupported rank signal: {rank_signal}")


def _select_legs(
    ranked: pd.DataFrame, *, direction_mode: str, rank_signal: str
) -> list[tuple[pd.Series, str, str]]:
    bottom = ranked.head(BASKET_SIZE_PER_SIDE)
    top = ranked.tail(BASKET_SIZE_PER_SIDE).iloc[::-1]
    if direction_mode == "trend_follow":
        return [(row, "long", "top") for _, row in top.iterrows()] + [
            (row, "short", "bottom") for _, row in bottom.iterrows()
        ]
    if direction_mode == "carry_aligned":
        carry_ranked = ranked.sort_values("funding_carry", ascending=True)
        funding_payers = carry_ranked.head(BASKET_SIZE_PER_SIDE)
        funding_receivers = carry_ranked.tail(BASKET_SIZE_PER_SIDE).iloc[::-1]
        return [(row, "long", "funding_receiver") for _, row in funding_receivers.iterrows()] + [
            (row, "short", "funding_payer") for _, row in funding_payers.iterrows()
        ]
    if direction_mode == "carry_contra_extreme":
        if rank_signal == "funding_carry":
            return [(row, "long", "contra_funding_payer") for _, row in bottom.iterrows()] + [
                (row, "short", "contra_funding_receiver") for _, row in top.iterrows()
            ]
        return [(row, "short", "top_contra") for _, row in top.iterrows()] + [
            (row, "long", "bottom_contra") for _, row in bottom.iterrows()
        ]
    raise ValueError(f"unsupported direction mode: {direction_mode}")


def _evaluate_variant(
    df: pd.DataFrame,
    *,
    direction_mode: str,
    rank_signal: str,
    hold_days: int,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
) -> dict[str, Any]:
    basket_net: list[float] = []
    basket_gross: list[float] = []
    basket_price: list[float] = []
    basket_funding: list[float] = []
    basket_plus_10: list[float] = []
    leg_details: list[dict[str, Any]] = []
    basket_details: list[dict[str, Any]] = []
    for ts, group in df.groupby("timestamp", sort=True):
        scored = group.copy()
        scored["_score"] = _rank_score(scored, rank_signal)
        valid = scored[
            np.isfinite(pd.to_numeric(scored["_score"], errors="coerce"))
            & np.isfinite(pd.to_numeric(scored[f"fwd_price_ret_{hold_days}d"], errors="coerce"))
            & np.isfinite(
                pd.to_numeric(scored[f"fwd_funding_long_bps_{hold_days}d"], errors="coerce")
            )
            & (pd.to_numeric(scored["funding_abs_pct"], errors="coerce") <= FUNDING_ABS_PCT_MAX)
            & scored["crash_filter_ok"].fillna(False)
        ].copy()
        if len(valid) < max(MIN_CROSS_SECTION, BASKET_SIZE_PER_SIDE * 2):
            continue
        ranked = valid.sort_values("_score", ascending=True)
        legs = _select_legs(ranked, direction_mode=direction_mode, rank_signal=rank_signal)
        leg_net: list[float] = []
        leg_gross: list[float] = []
        leg_price: list[float] = []
        leg_funding: list[float] = []
        leg_plus_10: list[float] = []
        basket_symbols: list[str] = []
        for row, direction, rank_bucket in legs:
            symbol = str(row["symbol"])
            price_bps = (
                float(row[f"fwd_price_ret_{hold_days}d"]) * 10000.0 * _direction_mult(direction)
            )
            funding_bps = float(
                row[f"fwd_funding_long_bps_{hold_days}d"]
                if direction == "long"
                else row[f"fwd_funding_short_bps_{hold_days}d"]
            )
            gross_bps = price_bps + funding_bps
            cost_bps = float(symbol_costs.get(symbol, 18.0))
            net_bps = gross_bps - cost_bps
            plus_10_bps = net_bps - extra_slippage_bps
            leg_price.append(price_bps)
            leg_funding.append(funding_bps)
            leg_gross.append(gross_bps)
            leg_net.append(net_bps)
            leg_plus_10.append(plus_10_bps)
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
                    "rank_signal": rank_signal,
                    "direction_mode": direction_mode,
                    "price_bps": price_bps,
                    "funding_bps": funding_bps,
                    "gross_bps": gross_bps,
                    "net_bps": net_bps,
                }
            )
        if len(leg_net) != BASKET_SIZE_PER_SIDE * 2:
            continue
        basket_net_value = float(np.mean(leg_net))
        basket_gross_value = float(np.mean(leg_gross))
        basket_price_value = float(np.mean(leg_price))
        basket_funding_value = float(np.mean(leg_funding))
        basket_plus_value = float(np.mean(leg_plus_10))
        basket_net.append(basket_net_value)
        basket_gross.append(basket_gross_value)
        basket_price.append(basket_price_value)
        basket_funding.append(basket_funding_value)
        basket_plus_10.append(basket_plus_value)
        basket_details.append(
            {
                "timestamp": str(ts),
                "year": str(pd.Timestamp(ts).year),
                "month": pd.Timestamp(ts).strftime("%Y-%m"),
                "symbols": ",".join(sorted(basket_symbols)),
                "price_bps": basket_price_value,
                "funding_bps": basket_funding_value,
                "gross_bps": basket_gross_value,
                "net_bps": basket_net_value,
            }
        )
    return _row_from_events(
        direction_mode=direction_mode,
        rank_signal=rank_signal,
        hold_days=hold_days,
        basket_net=basket_net,
        basket_gross=basket_gross,
        basket_price=basket_price,
        basket_funding=basket_funding,
        basket_plus_10=basket_plus_10,
        leg_details=leg_details,
        basket_details=basket_details,
    )


def _walk_forward_pass(by_year: dict[str, Any]) -> bool:
    years = sorted(by_year)
    if len(years) < 2:
        return False
    oos_years = years[1:]
    positive = sum(1 for year in oos_years if (by_year[year].get("net_bps") or -1e9) > 0.0)
    return positive >= max(2, math.ceil(0.67 * len(oos_years)))


def _row_from_events(
    *,
    direction_mode: str,
    rank_signal: str,
    hold_days: int,
    basket_net: list[float],
    basket_gross: list[float],
    basket_price: list[float],
    basket_funding: list[float],
    basket_plus_10: list[float],
    leg_details: list[dict[str, Any]],
    basket_details: list[dict[str, Any]],
) -> dict[str, Any]:
    net_summary = _return_summary(basket_net)
    gross_summary = _return_summary(basket_gross)
    price_summary = _return_summary(basket_price)
    funding_summary = _return_summary(basket_funding)
    plus_summary = _return_summary(basket_plus_10)
    by_symbol = _group_return_stats(leg_details, "symbol")
    by_year = _group_return_stats(basket_details, "year")
    by_month = _group_return_stats(basket_details, "month")
    symbol_month_counts = dict(Counter(event["symbol_month"] for event in leg_details))
    symbol_counts = dict(Counter(event["symbol"] for event in leg_details))
    positive_symbols = sorted(
        symbol for symbol, stats in by_symbol.items() if (stats.get("net_bps") or -1e9) > 0.0
    )
    net = net_summary.get("mean_bps")
    gross = gross_summary.get("mean_bps")
    row = {
        "variant_id": (
            f"{FAMILY}__{direction_mode.upper()}__RANK_{rank_signal.upper()}__"
            f"HOLD_{hold_days}D__TOP2_BOTTOM2"
        ),
        "family": FAMILY,
        "direction_mode": direction_mode,
        "rank_signal": rank_signal,
        "hold_days": hold_days,
        "event_count": len(basket_net),
        "leg_count": len(leg_details),
        "best_exit": {
            "policy": f"daily_rebalance_hold_{hold_days}d",
            "net_bps": net,
            "gross_bps": gross,
            "price_bps": price_summary.get("mean_bps"),
            "funding_bps": funding_summary.get("mean_bps"),
            "t_stat": net_summary.get("t_stat"),
            "hit_rate": float(np.mean(np.asarray(basket_net) > 0.0)) if basket_net else None,
            "cost_survival": float(net / gross)
            if net is not None and gross is not None and gross > 0.0
            else None,
            "slippage_plus_10_bps": {
                "net_bps": plus_summary.get("mean_bps"),
                "t_stat": plus_summary.get("t_stat"),
                "survives": (plus_summary.get("mean_bps") or -1e9) > 0.0,
            },
            "n": len(basket_net),
        },
        "by_symbol": by_symbol,
        "by_year": by_year,
        "by_month": by_month,
        "positive_symbols": positive_symbols,
        "top_symbol_month_share": _max_share(symbol_month_counts, len(leg_details)),
        "single_symbol_event_share": _max_share(symbol_counts, len(leg_details)),
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
        - 1000.0 * float(row["status"] != "fresh_daily_carry_trend_candidate")
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
    if not bool((best.get("slippage_plus_10_bps") or {}).get("survives")):
        return "failed_plus_10_bps_slippage"
    if len(row.get("positive_symbols") or []) < 3:
        return "symbol_scoped_research_only"
    if (row.get("top_symbol_month_share") or 0.0) > 0.35:
        return "symbol_month_concentrated_research_only"
    if not bool((row.get("walk_forward") or {}).get("pass")):
        return "walk_forward_failed"
    return "fresh_daily_carry_trend_candidate"


def build_daily_carry_trend_report(
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
    rows: list[dict[str, Any]] = []
    for direction_mode in ("trend_follow", "carry_aligned", "carry_contra_extreme"):
        for rank_signal in ("ret_7d", "ret_14d", "funding_carry", "oi_trend"):
            for hold_days in (1, 2, 3, 5):
                rows.append(  # noqa: PERF401
                    _evaluate_variant(
                        df,
                        direction_mode=direction_mode,
                        rank_signal=rank_signal,
                        hold_days=hold_days,
                        symbol_costs=symbol_costs,
                        extra_slippage_bps=extra_slippage_bps,
                    )
                )
    rows.sort(key=lambda row: row["score"], reverse=True)
    top = rows[0] if rows else {}
    report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "scope": {
            "family": FAMILY,
            "symbols_requested": symbols,
            "symbols_evaluated": sorted(df["symbol"].unique().tolist()),
            "years": years,
            "bar_interval": "1d",
            "hold_days": [1, 2, 3, 5],
            "direction_modes": ["trend_follow", "carry_aligned", "carry_contra_extreme"],
            "rank_signals": ["ret_7d", "ret_14d", "funding_carry", "oi_trend"],
            "portfolio": "long_top_2_short_bottom_2_daily_rebalance",
            "returns_include": ["price_pnl", "funding_pnl"],
            "cost_model": "symbol_cost_once_per_rebalance",
            "data_scope": ["OHLCV", "open_interest", "funding"],
            "historical_book_data": "deferred_not_used",
            "extra_slippage_bps": extra_slippage_bps,
            "cost_bps_by_symbol": symbol_costs,
            "approval_policy": "research_only_outputs_require_fresh_validation_no_paper_or_live",
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
        "slippage_plus_10_bps": (top.get("best_exit") or {}).get("slippage_plus_10_bps", {}),
        "paper_approved_events": [],
        "live_approved_events": [],
    }
    csv = pd.DataFrame(
        [
            {
                "variant_id": row["variant_id"],
                "direction_mode": row["direction_mode"],
                "rank_signal": row["rank_signal"],
                "hold_days": row["hold_days"],
                "event_count": row["event_count"],
                "leg_count": row["leg_count"],
                "net_bps": row["best_exit"].get("net_bps"),
                "gross_bps": row["best_exit"].get("gross_bps"),
                "price_bps": row["best_exit"].get("price_bps"),
                "funding_bps": row["best_exit"].get("funding_bps"),
                "t_stat": row["best_exit"].get("t_stat"),
                "cost_survival": row["best_exit"].get("cost_survival"),
                "hit_rate": row["best_exit"].get("hit_rate"),
                "positive_symbols": ",".join(row["positive_symbols"]),
                "top_symbol_month_share": row["top_symbol_month_share"],
                "single_symbol_event_share": row["single_symbol_event_share"],
                "walk_forward_pass": row["walk_forward"]["pass"],
                "slippage_plus_10_bps_net_bps": row["best_exit"]["slippage_plus_10_bps"].get(
                    "net_bps"
                ),
                "slippage_plus_10_bps_t_stat": row["best_exit"]["slippage_plus_10_bps"].get(
                    "t_stat"
                ),
                "slippage_plus_10_bps_survives": row["best_exit"]["slippage_plus_10_bps"].get(
                    "survives"
                ),
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
    parser = argparse.ArgumentParser(description="Daily carry + trend detector lab")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in DEFAULT_YEARS))
    parser.add_argument("--extra-slippage-bps", type=float, default=DEFAULT_EXTRA_SLIPPAGE_BPS)
    parser.add_argument("--cost-overrides", default="")
    parser.add_argument(
        "--json-output", default=str(DEFAULT_REPORT_DIR / "daily_carry_trend_lab_report.json")
    )
    parser.add_argument(
        "--csv-output", default=str(DEFAULT_REPORT_DIR / "top_daily_carry_trend_variants.csv")
    )
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    report, _ = build_daily_carry_trend_report(
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
