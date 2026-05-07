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
    _max_abs_pnl_share,
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
DEFAULT_LOOKBACKS = (12, 48)
DEFAULT_HORIZONS = (12, 24, 48)
DEFAULT_BASKET_SIZE = 2
DEFAULT_REBALANCE_MINUTES = 60
DEFAULT_MIN_CROSS_SECTION = 6
DEFAULT_EXTRA_SLIPPAGE_BPS = 10.0
FAMILY = "CROSS_SECTIONAL_PERP_MOMENTUM"


def _cost_for_symbol(symbol: str, overrides: dict[str, float]) -> float:
    return float(
        overrides.get(symbol.upper(), DEFAULT_COST_BPS_BY_SYMBOL.get(symbol.upper(), 18.0))
    )


def _add_cross_sectional_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values(["symbol", "timestamp"]).reset_index(drop=True).copy()
    close = pd.to_numeric(out["close"], errors="coerce")
    volume = pd.to_numeric(out.get("volume", 0.0), errors="coerce").fillna(0.0)
    oi = pd.to_numeric(out.get("oi_notional", np.nan), errors="coerce")
    funding = pd.to_numeric(out.get("funding_rate_scaled", 0.0), errors="coerce").fillna(0.0)
    grouped = out.groupby("symbol", sort=False)
    for lookback in DEFAULT_LOOKBACKS:
        out[f"ret_{lookback}"] = grouped["close"].pct_change(lookback)
        out[f"oi_chg_{lookback}"] = grouped["oi_notional"].transform(
            lambda series, lb=lookback: np.log(
                pd.to_numeric(series, errors="coerce").replace(0.0, np.nan)
            ).diff(lb)
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
    out["funding_abs_pct"] = grouped["funding_rate_scaled"].transform(
        lambda series: (
            pd.to_numeric(series, errors="coerce")
            .abs()
            .rolling(2880, min_periods=288)
            .rank(pct=True)
            .fillna(0.0)
            * 100.0
        )
    )
    out["shadow_year"] = out["timestamp"].dt.year.astype(str)
    out["shadow_month"] = out["timestamp"].dt.strftime("%Y-%m")
    out["close"] = close
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
    return _add_cross_sectional_features(pooled), missing, summary


def _direction_mult(direction: str) -> float:
    return 1.0 if direction == "long" else -1.0


def _add_forward_returns(df: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    out = df.sort_values(["symbol", "timestamp"]).reset_index(drop=True).copy()
    grouped = out.groupby("symbol", sort=False)
    close = pd.to_numeric(out["close"], errors="coerce")
    for horizon in horizons:
        out[f"fwd_ret_{horizon}"] = grouped["close"].shift(-horizon) / close - 1.0
    return out


def _rebalance_groups(
    df: pd.DataFrame, *, rebalance_minutes: int, min_cross_section: int
) -> list[tuple[pd.Timestamp, np.ndarray]]:
    rebalance = df[df["timestamp"].dt.minute.mod(rebalance_minutes) == 0]
    groups: list[tuple[pd.Timestamp, np.ndarray]] = []
    for ts, group in rebalance.groupby("timestamp", sort=True):
        if len(group) >= min_cross_section:
            groups.append((pd.Timestamp(ts), group.index.to_numpy(dtype=int)))
    return groups


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
    if not bool(best.get("slippage_plus_10_bps_survives")):
        return "failed_plus_10_bps_slippage"
    return "fresh_validation_candidate"


def _evaluate_variant(
    df: pd.DataFrame,
    *,
    groups: list[tuple[pd.Timestamp, np.ndarray]],
    symbol_costs: dict[str, float],
    lookback: int,
    horizon: int,
    rank_mode: str,
    funding_filter: str,
    oi_filter: str,
    volume_filter: str,
    basket_size: int,
    rebalance_minutes: int,
    min_cross_section: int,
    extra_slippage_bps: float,
) -> dict[str, Any]:
    score_values = pd.to_numeric(df[f"ret_{lookback}"], errors="coerce").to_numpy(dtype=float)
    close_values = pd.to_numeric(df["close"], errors="coerce").to_numpy(dtype=float)
    fwd_values = pd.to_numeric(df[f"fwd_ret_{horizon}"], errors="coerce").to_numpy(dtype=float)
    funding_abs_pct = (
        pd.to_numeric(df["funding_abs_pct"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    )
    oi_change = (
        pd.to_numeric(df[f"oi_chg_{lookback}"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    )
    volume_z = pd.to_numeric(df["volume_z"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    symbols = df["symbol"].astype(str).to_numpy()
    years = df["shadow_year"].astype(str).to_numpy()
    months = df["shadow_month"].astype(str).to_numpy()
    returns: list[float] = []
    gross_returns: list[float] = []
    plus_slippage_returns: list[float] = []
    basket_details: list[dict[str, Any]] = []
    leg_details: list[dict[str, Any]] = []
    for ts, group_indices in groups:
        valid = group_indices[
            np.isfinite(score_values[group_indices]) & np.isfinite(close_values[group_indices])
        ]
        if len(valid) < min_cross_section or len(valid) < basket_size * 2:
            continue
        order = valid[np.argsort(score_values[valid])]
        low_indices = order[:basket_size]
        high_indices = order[-basket_size:][::-1]
        if rank_mode == "momentum":
            legs = [(int(idx), "long", "top_rank") for idx in high_indices] + [
                (int(idx), "short", "bottom_rank") for idx in low_indices
            ]
        elif rank_mode == "reversal":
            legs = [(int(idx), "short", "top_rank") for idx in high_indices] + [
                (int(idx), "long", "bottom_rank") for idx in low_indices
            ]
        else:
            raise ValueError(f"unsupported rank mode: {rank_mode}")
        if funding_filter == "not_crowded" and any(
            funding_abs_pct[idx] > 90.0 for idx, _direction, _rank_bucket in legs
        ):
            continue
        if oi_filter == "aligned" and any(
            oi_change[idx] <= 0.0 for idx, _direction, _rank_bucket in legs
        ):
            continue
        if volume_filter == "waking" and any(
            volume_z[idx] < 1.0 for idx, _direction, _rank_bucket in legs
        ):
            continue
        leg_net: list[float] = []
        leg_gross: list[float] = []
        leg_plus_slippage: list[float] = []
        basket_symbols: list[str] = []
        for idx, direction, rank_bucket in legs:
            fwd = fwd_values[idx]
            if not np.isfinite(float(fwd)):
                continue
            symbol = str(symbols[idx])
            gross_bps = float(fwd) * 10000.0 * _direction_mult(direction)
            cost_bps = float(symbol_costs.get(symbol, 18.0))
            net_bps = gross_bps - cost_bps
            plus_10_bps = gross_bps - cost_bps - extra_slippage_bps
            leg_gross.append(gross_bps)
            leg_net.append(net_bps)
            leg_plus_slippage.append(plus_10_bps)
            basket_symbols.append(symbol)
            leg_details.append(
                {
                    "symbol": symbol,
                    "year": str(years[idx]),
                    "month": str(months[idx]),
                    "symbol_month": f"{symbol}:{months[idx]}",
                    "direction": direction,
                    "rank_bucket": rank_bucket,
                    "net_bps": net_bps,
                    "gross_bps": gross_bps,
                }
            )
        if len(leg_net) != basket_size * 2:
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
                "year": str(ts.year),
                "month": ts.strftime("%Y-%m"),
                "symbols": ",".join(sorted(basket_symbols)),
                "net_bps": basket_net,
                "gross_bps": basket_gross,
            }
        )
    net_summary = _return_summary(returns)
    gross_summary = _return_summary(gross_returns)
    plus_slippage_summary = _return_summary(plus_slippage_returns)
    net = net_summary.get("mean_bps")
    gross = gross_summary.get("mean_bps")
    by_symbol = _group_return_stats(leg_details, "symbol")
    by_year = _group_return_stats(basket_details, "year")
    by_month = _group_return_stats(basket_details, "month")
    symbol_month_counts = dict(Counter(event["symbol_month"] for event in leg_details))
    symbol_counts = dict(Counter(event["symbol"] for event in leg_details))
    row = {
        "variant_id": (
            f"{FAMILY}__{rank_mode.upper()}__RET_{lookback}B__H_{horizon}B__"
            f"FUNDING_{funding_filter.upper()}__OI_{oi_filter.upper()}__VOL_{volume_filter.upper()}"
        ),
        "family": FAMILY,
        "rank_mode": rank_mode,
        "event_count": len(returns),
        "leg_count": len(leg_details),
        "params": {
            "lookback_bars": lookback,
            "horizon_bars": horizon,
            "basket_size_per_side": basket_size,
            "funding_filter": funding_filter,
            "oi_filter": oi_filter,
            "volume_filter": volume_filter,
            "rebalance_minutes": rebalance_minutes,
            "min_cross_section": min_cross_section,
        },
        "best_exit": {
            "policy": f"market_neutral_hold_{horizon}b",
            "net_bps": net,
            "gross_bps": gross,
            "t_stat": net_summary.get("t_stat"),
            "hit_rate": float(sum(1 for value in returns if value > 0.0) / len(returns))
            if returns
            else None,
            "cost_survival": float(net / gross)
            if net is not None and gross is not None and gross > 0.0
            else None,
            "slippage_plus_10_bps_net_bps": plus_slippage_summary.get("mean_bps"),
            "slippage_plus_10_bps_t_stat": plus_slippage_summary.get("t_stat"),
            "slippage_plus_10_bps_survives": (plus_slippage_summary.get("mean_bps") or -(10**9))
            > 0.0,
            "n": len(returns),
        },
        "positive_symbols": sorted(
            symbol
            for symbol, stats in by_symbol.items()
            if (stats.get("net_bps") or -(10**9)) > 0.0
        ),
        "by_symbol_net_bps": by_symbol,
        "by_year_net_bps": by_year,
        "by_month_net_bps": by_month,
        "top_symbol_month_share": _max_share(symbol_month_counts, len(leg_details)),
        "single_symbol_event_share": _max_share(symbol_counts, len(leg_details)),
        "by_month_pnl_concentration": _max_abs_pnl_share(by_month),
        "walk_forward_pass": _walk_forward_pass(by_year),
        "paper_approved": False,
        "live_approved": False,
    }
    row["status"] = _status(row)
    best = row["best_exit"]
    row["score"] = (
        max(0.0, best.get("net_bps") or 0.0)
        + 10.0 * max(0.0, best.get("t_stat") or 0.0)
        + 20.0 * max(0.0, best.get("cost_survival") or 0.0)
        + 5.0 * len(row["positive_symbols"])
    )
    return row


def build_cross_sectional_report(
    *,
    repo_root: Path,
    symbols: list[str],
    years: list[int],
    lookbacks: list[int],
    horizons: list[int],
    basket_size: int,
    min_cross_section: int,
    rebalance_minutes: int,
    extra_slippage_bps: float,
    cost_overrides: dict[str, float],
    json_output: Path,
    csv_output: Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    raw, missing, input_summary = _load_frames(repo_root, symbols, years)
    df = _add_forward_returns(raw, horizons)
    groups = _rebalance_groups(
        df, rebalance_minutes=rebalance_minutes, min_cross_section=min_cross_section
    )
    symbol_costs = {symbol: _cost_for_symbol(symbol, cost_overrides) for symbol in symbols}
    rows: list[dict[str, Any]] = []
    for lookback in lookbacks:
        if lookback not in DEFAULT_LOOKBACKS:
            raise ValueError(f"unsupported lookback {lookback}; supported: {DEFAULT_LOOKBACKS}")
        for horizon in horizons:
            for rank_mode in ("momentum", "reversal"):
                for funding_filter in ("none", "not_crowded"):
                    for oi_filter in ("none", "aligned"):
                        for volume_filter in ("none", "waking"):
                            rows.append(  # noqa: PERF401
                                _evaluate_variant(
                                    df,
                                    groups=groups,
                                    symbol_costs=symbol_costs,
                                    lookback=lookback,
                                    horizon=horizon,
                                    rank_mode=rank_mode,
                                    funding_filter=funding_filter,
                                    oi_filter=oi_filter,
                                    volume_filter=volume_filter,
                                    basket_size=basket_size,
                                    min_cross_section=min_cross_section,
                                    rebalance_minutes=rebalance_minutes,
                                    extra_slippage_bps=extra_slippage_bps,
                                )
                            )
    rows.sort(key=lambda item: item["score"], reverse=True)
    report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "scope": {
            "family": FAMILY,
            "symbols_requested": symbols,
            "symbols_evaluated": sorted(raw["symbol"].unique().tolist()),
            "years": years,
            "timeframe": "5m",
            "lookbacks": lookbacks,
            "horizons": horizons,
            "basket_size_per_side": basket_size,
            "min_cross_section": min_cross_section,
            "rebalance_minutes": rebalance_minutes,
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
        "paper_approved_events": [],
        "live_approved_events": [],
    }
    csv = pd.DataFrame(
        [
            {
                "variant_id": row["variant_id"],
                "rank_mode": row["rank_mode"],
                "event_count": row["event_count"],
                "leg_count": row["leg_count"],
                "positive_symbols": ",".join(row["positive_symbols"]),
                "net_bps": row["best_exit"].get("net_bps"),
                "gross_bps": row["best_exit"].get("gross_bps"),
                "t_stat": row["best_exit"].get("t_stat"),
                "cost_survival": row["best_exit"].get("cost_survival"),
                "hit_rate": row["best_exit"].get("hit_rate"),
                "slippage_plus_10_bps_net_bps": row["best_exit"].get(
                    "slippage_plus_10_bps_net_bps"
                ),
                "slippage_plus_10_bps_t_stat": row["best_exit"].get("slippage_plus_10_bps_t_stat"),
                "slippage_plus_10_bps_survives": row["best_exit"].get(
                    "slippage_plus_10_bps_survives"
                ),
                "top_symbol_month_share": row["top_symbol_month_share"],
                "single_symbol_event_share": row["single_symbol_event_share"],
                "by_month_pnl_concentration": row["by_month_pnl_concentration"],
                "walk_forward_pass": row["walk_forward_pass"],
                "lookback_bars": row["params"]["lookback_bars"],
                "horizon_bars": row["params"]["horizon_bars"],
                "basket_size_per_side": row["params"]["basket_size_per_side"],
                "funding_filter": row["params"]["funding_filter"],
                "oi_filter": row["params"]["oi_filter"],
                "volume_filter": row["params"]["volume_filter"],
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
    parser = argparse.ArgumentParser(
        description="Cross-sectional perp momentum/reversal detector lab"
    )
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in DEFAULT_YEARS))
    parser.add_argument("--lookbacks", default=",".join(str(value) for value in DEFAULT_LOOKBACKS))
    parser.add_argument("--horizons", default=",".join(str(value) for value in DEFAULT_HORIZONS))
    parser.add_argument("--basket-size", type=int, default=DEFAULT_BASKET_SIZE)
    parser.add_argument("--min-cross-section", type=int, default=DEFAULT_MIN_CROSS_SECTION)
    parser.add_argument("--rebalance-minutes", type=int, default=DEFAULT_REBALANCE_MINUTES)
    parser.add_argument("--extra-slippage-bps", type=float, default=DEFAULT_EXTRA_SLIPPAGE_BPS)
    parser.add_argument("--cost-overrides", default="")
    parser.add_argument(
        "--json-output", default=str(DEFAULT_REPORT_DIR / "cross_sectional_lab_report.json")
    )
    parser.add_argument(
        "--csv-output", default=str(DEFAULT_REPORT_DIR / "top_cross_sectional_variants.csv")
    )
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    report, _ = build_cross_sectional_report(
        repo_root=repo_root,
        symbols=[item.upper() for item in _parse_csv(args.symbols)],
        years=_parse_ints(args.years),
        lookbacks=_parse_ints(args.lookbacks),
        horizons=_parse_ints(args.horizons),
        basket_size=args.basket_size,
        min_cross_section=args.min_cross_section,
        rebalance_minutes=args.rebalance_minutes,
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
