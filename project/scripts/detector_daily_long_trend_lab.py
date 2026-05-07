from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
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
FAMILY = "DAILY_LONG_TREND"
TARGET_YEARS = ("2023", "2024", "2025")
SIGNAL_CONTROL = "top_momentum_long"
CONTROL_TYPES = (
    SIGNAL_CONTROL,
    "bottom_momentum_long_placebo",
    "random_rank_placebo",
    "btc_only_benchmark",
    "equal_weight_universe_benchmark",
)
RANK_SIGNALS = ("ret_14d", "ret_30d", "ret_14d_vol_adj", "ret_30d_vol_adj")
HOLD_DAYS = (5, 10, 20)
BASKET_SIZES = (1, 2, 3)
FUNDING_FILTERS = ("off", "not_extreme")
BTC_REGIMES = ("any", "btc_above_30d_ma")
VOL_REGIMES = ("any", "not_crash_vol")


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
    drawdowns = curve - np.maximum.accumulate(curve)
    return float(np.min(drawdowns))


def _annualized_sharpe(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    arr = np.asarray(values, dtype=float)
    std = float(np.std(arr, ddof=1))
    if std <= 0.0 or not math.isfinite(std):
        return None
    return float(np.mean(arr) / std * math.sqrt(365.0))


def _pnl_share(events: list[dict[str, Any]], key: str) -> float | None:
    totals: defaultdict[str, float] = defaultdict(float)
    for event in events:
        totals[str(event[key])] += float(event["net_bps"])
    if not totals:
        return None
    positive_total = sum(value for value in totals.values() if value > 0.0)
    if positive_total > 0.0:
        return float(
            max((value for value in totals.values() if value > 0.0), default=0.0) / positive_total
        )
    absolute_total = sum(abs(value) for value in totals.values())
    return (
        float(max(abs(value) for value in totals.values()) / absolute_total)
        if absolute_total
        else None
    )


def _pnl_share_from_totals(totals: dict[str, float]) -> float | None:
    if not totals:
        return None
    positive_total = sum(value for value in totals.values() if value > 0.0)
    if positive_total > 0.0:
        return float(
            max((value for value in totals.values() if value > 0.0), default=0.0) / positive_total
        )
    absolute_total = sum(abs(value) for value in totals.values())
    return (
        float(max(abs(value) for value in totals.values()) / absolute_total)
        if absolute_total
        else None
    )


def _group_stats_from_values(groups: dict[str, list[float]]) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    for key, values in groups.items():
        summary = daily_lab._return_summary(values)
        stats[key] = {
            "event_count": len(values),
            "net_bps": summary.get("mean_bps"),
            "t_stat": summary.get("t_stat"),
            "total_net_bps": float(np.sum(np.asarray(values, dtype=float))) if values else 0.0,
        }
    return stats


def _add_btc_regime(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    btc = out[out["symbol"] == "BTCUSDT"][["timestamp", "close", "close_ma_30d"]].copy()
    btc["btc_above_30d_ma"] = pd.to_numeric(btc["close"], errors="coerce") > pd.to_numeric(
        btc["close_ma_30d"], errors="coerce"
    )
    regime = btc[["timestamp", "btc_above_30d_ma"]].drop_duplicates("timestamp")
    out = out.merge(regime, on="timestamp", how="left")
    out["btc_above_30d_ma"] = out["btc_above_30d_ma"].fillna(False).astype(bool)
    return out


def _add_fast_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for rank_signal in RANK_SIGNALS:
        out[f"_score_{rank_signal}"] = daily_lab._rank_score(out, rank_signal)
    for hold_days in HOLD_DAYS:
        out[f"_valid_hold_{hold_days}d"] = np.isfinite(
            pd.to_numeric(out[f"fwd_price_ret_{hold_days}d"], errors="coerce")
        ) & np.isfinite(pd.to_numeric(out[f"fwd_funding_long_bps_{hold_days}d"], errors="coerce"))
    out["_funding_not_extreme"] = (
        pd.to_numeric(out["funding_abs_pct"], errors="coerce") <= daily_lab.FUNDING_ABS_PCT_MAX
    )
    out["_not_crash_vol"] = out["crash_filter_ok"].fillna(False).astype(bool)
    return out


def _random_score(group: pd.DataFrame) -> pd.Series:
    keys = group[["timestamp", "symbol"]].astype(str)
    return pd.util.hash_pandas_object(keys, index=False).astype("uint64")


def _valid_group(
    group: pd.DataFrame,
    *,
    hold_days: int,
    rank_signal: str | None,
    funding_filter: str,
    btc_regime: str,
    vol_regime: str,
) -> pd.DataFrame:
    scored = group.copy()
    if rank_signal is not None:
        scored["_score"] = daily_lab._rank_score(scored, rank_signal)
        score_mask = np.isfinite(pd.to_numeric(scored["_score"], errors="coerce"))
    else:
        scored["_score"] = np.nan
        score_mask = pd.Series(True, index=scored.index)
    valid_mask = (
        score_mask
        & np.isfinite(pd.to_numeric(scored[f"fwd_price_ret_{hold_days}d"], errors="coerce"))
        & np.isfinite(pd.to_numeric(scored[f"fwd_funding_long_bps_{hold_days}d"], errors="coerce"))
    )
    if funding_filter == "not_extreme":
        valid_mask &= (
            pd.to_numeric(scored["funding_abs_pct"], errors="coerce")
            <= daily_lab.FUNDING_ABS_PCT_MAX
        )
    if btc_regime == "btc_above_30d_ma":
        valid_mask &= scored["btc_above_30d_ma"].fillna(False)
    if vol_regime == "not_crash_vol":
        valid_mask &= scored["crash_filter_ok"].fillna(False)
    return scored[valid_mask].copy()


def _select_rows(
    valid: pd.DataFrame,
    *,
    control_type: str,
    basket_size: int,
) -> pd.DataFrame:
    if control_type == SIGNAL_CONTROL:
        return valid.sort_values("_score", ascending=False).head(basket_size)
    if control_type == "bottom_momentum_long_placebo":
        return valid.sort_values("_score", ascending=True).head(basket_size)
    if control_type == "random_rank_placebo":
        randomed = valid.copy()
        randomed["_random_score"] = _random_score(randomed)
        return randomed.sort_values("_random_score", ascending=False).head(basket_size)
    if control_type == "btc_only_benchmark":
        return valid[valid["symbol"] == "BTCUSDT"].head(1)
    if control_type == "equal_weight_universe_benchmark":
        return valid
    raise ValueError(f"unsupported control type: {control_type}")


def _variant_id(
    *,
    control_type: str,
    rank_signal: str,
    hold_days: int,
    basket_size: int,
    funding_filter: str,
    btc_regime: str,
    vol_regime: str,
) -> str:
    return (
        f"{FAMILY}__{control_type.upper()}__RANK_{rank_signal.upper()}__"
        f"HOLD_{hold_days}D__TOP{basket_size}__FUNDING_{funding_filter.upper()}__"
        f"BTC_{btc_regime.upper()}__VOL_{vol_regime.upper()}"
    )


def _core_gate_failure(row: dict[str, Any]) -> str | None:
    if int(row.get("event_count") or 0) < 100:
        return "needs_sample_expansion"
    if (row.get("net_bps") or -1e9) <= 0.0:
        return "failed_net"
    if (row.get("t_stat") or -1e9) <= 2.0:
        return "failed_t_stat"
    if (row.get("plus_10_bps_net_bps") or -1e9) <= 0.0:
        return "failed_plus_10_bps_slippage"
    years = row.get("positive_target_years") or {}
    if not bool(years.get("2023")):
        return "failed_2023_year_split"
    if not bool(years.get("2024")):
        return "failed_2024_year_split"
    if not bool(years.get("2025")):
        return "failed_2025_year_split_needs_diagnosis"
    if (row.get("top_symbol_pnl_share") or 0.0) > 0.35:
        return "failed_top_symbol_pnl_concentration"
    if (row.get("top_month_pnl_share") or 0.0) > 0.35:
        return "failed_top_month_pnl_concentration"
    return None


def _status(row: dict[str, Any]) -> str:
    failure = _core_gate_failure(row)
    if row["control_type"] != SIGNAL_CONTROL:
        return "control_passed_core_gates" if failure is None else f"control_{failure}"
    if failure is not None:
        return failure
    return "long_trend_research_candidate_needs_drawdown_review"


def _evaluate_variant(
    groups: list[tuple[pd.Timestamp, pd.DataFrame]],
    *,
    control_type: str,
    rank_signal: str,
    hold_days: int,
    basket_size: int,
    funding_filter: str,
    btc_regime: str,
    vol_regime: str,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
) -> dict[str, Any]:
    rank_for_valid = (
        None
        if control_type
        in {"random_rank_placebo", "btc_only_benchmark", "equal_weight_universe_benchmark"}
        else rank_signal
    )
    basket_net: list[float] = []
    basket_gross: list[float] = []
    basket_price: list[float] = []
    basket_funding: list[float] = []
    basket_plus_10: list[float] = []
    turnover_values: list[float] = []
    leg_details: list[dict[str, Any]] = []
    basket_details: list[dict[str, Any]] = []
    previous_symbols: set[str] | None = None
    for ts, group in groups:
        valid = _valid_group(
            group,
            hold_days=hold_days,
            rank_signal=rank_for_valid,
            funding_filter=funding_filter,
            btc_regime=btc_regime,
            vol_regime=vol_regime,
        )
        if control_type != "btc_only_benchmark" and len(valid) < daily_lab.MIN_CROSS_SECTION:
            continue
        selected = _select_rows(valid, control_type=control_type, basket_size=basket_size)
        if selected.empty:
            continue
        selected_symbols = set(selected["symbol"].astype(str).tolist())
        if previous_symbols is None:
            turnover = 1.0
        else:
            denominator = max(len(selected_symbols | previous_symbols), 1)
            turnover = float(len(selected_symbols.symmetric_difference(previous_symbols))) / float(
                denominator
            )
        previous_symbols = selected_symbols
        turnover_values.append(turnover)

        leg_net: list[float] = []
        leg_gross: list[float] = []
        leg_price: list[float] = []
        leg_funding: list[float] = []
        leg_plus_10: list[float] = []
        for _, row in selected.iterrows():
            symbol = str(row["symbol"])
            price_bps = float(row[f"fwd_price_ret_{hold_days}d"]) * 10000.0
            funding_bps = float(row[f"fwd_funding_long_bps_{hold_days}d"])
            gross_bps = price_bps + funding_bps
            cost_bps = float(symbol_costs.get(symbol, 18.0))
            net_bps = gross_bps - cost_bps
            plus_10_bps = net_bps - extra_slippage_bps
            leg_net.append(net_bps)
            leg_gross.append(gross_bps)
            leg_price.append(price_bps)
            leg_funding.append(funding_bps)
            leg_plus_10.append(plus_10_bps)
            leg_details.append(
                {
                    "timestamp": str(ts),
                    "symbol": symbol,
                    "year": str(row["shadow_year"]),
                    "month": str(row["shadow_month"]),
                    "net_bps": net_bps,
                    "gross_bps": gross_bps,
                    "price_bps": price_bps,
                    "funding_bps": funding_bps,
                }
            )
        net_value = float(np.mean(leg_net))
        gross_value = float(np.mean(leg_gross))
        price_value = float(np.mean(leg_price))
        funding_value = float(np.mean(leg_funding))
        plus_10_value = float(np.mean(leg_plus_10))
        basket_net.append(net_value)
        basket_gross.append(gross_value)
        basket_price.append(price_value)
        basket_funding.append(funding_value)
        basket_plus_10.append(plus_10_value)
        basket_details.append(
            {
                "timestamp": str(ts),
                "year": str(pd.Timestamp(ts).year),
                "month": pd.Timestamp(ts).strftime("%Y-%m"),
                "net_bps": net_value,
                "gross_bps": gross_value,
                "price_bps": price_value,
                "funding_bps": funding_value,
            }
        )

    net_summary = daily_lab._return_summary(basket_net)
    gross_summary = daily_lab._return_summary(basket_gross)
    plus_summary = daily_lab._return_summary(basket_plus_10)
    price_pnl = _finite_mean(basket_price)
    funding_pnl = _finite_mean(basket_funding)
    pnl_denom = abs(price_pnl or 0.0) + abs(funding_pnl or 0.0)
    by_symbol = daily_lab._group_return_stats(leg_details, "symbol")
    by_year = daily_lab._group_return_stats(basket_details, "year")
    by_month = daily_lab._group_return_stats(basket_details, "month")
    target_year_positive = {
        year: (by_year.get(year, {}).get("net_bps") or -1e9) > 0.0 for year in TARGET_YEARS
    }
    row = {
        "variant_id": _variant_id(
            control_type=control_type,
            rank_signal=rank_signal,
            hold_days=hold_days,
            basket_size=basket_size,
            funding_filter=funding_filter,
            btc_regime=btc_regime,
            vol_regime=vol_regime,
        ),
        "family": FAMILY,
        "control_type": control_type,
        "rank_signal": rank_signal,
        "hold_days": hold_days,
        "basket_size": basket_size,
        "funding_filter": funding_filter,
        "btc_regime": btc_regime,
        "vol_regime": vol_regime,
        "event_count": len(basket_net),
        "leg_count": len(leg_details),
        "exposure_days": len(basket_net) * hold_days,
        "leg_exposure_days": len(leg_details) * hold_days,
        "gross_bps": gross_summary.get("mean_bps"),
        "net_bps": net_summary.get("mean_bps"),
        "price_pnl": price_pnl,
        "funding_pnl": funding_pnl,
        "price_pnl_share": float(abs(price_pnl or 0.0) / pnl_denom) if pnl_denom > 0.0 else None,
        "funding_pnl_share": float(abs(funding_pnl or 0.0) / pnl_denom)
        if pnl_denom > 0.0
        else None,
        "t_stat": net_summary.get("t_stat"),
        "gross_t_stat": gross_summary.get("t_stat"),
        "hit_rate": float(np.mean(np.asarray(basket_net) > 0.0)) if basket_net else None,
        "plus_10_bps_net_bps": plus_summary.get("mean_bps"),
        "plus_10_bps_t_stat": plus_summary.get("t_stat"),
        "annualized_sharpe": _annualized_sharpe(basket_net),
        "max_drawdown": _max_drawdown(basket_net),
        "turnover": _finite_mean(turnover_values),
        "by_symbol": by_symbol,
        "by_year": by_year,
        "by_month": by_month,
        "positive_target_years": target_year_positive,
        "top_symbol_pnl_share": _pnl_share(leg_details, "symbol"),
        "top_month_pnl_share": _pnl_share(basket_details, "month"),
        "paper_approved": False,
        "live_approved": False,
    }
    row["status"] = _status(row)
    row["passes_core_gates"] = _core_gate_failure(row) is None
    row["score"] = (
        max(0.0, row.get("net_bps") or 0.0)
        + 10.0 * max(0.0, row.get("t_stat") or 0.0)
        + 10.0 * max(0.0, row.get("annualized_sharpe") or 0.0)
        - 1000.0 * float(row["control_type"] != SIGNAL_CONTROL)
        - 1000.0 * float(row["status"] != "long_trend_research_candidate_needs_drawdown_review")
    )
    return row


def _best(rows: list[dict[str, Any]], control_type: str) -> dict[str, Any]:
    candidates = [row for row in rows if row["control_type"] == control_type]
    if not candidates:
        return {}
    return max(candidates, key=lambda row: (row.get("net_bps") or -1e9, row.get("t_stat") or -1e9))


def _control_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    best_signal = _best(rows, SIGNAL_CONTROL)
    signal_candidates = [
        row
        for row in rows
        if row["control_type"] == SIGNAL_CONTROL
        and row["status"] == "long_trend_research_candidate_needs_drawdown_review"
    ]
    best_signal_candidate = (
        max(
            signal_candidates,
            key=lambda row: (row.get("net_bps") or -1e9, row.get("t_stat") or -1e9),
        )
        if signal_candidates
        else {}
    )
    control_rows = [row for row in rows if row["control_type"] != SIGNAL_CONTROL]
    controls = {
        control: _best(rows, control) for control in CONTROL_TYPES if control != SIGNAL_CONTROL
    }
    best_control = max(
        (row for row in controls.values() if row),
        key=lambda row: (row.get("net_bps") or -1e9, row.get("t_stat") or -1e9),
        default={},
    )
    control_pass_count = sum(1 for row in control_rows if row.get("passes_core_gates"))
    controls_fail = control_pass_count == 0
    return {
        "best_signal": best_signal.get("variant_id"),
        "best_signal_net_bps": best_signal.get("net_bps"),
        "best_signal_t_stat": best_signal.get("t_stat"),
        "best_signal_status": best_signal.get("status"),
        "best_signal_candidate": best_signal_candidate.get("variant_id"),
        "best_signal_candidate_net_bps": best_signal_candidate.get("net_bps"),
        "best_signal_candidate_t_stat": best_signal_candidate.get("t_stat"),
        "best_control": best_control.get("variant_id"),
        "best_control_type": best_control.get("control_type"),
        "best_control_net_bps": best_control.get("net_bps"),
        "best_control_t_stat": best_control.get("t_stat"),
        "controls_fail_core_gates": controls_fail,
        "control_pass_count": control_pass_count,
        "passed_control_variants": [
            row["variant_id"] for row in control_rows if row.get("passes_core_gates")
        ][:20],
        "signal_beats_best_control_net": bool(
            (best_signal.get("net_bps") or -1e9) > (best_control.get("net_bps") or -1e9)
        ),
        "signal_beats_controls": bool(
            controls_fail
            and (best_signal.get("net_bps") or -1e9) > (best_control.get("net_bps") or -1e9)
            and (best_signal.get("t_stat") or -1e9) > (best_control.get("t_stat") or -1e9)
        ),
        "best_by_control_type": {
            control: {
                "variant_id": row.get("variant_id"),
                "net_bps": row.get("net_bps"),
                "t_stat": row.get("t_stat"),
                "status": row.get("status"),
            }
            for control, row in controls.items()
        },
    }


def _control_grid() -> list[tuple[str, str, int, int, str, str, str]]:
    rows: list[tuple[str, str, int, int, str, str, str]] = []
    for control_type in (SIGNAL_CONTROL, "bottom_momentum_long_placebo"):
        rows.extend(
            (
                control_type,
                rank_signal,
                hold_days,
                basket_size,
                funding_filter,
                btc_regime,
                vol_regime,
            )
            for rank_signal, hold_days, basket_size, funding_filter, btc_regime, vol_regime in product(
                RANK_SIGNALS, HOLD_DAYS, BASKET_SIZES, FUNDING_FILTERS, BTC_REGIMES, VOL_REGIMES
            )
        )
    rows.extend(
        (
            "random_rank_placebo",
            "random_hash",
            hold_days,
            basket_size,
            funding_filter,
            btc_regime,
            vol_regime,
        )
        for hold_days, basket_size, funding_filter, btc_regime, vol_regime in product(
            HOLD_DAYS, BASKET_SIZES, FUNDING_FILTERS, BTC_REGIMES, VOL_REGIMES
        )
    )
    for control_type in ("btc_only_benchmark", "equal_weight_universe_benchmark"):
        rows.extend(
            (control_type, control_type, hold_days, 1, funding_filter, btc_regime, vol_regime)
            for hold_days, funding_filter, btc_regime, vol_regime in product(
                HOLD_DAYS, FUNDING_FILTERS, BTC_REGIMES, VOL_REGIMES
            )
        )
    return rows


def _state() -> dict[str, Any]:
    return {
        "basket_net": [],
        "basket_gross": [],
        "basket_price": [],
        "basket_funding": [],
        "basket_plus_10": [],
        "turnover": [],
        "symbol_returns": defaultdict(list),
        "symbol_pnl": defaultdict(float),
        "year_returns": defaultdict(list),
        "month_returns": defaultdict(list),
        "month_pnl": defaultdict(float),
        "previous_symbols": None,
    }


def _append_selection(
    states: dict[tuple[str, str, int, int, str, str, str], dict[str, Any]],
    key: tuple[str, str, int, int, str, str, str],
    *,
    ts: pd.Timestamp,
    selected: pd.DataFrame,
    hold_days: int,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
) -> None:
    if selected.empty:
        return
    current = states[key]
    selected_symbols = set(selected["symbol"].astype(str).tolist())
    previous_symbols = current["previous_symbols"]
    if previous_symbols is None:
        turnover = 1.0
    else:
        denominator = max(len(selected_symbols | previous_symbols), 1)
        turnover = float(len(selected_symbols.symmetric_difference(previous_symbols))) / float(
            denominator
        )
    current["previous_symbols"] = selected_symbols
    current["turnover"].append(turnover)

    leg_net: list[float] = []
    leg_gross: list[float] = []
    leg_price: list[float] = []
    leg_funding: list[float] = []
    leg_plus_10: list[float] = []
    for _, row in selected.iterrows():
        symbol = str(row["symbol"])
        price_bps = float(row[f"fwd_price_ret_{hold_days}d"]) * 10000.0
        funding_bps = float(row[f"fwd_funding_long_bps_{hold_days}d"])
        gross_bps = price_bps + funding_bps
        cost_bps = float(symbol_costs.get(symbol, 18.0))
        net_bps = gross_bps - cost_bps
        plus_10_bps = net_bps - extra_slippage_bps
        leg_net.append(net_bps)
        leg_gross.append(gross_bps)
        leg_price.append(price_bps)
        leg_funding.append(funding_bps)
        leg_plus_10.append(plus_10_bps)
        current["symbol_returns"][symbol].append(net_bps)
        current["symbol_pnl"][symbol] += net_bps
    net_value = float(np.mean(leg_net))
    gross_value = float(np.mean(leg_gross))
    price_value = float(np.mean(leg_price))
    funding_value = float(np.mean(leg_funding))
    plus_10_value = float(np.mean(leg_plus_10))
    current["basket_net"].append(net_value)
    current["basket_gross"].append(gross_value)
    current["basket_price"].append(price_value)
    current["basket_funding"].append(funding_value)
    current["basket_plus_10"].append(plus_10_value)
    year = str(pd.Timestamp(ts).year)
    month = pd.Timestamp(ts).strftime("%Y-%m")
    current["year_returns"][year].append(net_value)
    current["month_returns"][month].append(net_value)
    current["month_pnl"][month] += net_value


def _row_from_state(
    key: tuple[str, str, int, int, str, str, str], state: dict[str, Any]
) -> dict[str, Any]:
    control_type, rank_signal, hold_days, basket_size, funding_filter, btc_regime, vol_regime = key
    basket_net = state["basket_net"]
    basket_gross = state["basket_gross"]
    basket_price = state["basket_price"]
    basket_funding = state["basket_funding"]
    basket_plus_10 = state["basket_plus_10"]
    net_summary = daily_lab._return_summary(basket_net)
    gross_summary = daily_lab._return_summary(basket_gross)
    plus_summary = daily_lab._return_summary(basket_plus_10)
    price_pnl = _finite_mean(basket_price)
    funding_pnl = _finite_mean(basket_funding)
    pnl_denom = abs(price_pnl or 0.0) + abs(funding_pnl or 0.0)
    by_symbol = _group_stats_from_values(state["symbol_returns"])
    by_year = _group_stats_from_values(state["year_returns"])
    by_month = _group_stats_from_values(state["month_returns"])
    target_year_positive = {
        year: (by_year.get(year, {}).get("net_bps") or -1e9) > 0.0 for year in TARGET_YEARS
    }
    row = {
        "variant_id": _variant_id(
            control_type=control_type,
            rank_signal=rank_signal,
            hold_days=hold_days,
            basket_size=basket_size,
            funding_filter=funding_filter,
            btc_regime=btc_regime,
            vol_regime=vol_regime,
        ),
        "family": FAMILY,
        "control_type": control_type,
        "rank_signal": rank_signal,
        "hold_days": hold_days,
        "basket_size": basket_size,
        "funding_filter": funding_filter,
        "btc_regime": btc_regime,
        "vol_regime": vol_regime,
        "event_count": len(basket_net),
        "leg_count": sum(len(values) for values in state["symbol_returns"].values()),
        "exposure_days": len(basket_net) * hold_days,
        "leg_exposure_days": sum(len(values) for values in state["symbol_returns"].values())
        * hold_days,
        "gross_bps": gross_summary.get("mean_bps"),
        "net_bps": net_summary.get("mean_bps"),
        "price_pnl": price_pnl,
        "funding_pnl": funding_pnl,
        "price_pnl_share": float(abs(price_pnl or 0.0) / pnl_denom) if pnl_denom > 0.0 else None,
        "funding_pnl_share": float(abs(funding_pnl or 0.0) / pnl_denom)
        if pnl_denom > 0.0
        else None,
        "t_stat": net_summary.get("t_stat"),
        "gross_t_stat": gross_summary.get("t_stat"),
        "hit_rate": float(np.mean(np.asarray(basket_net) > 0.0)) if basket_net else None,
        "plus_10_bps_net_bps": plus_summary.get("mean_bps"),
        "plus_10_bps_t_stat": plus_summary.get("t_stat"),
        "annualized_sharpe": _annualized_sharpe(basket_net),
        "max_drawdown": _max_drawdown(basket_net),
        "turnover": _finite_mean(state["turnover"]),
        "by_symbol": by_symbol,
        "by_year": by_year,
        "by_month": by_month,
        "positive_target_years": target_year_positive,
        "top_symbol_pnl_share": _pnl_share_from_totals(state["symbol_pnl"]),
        "top_month_pnl_share": _pnl_share_from_totals(state["month_pnl"]),
        "paper_approved": False,
        "live_approved": False,
    }
    row["status"] = _status(row)
    row["passes_core_gates"] = _core_gate_failure(row) is None
    row["score"] = (
        max(0.0, row.get("net_bps") or 0.0)
        + 10.0 * max(0.0, row.get("t_stat") or 0.0)
        + 10.0 * max(0.0, row.get("annualized_sharpe") or 0.0)
        - 1000.0 * float(row["control_type"] != SIGNAL_CONTROL)
        - 1000.0 * float(row["status"] != "long_trend_research_candidate_needs_drawdown_review")
    )
    return row


def _evaluate_grid_fast(
    groups: list[tuple[pd.Timestamp, pd.DataFrame]],
    *,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
) -> list[dict[str, Any]]:
    grid = _control_grid()
    states = {key: _state() for key in grid}
    for hold_days, funding_filter, btc_regime, vol_regime in product(
        HOLD_DAYS, FUNDING_FILTERS, BTC_REGIMES, VOL_REGIMES
    ):
        for ts, group in groups:
            base_mask = group[f"_valid_hold_{hold_days}d"]
            if funding_filter == "not_extreme":
                base_mask &= group["_funding_not_extreme"]
            if btc_regime == "btc_above_30d_ma":
                base_mask &= group["btc_above_30d_ma"]
            if vol_regime == "not_crash_vol":
                base_mask &= group["_not_crash_vol"]
            base_valid = group.loc[base_mask]
            if not base_valid.empty:
                for basket_size in BASKET_SIZES:
                    random_key = (
                        "random_rank_placebo",
                        "random_hash",
                        hold_days,
                        basket_size,
                        funding_filter,
                        btc_regime,
                        vol_regime,
                    )
                    randomed = base_valid.copy()
                    randomed["_random_score"] = _random_score(randomed)
                    _append_selection(
                        states,
                        random_key,
                        ts=ts,
                        selected=randomed.sort_values("_random_score", ascending=False).head(
                            basket_size
                        ),
                        hold_days=hold_days,
                        symbol_costs=symbol_costs,
                        extra_slippage_bps=extra_slippage_bps,
                    )
                for control_type in ("btc_only_benchmark", "equal_weight_universe_benchmark"):
                    key = (
                        control_type,
                        control_type,
                        hold_days,
                        1,
                        funding_filter,
                        btc_regime,
                        vol_regime,
                    )
                    selected = (
                        base_valid[base_valid["symbol"] == "BTCUSDT"].head(1)
                        if control_type == "btc_only_benchmark"
                        else base_valid
                    )
                    _append_selection(
                        states,
                        key,
                        ts=ts,
                        selected=selected,
                        hold_days=hold_days,
                        symbol_costs=symbol_costs,
                        extra_slippage_bps=extra_slippage_bps,
                    )
            for rank_signal in RANK_SIGNALS:
                score_col = f"_score_{rank_signal}"
                ranked_valid = base_valid.loc[
                    np.isfinite(pd.to_numeric(base_valid[score_col], errors="coerce"))
                ]
                if len(ranked_valid) < daily_lab.MIN_CROSS_SECTION:
                    continue
                ranked_desc = ranked_valid.sort_values(score_col, ascending=False)
                ranked_asc = ranked_valid.sort_values(score_col, ascending=True)
                for basket_size in BASKET_SIZES:
                    for control_type, selected in (
                        (SIGNAL_CONTROL, ranked_desc.head(basket_size)),
                        ("bottom_momentum_long_placebo", ranked_asc.head(basket_size)),
                    ):
                        key = (
                            control_type,
                            rank_signal,
                            hold_days,
                            basket_size,
                            funding_filter,
                            btc_regime,
                            vol_regime,
                        )
                        _append_selection(
                            states,
                            key,
                            ts=ts,
                            selected=selected,
                            hold_days=hold_days,
                            symbol_costs=symbol_costs,
                            extra_slippage_bps=extra_slippage_bps,
                        )
    return [_row_from_state(key, states[key]) for key in grid]


def build_daily_long_trend_report(
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
    df = _add_fast_columns(_add_btc_regime(df))
    groups = [(pd.Timestamp(ts), group.copy()) for ts, group in df.groupby("timestamp", sort=True)]
    symbol_costs = {
        symbol: daily_lab._cost_for_symbol(symbol, cost_overrides) for symbol in symbols
    }
    rows = _evaluate_grid_fast(
        groups, symbol_costs=symbol_costs, extra_slippage_bps=extra_slippage_bps
    )
    signal_rows = [row for row in rows if row["control_type"] == SIGNAL_CONTROL]
    signal_rows.sort(key=lambda row: row["score"], reverse=True)
    rows.sort(key=lambda row: (row["control_type"] != SIGNAL_CONTROL, row["score"]), reverse=True)
    top_signal = signal_rows[0] if signal_rows else {}
    report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "scope": {
            "family": FAMILY,
            "symbols_requested": symbols,
            "symbols_evaluated": sorted(df["symbol"].unique().tolist()),
            "years": years,
            "bar_interval": "1d",
            "portfolio": "long_only_daily_rebalance",
            "basket_sizes": list(BASKET_SIZES),
            "rank_signals": list(RANK_SIGNALS),
            "hold_days": list(HOLD_DAYS),
            "funding_filter": list(FUNDING_FILTERS),
            "btc_regime": list(BTC_REGIMES),
            "vol_regime": list(VOL_REGIMES),
            "controls": list(CONTROL_TYPES[1:]),
            "returns_include": ["price_pnl", "funding_pnl", "costs"],
            "data_scope": ["OHLCV", "open_interest", "funding"],
            "historical_book_data": "deferred_not_used",
            "approval_policy": "research_only_outputs_no_paper_or_live",
        },
        "input_summary": input_summary,
        "missing_symbol_data": missing,
        "candidate_count": len(rows),
        "signal_candidate_count": len(signal_rows),
        "status_counts": dict(Counter(row["status"] for row in rows)),
        "control_summary": _control_summary(rows),
        "top_signal_variants": signal_rows[:50],
        "top_control_variants": [row for row in rows if row["control_type"] != SIGNAL_CONTROL][:50],
        "top_variants": rows[:50],
        "by_symbol": top_signal.get("by_symbol", {}),
        "by_year": top_signal.get("by_year", {}),
        "by_month": top_signal.get("by_month", {}),
        "paper_approved_events": [],
        "live_approved_events": [],
    }
    csv = pd.DataFrame(
        [
            {
                "variant_id": row["variant_id"],
                "control_type": row["control_type"],
                "rank_signal": row["rank_signal"],
                "hold_days": row["hold_days"],
                "basket_size": row["basket_size"],
                "funding_filter": row["funding_filter"],
                "btc_regime": row["btc_regime"],
                "vol_regime": row["vol_regime"],
                "event_count": row["event_count"],
                "leg_count": row["leg_count"],
                "exposure_days": row["exposure_days"],
                "leg_exposure_days": row["leg_exposure_days"],
                "gross_bps": row["gross_bps"],
                "net_bps": row["net_bps"],
                "price_pnl": row["price_pnl"],
                "funding_pnl": row["funding_pnl"],
                "price_pnl_share": row["price_pnl_share"],
                "funding_pnl_share": row["funding_pnl_share"],
                "t_stat": row["t_stat"],
                "gross_t_stat": row["gross_t_stat"],
                "hit_rate": row["hit_rate"],
                "plus_10_bps_net_bps": row["plus_10_bps_net_bps"],
                "plus_10_bps_t_stat": row["plus_10_bps_t_stat"],
                "annualized_sharpe": row["annualized_sharpe"],
                "max_drawdown": row["max_drawdown"],
                "turnover": row["turnover"],
                "top_symbol_pnl_share": row["top_symbol_pnl_share"],
                "top_month_pnl_share": row["top_month_pnl_share"],
                "positive_target_years": json.dumps(row["positive_target_years"], sort_keys=True),
                "by_symbol": json.dumps(row["by_symbol"], sort_keys=True),
                "by_year": json.dumps(row["by_year"], sort_keys=True),
                "by_month": json.dumps(row["by_month"], sort_keys=True),
                "passes_core_gates": row["passes_core_gates"],
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
    parser = argparse.ArgumentParser(description="Daily long-only trend detector lab")
    parser.add_argument("--symbols", default=",".join(daily_lab.DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in daily_lab.DEFAULT_YEARS))
    parser.add_argument(
        "--extra-slippage-bps", type=float, default=daily_lab.DEFAULT_EXTRA_SLIPPAGE_BPS
    )
    parser.add_argument("--cost-overrides", default="")
    parser.add_argument(
        "--json-output", default=str(DEFAULT_REPORT_DIR / "daily_long_trend_lab_report.json")
    )
    parser.add_argument(
        "--csv-output", default=str(DEFAULT_REPORT_DIR / "top_daily_long_trend_variants.csv")
    )
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    report, _ = build_daily_long_trend_report(
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
                "signal_candidate_count": report["signal_candidate_count"],
                "status_counts": report["status_counts"],
                "control_summary": report["control_summary"],
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
