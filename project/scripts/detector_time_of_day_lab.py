from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.scripts import detector_session_lab as session_lab
from project.scripts.detector_targeted_expansion import _parse_cost_overrides
from project.scripts.detector_tuning_lab import _parse_csv, _parse_ints

DEFAULT_SYMBOLS = session_lab.DEFAULT_SYMBOLS
DEFAULT_YEARS = session_lab.DEFAULT_YEARS
DEFAULT_REPORT_DIR = Path("data/reports/detectors/no_liquidations_v1")
DEFAULT_HOURS = (0, 4, 8, 12, 16, 20)
DEFAULT_EXIT_POLICIES = session_lab.DEFAULT_EXIT_POLICIES
DEFAULT_EXTRA_SLIPPAGE_BPS = 10.0
FAMILY = "TIME_OF_DAY_DRIFT"
FUNDING_HOURS = (0, 8, 16)
FUNDING_DIRECTION_MODES = {"funding_fade", "funding_sign"}
PLACEBO_SEED = 20260507
CONTROL_NAMES = (
    "same_hour_next_day",
    "same_hour_previous_day",
    "neighbor_hour_minus_1",
    "neighbor_hour_plus_1",
    "inverted_direction",
    "randomized_funding_sign",
    "non_funding_hours",
)


def _add_timing_features(df: pd.DataFrame) -> pd.DataFrame:
    if {"session", "volume_z", "oi_change_12", "trend_regime"}.issubset(df.columns):
        out = df.sort_values(["symbol", "timestamp"]).reset_index(drop=True).copy()
    else:
        out = session_lab._add_session_features(df)
    close = pd.to_numeric(out["close"], errors="coerce")
    out["prev_ret_12"] = out.groupby("symbol", sort=False)["close"].transform(
        lambda series: pd.to_numeric(series, errors="coerce").pct_change(12)
    )
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out["hour"] = out["timestamp"].dt.hour.astype(int)
    out["minute"] = out["timestamp"].dt.minute.astype(int)
    out["close"] = close
    return out


def _load_frames(
    repo_root: Path, symbols: list[str], years: list[int]
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    raw, missing, summary = session_lab._load_frames(repo_root, symbols, years)
    return _add_timing_features(raw), missing, summary


def _array_context(df: pd.DataFrame) -> dict[str, Any]:
    timestamps = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    symbols = df["symbol"].astype(str).to_numpy()
    symbol_positions = {
        symbol: np.flatnonzero(symbols == symbol) for symbol in sorted(set(symbols.tolist()))
    }
    return {
        "timestamp": timestamps.astype(str).to_numpy(dtype=object),
        "timestamp_ns": timestamps.astype("int64").to_numpy(dtype=np.int64),
        "close": pd.to_numeric(df["close"], errors="coerce").to_numpy(dtype=float),
        "volume_z": pd.to_numeric(df["volume_z"], errors="coerce")
        .fillna(0.0)
        .to_numpy(dtype=float),
        "oi_change": pd.to_numeric(df["oi_change_12"], errors="coerce")
        .fillna(0.0)
        .to_numpy(dtype=float),
        "prev_ret_12": pd.to_numeric(df["prev_ret_12"], errors="coerce")
        .fillna(0.0)
        .to_numpy(dtype=float),
        "funding_sign": df["funding_sign"].astype(str).to_numpy(),
        "trend": df["trend_regime"].astype(str).to_numpy(),
        "session": df["session"].astype(str).to_numpy(),
        "hour": pd.to_numeric(df["hour"], errors="coerce").fillna(-1).to_numpy(dtype=int),
        "minute": pd.to_numeric(df["minute"], errors="coerce").fillna(-1).to_numpy(dtype=int),
        "symbols": symbols,
        "years": df["shadow_year"].astype(str).to_numpy(),
        "months": df["shadow_month"].astype(str).to_numpy(),
        "eval_indices": np.flatnonzero(timestamps.dt.minute.to_numpy(dtype=int) == 0),
        "symbol_positions": symbol_positions,
    }


def _direction_for_mode(
    context: dict[str, Any], indices: np.ndarray, mode: str
) -> tuple[np.ndarray, np.ndarray]:
    funding = context["funding_sign"][indices]
    trend = context["trend"][indices]
    momentum = context["prev_ret_12"][indices]
    directions = np.full(len(indices), "long", dtype=object)
    valid = np.ones(len(indices), dtype=bool)
    if mode == "funding_fade":
        valid = (funding == "positive") | (funding == "negative")
        directions = np.where(funding == "negative", "long", "short").astype(object)
    elif mode == "funding_sign":
        valid = (funding == "positive") | (funding == "negative")
        directions = np.where(funding == "positive", "long", "short").astype(object)
    elif mode == "trend_follow":
        valid = (trend == "uptrend") | (trend == "downtrend")
        directions = np.where(trend == "uptrend", "long", "short").astype(object)
    elif mode == "previous_12bar_momentum":
        valid = np.isfinite(momentum) & (momentum != 0.0)
        directions = np.where(momentum > 0.0, "long", "short").astype(object)
    else:
        raise ValueError(f"unsupported direction mode: {mode}")
    return valid, directions


def _filter_indices(
    context: dict[str, Any],
    indices: np.ndarray,
    *,
    hour: int | None,
    session: str,
    direction_mode: str,
    volume_mode: str,
    oi_mode: str,
    trend_filter: str,
    non_funding_hours_only: bool = False,
) -> tuple[np.ndarray, np.ndarray]:
    mask = (
        np.isfinite(context["close"][indices])
        & (context["close"][indices] > 0.0)
        & (context["session"][indices] == session)
    )
    if hour is not None:
        mask &= context["hour"][indices] == hour
    if non_funding_hours_only:
        mask &= ~np.isin(context["hour"][indices], FUNDING_HOURS)
    valid_direction, directions = _direction_for_mode(context, indices, direction_mode)
    mask &= valid_direction
    if volume_mode == "required":
        mask &= context["volume_z"][indices] >= 1.0
    elif volume_mode != "optional":
        raise ValueError(f"unsupported volume mode: {volume_mode}")
    if oi_mode == "aligned":
        mask &= context["oi_change"][indices] > 0.0
    elif oi_mode != "optional":
        raise ValueError(f"unsupported OI mode: {oi_mode}")
    if trend_filter == "aligned":
        long_trade = directions == "long"
        aligned = (long_trade & (context["trend"][indices] == "uptrend")) | (
            ~long_trade & (context["trend"][indices] == "downtrend")
        )
        mask &= aligned
    elif trend_filter != "optional":
        raise ValueError(f"unsupported trend filter: {trend_filter}")
    return indices[mask], directions[mask]


def _base_events(
    context: dict[str, Any],
    *,
    hour: int | None,
    session: str,
    direction_mode: str,
    volume_mode: str,
    oi_mode: str,
    trend_filter: str,
    non_funding_hours_only: bool = False,
) -> tuple[np.ndarray, np.ndarray]:
    return _filter_indices(
        context,
        context["eval_indices"],
        hour=hour,
        session=session,
        direction_mode=direction_mode,
        volume_mode=volume_mode,
        oi_mode=oi_mode,
        trend_filter=trend_filter,
        non_funding_hours_only=non_funding_hours_only,
    )


def _shift_indices_same_symbol(
    context: dict[str, Any], indices: np.ndarray, *, bars: int
) -> np.ndarray:
    if len(indices) == 0:
        return np.asarray([], dtype=int)
    ns_delta = np.int64(bars) * np.int64(5 * 60 * 1_000_000_000)
    shifted: list[np.ndarray] = []
    symbols = context["symbols"].astype(str)
    timestamp_ns = context["timestamp_ns"]
    for symbol in sorted(set(symbols[indices].tolist())):
        source = indices[symbols[indices] == symbol]
        positions = context["symbol_positions"][symbol]
        target_ns = timestamp_ns[source] + ns_delta
        loc = np.searchsorted(timestamp_ns[positions], target_ns)
        valid = loc < len(positions)
        if not np.any(valid):
            continue
        loc = loc[valid]
        source_targets = target_ns[valid]
        matched = timestamp_ns[positions[loc]] == source_targets
        if np.any(matched):
            shifted.append(positions[loc[matched]])
    if not shifted:
        return np.asarray([], dtype=int)
    return np.concatenate(shifted).astype(int)


def _context_with_random_funding_signs(context: dict[str, Any]) -> dict[str, Any]:
    rng = np.random.default_rng(PLACEBO_SEED)
    out = dict(context)
    signs = context["funding_sign"].astype(object).copy()
    observed = signs[np.isin(signs, ["positive", "negative"])]
    if len(observed) == 0:
        observed = np.asarray(["positive", "negative"], dtype=object)
    signs[:] = rng.choice(observed, size=len(signs))
    out["funding_sign"] = signs
    return out


def _simulate(
    context: dict[str, Any],
    indices: np.ndarray,
    directions: np.ndarray,
    *,
    exit_policy: str,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
) -> dict[str, Any]:
    best_exit, details = session_lab._simulate_time_stop(
        context,
        indices,
        directions=directions,
        exit_policy=exit_policy,
        symbol_costs=symbol_costs,
        extra_slippage_bps=extra_slippage_bps,
    )
    by_symbol = session_lab._group_return_stats_arrays(details["symbols"], details["net_bps"])
    by_year = session_lab._group_return_stats_arrays(details["years"], details["net_bps"])
    by_month = session_lab._group_return_stats_arrays(details["months"], details["net_bps"])
    symbol_month_counts = dict(Counter(details["symbol_months"].tolist()))
    symbol_counts = dict(Counter(details["symbols"].tolist()))
    positive_symbols = sorted(
        symbol for symbol, stats in by_symbol.items() if (stats.get("net_bps") or -1e9) > 0.0
    )
    row = {
        "event_count": int(best_exit["n"]),
        "best_exit": best_exit,
        "by_symbol": by_symbol,
        "by_year": by_year,
        "by_month": by_month,
        "positive_symbols": positive_symbols,
        "positive_symbol_count": len(positive_symbols),
        "top_symbol_month_share": session_lab._max_share(
            symbol_month_counts, len(details["net_bps"])
        ),
        "single_symbol_event_share": session_lab._max_share(symbol_counts, len(details["net_bps"])),
        "walk_forward_pass": session_lab._walk_forward_pass(by_year),
        "slippage_plus_10_bps": best_exit["slippage_plus_10_bps"],
    }
    row["passes_core_gates"] = _passes_core_gates(row)
    return row


def _passes_core_gates(row: dict[str, Any]) -> bool:
    best = row.get("best_exit") or {}
    return (
        int(row.get("event_count") or 0) >= 100
        and (best.get("net_bps") or -1e9) > 0.0
        and (best.get("t_stat") or -1e9) > 2.0
        and (best.get("cost_survival") or -1e9) >= 0.8
        and int(row.get("positive_symbol_count") or 0) >= 3
        and (row.get("top_symbol_month_share") or 1.0) <= 0.35
        and bool(row.get("walk_forward_pass"))
        and bool((best.get("slippage_plus_10_bps") or {}).get("survives"))
    )


def _empty_control(reason: str) -> dict[str, Any]:
    return {
        "not_applicable": True,
        "reason": reason,
        "event_count": 0,
        "passes_core_gates": False,
        "best_exit": {"net_bps": None, "t_stat": None, "cost_survival": None},
    }


def _evaluate_control(
    context: dict[str, Any],
    *,
    hour: int,
    session: str,
    direction_mode: str,
    volume_mode: str,
    oi_mode: str,
    trend_filter: str,
    exit_policy: str,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
    control: str,
    true_indices: np.ndarray,
) -> dict[str, Any]:
    effective_context = context
    inverted = False
    control_hour: int | None = hour
    non_funding = False
    if control == "same_hour_next_day":
        control_indices = _shift_indices_same_symbol(context, true_indices, bars=288)
        control_indices, directions = _filter_indices(
            context,
            control_indices,
            hour=hour,
            session=session,
            direction_mode=direction_mode,
            volume_mode=volume_mode,
            oi_mode=oi_mode,
            trend_filter=trend_filter,
        )
        return _simulate(
            context,
            control_indices,
            directions,
            exit_policy=exit_policy,
            symbol_costs=symbol_costs,
            extra_slippage_bps=extra_slippage_bps,
        )
    if control == "same_hour_previous_day":
        control_indices = _shift_indices_same_symbol(context, true_indices, bars=-288)
        control_indices, directions = _filter_indices(
            context,
            control_indices,
            hour=hour,
            session=session,
            direction_mode=direction_mode,
            volume_mode=volume_mode,
            oi_mode=oi_mode,
            trend_filter=trend_filter,
        )
        return _simulate(
            context,
            control_indices,
            directions,
            exit_policy=exit_policy,
            symbol_costs=symbol_costs,
            extra_slippage_bps=extra_slippage_bps,
        )
    if control == "neighbor_hour_minus_1":
        control_hour = (hour - 1) % 24
    elif control == "neighbor_hour_plus_1":
        control_hour = (hour + 1) % 24
    elif control == "inverted_direction":
        inverted = True
    elif control == "randomized_funding_sign":
        if direction_mode not in FUNDING_DIRECTION_MODES:
            return _empty_control("direction_mode_does_not_use_funding_sign")
        effective_context = _context_with_random_funding_signs(context)
    elif control == "non_funding_hours":
        non_funding = True
        control_hour = None
    else:
        raise ValueError(f"unsupported control: {control}")
    indices, event_directions = _base_events(
        effective_context,
        hour=control_hour,
        session=session,
        direction_mode=direction_mode,
        volume_mode=volume_mode,
        oi_mode=oi_mode,
        trend_filter=trend_filter,
        non_funding_hours_only=non_funding,
    )
    if inverted:
        event_directions = np.where(event_directions == "long", "short", "long").astype(object)
    return _simulate(
        effective_context,
        indices,
        event_directions,
        exit_policy=exit_policy,
        symbol_costs=symbol_costs,
        extra_slippage_bps=extra_slippage_bps,
    )


def _status(row: dict[str, Any]) -> str:
    if not bool(row.get("passes_core_gates")):
        if int(row.get("event_count") or 0) < 100:
            return "needs_sample_expansion"
        best = row.get("best_exit") or {}
        if (best.get("net_bps") or -1e9) <= 0.0:
            return "failed_net"
        if (best.get("t_stat") or -1e9) <= 2.0:
            return "failed_t_stat"
        if (best.get("cost_survival") or -1e9) < 0.8:
            return "failed_cost_survival"
        if int(row.get("positive_symbol_count") or 0) < 3:
            return "symbol_scoped_research_only"
        if (row.get("top_symbol_month_share") or 0.0) > 0.35:
            return "symbol_month_concentrated_research_only"
        if not bool(row.get("walk_forward_pass")):
            return "walk_forward_failed"
        return "failed_plus_10_bps_slippage"
    controls = row.get("controls") or {}
    applicable_controls = [
        control for control in controls.values() if not bool(control.get("not_applicable"))
    ]
    if any(bool(control.get("passes_core_gates")) for control in applicable_controls):
        return "control_passed_research_only"
    true_net = (row.get("best_exit") or {}).get("net_bps") or -1e9
    sufficiently_sampled_controls = [
        control for control in applicable_controls if int(control.get("event_count") or 0) >= 100
    ]
    control_nets = [
        (control.get("best_exit") or {}).get("net_bps") or -1e9
        for control in sufficiently_sampled_controls
    ]
    if control_nets and true_net <= max(control_nets):
        return "control_not_beaten_research_only"
    return "fresh_timing_candidate"


def _evaluate_variant(
    context: dict[str, Any],
    *,
    hour: int,
    session: str,
    direction_mode: str,
    volume_mode: str,
    oi_mode: str,
    trend_filter: str,
    exit_policy: str,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
) -> dict[str, Any]:
    indices, event_directions = _base_events(
        context,
        hour=hour,
        session=session,
        direction_mode=direction_mode,
        volume_mode=volume_mode,
        oi_mode=oi_mode,
        trend_filter=trend_filter,
    )
    row = _simulate(
        context,
        indices,
        event_directions,
        exit_policy=exit_policy,
        symbol_costs=symbol_costs,
        extra_slippage_bps=extra_slippage_bps,
    )
    if row["passes_core_gates"]:
        controls = {
            control: _evaluate_control(
                context,
                hour=hour,
                session=session,
                direction_mode=direction_mode,
                volume_mode=volume_mode,
                oi_mode=oi_mode,
                trend_filter=trend_filter,
                exit_policy=exit_policy,
                symbol_costs=symbol_costs,
                extra_slippage_bps=extra_slippage_bps,
                control=control,
                true_indices=indices,
            )
            for control in CONTROL_NAMES
        }
    else:
        controls = {
            control: _empty_control("true_condition_failed_core_gates") for control in CONTROL_NAMES
        }
    row.update(
        {
            "variant_id": (
                f"{FAMILY}__HOUR_{hour:02d}UTC__SESSION_{session.upper()}__"
                f"DIR_{direction_mode.upper()}__VOL_{volume_mode.upper()}__"
                f"OI_{oi_mode.upper()}__TREND_{trend_filter.upper()}__{exit_policy.upper()}"
            ),
            "family": FAMILY,
            "params": {
                "hour_utc": hour,
                "session": session,
                "direction_mode": direction_mode,
                "volume_mode": volume_mode,
                "oi_mode": oi_mode,
                "trend_filter": trend_filter,
                "exit_policy": exit_policy,
            },
            "controls": controls,
            "paper_approved": False,
            "live_approved": False,
        }
    )
    row["status"] = _status(row)
    row["score"] = (
        max(0.0, (row["best_exit"].get("net_bps") or 0.0))
        + 10.0 * max(0.0, (row["best_exit"].get("t_stat") or 0.0))
        + 20.0 * max(0.0, (row["best_exit"].get("cost_survival") or 0.0))
        + 5.0 * len(row["positive_symbols"])
        - 1000.0 * float(row["status"] != "fresh_timing_candidate")
    )
    return row


def build_time_of_day_report(
    *,
    repo_root: Path,
    symbols: list[str],
    years: list[int],
    hours: list[int],
    exit_policies: list[str],
    extra_slippage_bps: float,
    cost_overrides: dict[str, float],
    json_output: Path,
    csv_output: Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    raw, missing, input_summary = _load_frames(repo_root, symbols, years)
    context = _array_context(raw)
    symbol_costs = {
        symbol: session_lab._cost_for_symbol(symbol, cost_overrides) for symbol in symbols
    }
    rows: list[dict[str, Any]] = []
    for hour in hours:
        for session in ("asia", "europe", "us", "late_us"):
            for direction_mode in (
                "funding_fade",
                "trend_follow",
                "funding_sign",
                "previous_12bar_momentum",
            ):
                for volume_mode in ("optional", "required"):
                    for oi_mode in ("optional", "aligned"):
                        for trend_filter in ("optional", "aligned"):
                            for exit_policy in exit_policies:
                                rows.append(  # noqa: PERF401
                                    _evaluate_variant(
                                        context,
                                        hour=hour,
                                        session=session,
                                        direction_mode=direction_mode,
                                        volume_mode=volume_mode,
                                        oi_mode=oi_mode,
                                        trend_filter=trend_filter,
                                        exit_policy=exit_policy,
                                        symbol_costs=symbol_costs,
                                        extra_slippage_bps=extra_slippage_bps,
                                    )
                                )
    rows.sort(key=lambda item: item["score"], reverse=True)
    top = rows[0] if rows else {}
    report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "scope": {
            "family": FAMILY,
            "symbols_requested": symbols,
            "symbols_evaluated": sorted(raw["symbol"].unique().tolist()),
            "years": years,
            "timeframe": "5m",
            "hours_utc": hours,
            "sessions": ["asia", "europe", "us", "late_us"],
            "direction_modes": [
                "funding_fade",
                "trend_follow",
                "funding_sign",
                "previous_12bar_momentum",
            ],
            "volume_modes": ["optional", "required"],
            "oi_modes": ["optional", "aligned"],
            "trend_filters": ["optional", "aligned"],
            "exit_policies": exit_policies,
            "controls_required": [
                *CONTROL_NAMES,
            ],
            "promotion_policy": "true_condition_must_pass_and_all_applicable_controls_must_fail",
            "paper_live_policy": "paper/live remain empty until timing clue survives controls",
            "data_scope": ["OHLCV", "open_interest", "funding"],
        },
        "input_summary": input_summary,
        "missing_symbol_data": missing,
        "candidate_count": len(rows),
        "status_counts": dict(Counter(row["status"] for row in rows)),
        "top_variants": rows[:50],
        "by_symbol": top.get("by_symbol", {}),
        "by_year": top.get("by_year", {}),
        "by_month": top.get("by_month", {}),
        "paper_approved_events": [],
        "live_approved_events": [],
    }
    csv = pd.DataFrame(
        [
            {
                "variant_id": row["variant_id"],
                "event_count": row["event_count"],
                "net_bps": row["best_exit"].get("net_bps"),
                "gross_bps": row["best_exit"].get("gross_bps"),
                "t_stat": row["best_exit"].get("t_stat"),
                "cost_survival": row["best_exit"].get("cost_survival"),
                "positive_symbol_count": row["positive_symbol_count"],
                "top_symbol_month_share": row["top_symbol_month_share"],
                "walk_forward_pass": row["walk_forward_pass"],
                "slippage_plus_10_bps_net_bps": row["slippage_plus_10_bps"].get("net_bps"),
                "slippage_plus_10_bps_survives": row["slippage_plus_10_bps"].get("survives"),
                "control_passed": any(
                    bool(control.get("passes_core_gates"))
                    for control in row["controls"].values()
                    if not bool(control.get("not_applicable"))
                ),
                "max_control_net_bps": max(
                    [
                        (control.get("best_exit") or {}).get("net_bps") or -1e9
                        for control in row["controls"].values()
                        if not bool(control.get("not_applicable"))
                        and int(control.get("event_count") or 0) >= 100
                    ],
                    default=None,
                ),
                "hour_utc": row["params"]["hour_utc"],
                "session": row["params"]["session"],
                "direction_mode": row["params"]["direction_mode"],
                "volume_mode": row["params"]["volume_mode"],
                "oi_mode": row["params"]["oi_mode"],
                "trend_filter": row["params"]["trend_filter"],
                "exit_policy": row["params"]["exit_policy"],
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
    parser = argparse.ArgumentParser(description="Time-of-day drift detector lab")
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--years", default=",".join(str(year) for year in DEFAULT_YEARS))
    parser.add_argument("--hours", default=",".join(str(hour) for hour in DEFAULT_HOURS))
    parser.add_argument("--exit-policies", default=",".join(DEFAULT_EXIT_POLICIES))
    parser.add_argument("--extra-slippage-bps", type=float, default=DEFAULT_EXTRA_SLIPPAGE_BPS)
    parser.add_argument("--cost-overrides", default="")
    parser.add_argument(
        "--json-output", default=str(DEFAULT_REPORT_DIR / "time_of_day_lab_report.json")
    )
    parser.add_argument(
        "--csv-output", default=str(DEFAULT_REPORT_DIR / "top_time_of_day_variants.csv")
    )
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    report, _ = build_time_of_day_report(
        repo_root=repo_root,
        symbols=[item.upper() for item in _parse_csv(args.symbols)],
        years=_parse_ints(args.years),
        hours=[int(item) for item in _parse_csv(args.hours)],
        exit_policies=_parse_csv(args.exit_policies),
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
