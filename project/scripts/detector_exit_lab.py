from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.scripts.detector_shadow_report import DEFAULT_HORIZONS, _prepare_symbol_frame, _return_summary
from project.scripts.detector_tuning_lab import (
    MAX_HOLD_GRID,
    _add_features,
    _cooldown_indices_by_symbol,
    _direction_mult,
    _generate_variants,
    _path_metrics,
)


DEFAULT_REPORT_DIR = Path("data/reports/detectors/no_liquidations_v1")
TP_GRID = (10.0, 15.0, 25.0, 40.0, 60.0)
SL_GRID = (10.0, 15.0, 25.0, 40.0, 60.0)
TRAIL_ACTIVATE_GRID = (10.0, 20.0, 30.0)
TRAIL_GRID = (10.0, 15.0, 25.0)
TIME_STOP_GRID = (6, 12, 24)
KNOWN_PREFIXES = (
    "FUNDING_POS_EXHAUSTION_AFTER_PERSISTENCE",
    "FUNDING_NEG_EXHAUSTION_AFTER_PERSISTENCE",
    "FUNDING_POS_CONTINUATION_STRICT",
    "FUNDING_NEG_CONTINUATION_STRICT",
    "FUNDING_POS_BREAK_STRICT",
    "FUNDING_NEG_BREAK_STRICT",
    "SHORT_BUILD_CONTINUATION_STRICT",
    "LONG_BUILD_CONTINUATION_STRICT",
    "LONG_STRESS_REVERSAL_STRICT",
    "SHORT_SQUEEZE_SETUP_STRICT",
    "OI_FLUSH_DOWN_REVERSAL_STRICT",
    "OI_FLUSH_DOWN_CONTINUATION_STRICT",
    "OI_FLUSH_UP_REVERSAL_STRICT",
    "OI_FLUSH_UP_CONTINUATION_STRICT",
    "FAILED_CONTINUATION_STRONG_RECLAIM",
    "FAILED_CONTINUATION_WICK_REVERSAL",
    "FAILED_BREAKDOWN_RECLAIM",
    "FAILED_BREAKOUT_REJECTION",
)


@dataclass(frozen=True)
class CandidateKey:
    variant_id: str
    cooldown_bars: int


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _json_load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _variant_family(variant_id: str) -> str:
    for prefix in sorted(KNOWN_PREFIXES, key=len, reverse=True):
        if variant_id.startswith(prefix):
            return prefix
    return variant_id.rsplit("_", 3)[0]


def _load_candidate_keys(
    tuning_report: dict[str, Any],
    csv_path: Path,
    *,
    top_n_by_edge: int,
    include_rare_interesting: bool,
) -> tuple[set[CandidateKey], dict[CandidateKey, dict[str, Any]]]:
    cooldowns = tuning_report.get("scope", {}).get("cooldown_bars") or [12]
    if isinstance(cooldowns, int):
        cooldowns = [cooldowns]
    default_cooldown = int(cooldowns[0]) if cooldowns else 12
    selected: set[CandidateKey] = set()
    source_rows: dict[CandidateKey, dict[str, Any]] = {}

    def add(row: dict[str, Any], reason: str) -> None:
        cooldown = _safe_int(row.get("cooldown_bars")) or default_cooldown
        key = CandidateKey(str(row["variant_id"]), cooldown)
        selected.add(key)
        existing = source_rows.setdefault(key, dict(row))
        reasons = set(existing.get("selection_reasons", []))
        reasons.add(reason)
        existing["selection_reasons"] = sorted(reasons)

    top_variants = list(tuning_report.get("top_variants") or [])
    for row in top_variants:
        if row.get("status") == "path_research_candidate":
            add(row, "path_research_candidate")
        count = _safe_int(row.get("event_count")) or 0
        net = _safe_float(row.get("net_bps"))
        t_stat = _safe_float(row.get("t_stat"))
        edge = _safe_float(row.get("edge_ratio"))
        cost_survival = _safe_float(row.get("cost_survival"))
        if include_rare_interesting and 20 <= count < 50 and (
            (net is not None and net > 0.0 and t_stat is not None and t_stat >= 1.5)
            or (edge is not None and edge >= 1.5)
            or (net is not None and net > 0.0 and cost_survival is not None and cost_survival >= 0.8)
        ):
            add(row, "rare_but_interesting")

    edge_rows = sorted(
        top_variants,
        key=lambda item: _safe_float(item.get("edge_ratio")) or -10**9,
        reverse=True,
    )
    for row in edge_rows[:top_n_by_edge]:
        add(row, "top_mfe_mae_edge_ratio")

    if csv_path.exists():
        csv = pd.read_csv(csv_path)
        if not csv.empty:
            csv["_edge"] = pd.to_numeric(csv.get("edge_ratio"), errors="coerce")
            csv["_net"] = pd.to_numeric(csv.get("net_bps"), errors="coerce")
            csv["_t"] = pd.to_numeric(csv.get("t_stat"), errors="coerce")
            csv["_cost"] = pd.to_numeric(csv.get("cost_survival"), errors="coerce")
            csv["_count"] = pd.to_numeric(csv.get("event_count"), errors="coerce")
            for row in csv[csv.get("status") == "path_research_candidate"].to_dict("records"):
                add(row, "path_research_candidate_csv")
            rare = csv[
                (csv["_count"] >= 20)
                & (csv["_count"] < 50)
                & (
                    ((csv["_net"] > 0.0) & (csv["_t"] >= 1.5))
                    | (csv["_edge"] >= 1.5)
                    | ((csv["_net"] > 0.0) & (csv["_cost"] >= 0.8))
                )
            ]
            for row in rare.to_dict("records"):
                add(row, "rare_but_interesting_csv")
            for row in csv.sort_values("_edge", ascending=False).head(top_n_by_edge).to_dict("records"):
                add(row, "top_mfe_mae_edge_ratio_csv")
    return selected, source_rows


def _finalize_returns(returns: list[float], holds: list[float], gross_returns: list[float]) -> dict[str, Any]:
    summary = _return_summary(returns)
    gross = _return_summary(gross_returns)
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in returns:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = min(max_drawdown, cumulative - peak)
    net = summary.get("mean_bps")
    gross_mean = gross.get("mean_bps")
    return {
        "net_bps": net,
        "gross_bps": gross_mean,
        "t_stat": summary.get("t_stat"),
        "hit_rate": float(sum(1 for value in returns if value > 0.0) / len(returns)) if returns else None,
        "avg_hold_bars": float(np.mean(holds)) if holds else None,
        "max_drawdown_bps": float(max_drawdown),
        "cost_survival": float(net / gross_mean) if net is not None and gross_mean is not None and gross_mean > 0.0 else None,
        "n": len(returns),
    }


def _simulate_tp_sl(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    indices: np.ndarray,
    direction: str,
    *,
    tp_bps: float,
    sl_bps: float,
    max_hold_bars: int,
    cost_bps: float,
) -> dict[str, Any]:
    mult = _direction_mult(direction)
    returns: list[float] = []
    gross_returns: list[float] = []
    holds: list[float] = []
    for idx in indices:
        if idx + 1 >= len(close) or not np.isfinite(close[idx]) or close[idx] <= 0.0:
            continue
        entry = close[idx]
        exit_bps: float | None = None
        hold = 0
        for step in range(1, min(max_hold_bars, len(close) - idx - 1) + 1):
            if mult > 0:
                fav = (high[idx + step] / entry - 1.0) * 10000.0
                adv = (low[idx + step] / entry - 1.0) * 10000.0
            else:
                fav = (entry / low[idx + step] - 1.0) * 10000.0
                adv = (entry / high[idx + step] - 1.0) * 10000.0
            hold = step
            if adv <= -sl_bps:
                exit_bps = -sl_bps
                break
            if fav >= tp_bps:
                exit_bps = tp_bps
                break
        if exit_bps is None:
            end_idx = min(idx + max_hold_bars, len(close) - 1)
            exit_bps = ((close[end_idx] / entry) - 1.0) * 10000.0 * mult
            hold = end_idx - idx
        gross_returns.append(float(exit_bps))
        returns.append(float(exit_bps - cost_bps))
        holds.append(float(hold))
    return _finalize_returns(returns, holds, gross_returns)


def _simulate_trailing(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    indices: np.ndarray,
    direction: str,
    *,
    activate_bps: float,
    trail_bps: float,
    max_hold_bars: int,
    cost_bps: float,
) -> dict[str, Any]:
    mult = _direction_mult(direction)
    returns: list[float] = []
    gross_returns: list[float] = []
    holds: list[float] = []
    for idx in indices:
        if idx + 1 >= len(close) or not np.isfinite(close[idx]) or close[idx] <= 0.0:
            continue
        entry = close[idx]
        active = False
        best_price = entry
        exit_bps: float | None = None
        hold = 0
        for step in range(1, min(max_hold_bars, len(close) - idx - 1) + 1):
            hold = step
            if mult > 0:
                best_price = max(best_price, high[idx + step])
                fav = (best_price / entry - 1.0) * 10000.0
                if fav >= activate_bps:
                    active = True
                if active:
                    stop_price = best_price * (1.0 - trail_bps / 10000.0)
                    if low[idx + step] <= stop_price:
                        exit_bps = (stop_price / entry - 1.0) * 10000.0
                        break
            else:
                best_price = min(best_price, low[idx + step])
                fav = (entry / best_price - 1.0) * 10000.0
                if fav >= activate_bps:
                    active = True
                if active:
                    stop_price = best_price * (1.0 + trail_bps / 10000.0)
                    if high[idx + step] >= stop_price:
                        exit_bps = (entry / stop_price - 1.0) * 10000.0
                        break
        if exit_bps is None:
            end_idx = min(idx + max_hold_bars, len(close) - 1)
            exit_bps = ((close[end_idx] / entry) - 1.0) * 10000.0 * mult
            hold = end_idx - idx
        gross_returns.append(float(exit_bps))
        returns.append(float(exit_bps - cost_bps))
        holds.append(float(hold))
    return _finalize_returns(returns, holds, gross_returns)


def _simulate_time_stop(
    close: np.ndarray,
    indices: np.ndarray,
    direction: str,
    *,
    time_stop_bars: int,
    max_hold_bars: int,
    cost_bps: float,
) -> dict[str, Any]:
    mult = _direction_mult(direction)
    returns: list[float] = []
    gross_returns: list[float] = []
    holds: list[float] = []
    for idx in indices:
        if idx + 1 >= len(close) or not np.isfinite(close[idx]) or close[idx] <= 0.0:
            continue
        entry = close[idx]
        check_idx = min(idx + time_stop_bars, len(close) - 1)
        check_bps = ((close[check_idx] / entry) - 1.0) * 10000.0 * mult
        if check_bps <= 0.0 or check_idx >= idx + max_hold_bars:
            exit_bps = check_bps
            hold = check_idx - idx
        else:
            end_idx = min(idx + max_hold_bars, len(close) - 1)
            exit_bps = ((close[end_idx] / entry) - 1.0) * 10000.0 * mult
            hold = end_idx - idx
        gross_returns.append(float(exit_bps))
        returns.append(float(exit_bps - cost_bps))
        holds.append(float(hold))
    return _finalize_returns(returns, holds, gross_returns)


def _simulate_state_exit(
    df: pd.DataFrame,
    close: np.ndarray,
    indices: np.ndarray,
    direction: str,
    *,
    policy: str,
    max_hold_bars: int,
    cost_bps: float,
) -> dict[str, Any]:
    mult = _direction_mult(direction)
    funding_abs_pct = pd.to_numeric(df["funding_abs_pct"], errors="coerce").to_numpy()
    oi_chg = pd.to_numeric(df["oi_chg_12"], errors="coerce").to_numpy()
    failed_up = df["failed_breakout_rejection_24"].fillna(False).to_numpy()
    failed_down = df["failed_breakdown_reclaim_24"].fillna(False).to_numpy()
    returns: list[float] = []
    gross_returns: list[float] = []
    holds: list[float] = []
    for idx in indices:
        if idx + 1 >= len(close) or not np.isfinite(close[idx]) or close[idx] <= 0.0:
            continue
        entry = close[idx]
        entry_oi = oi_chg[idx] if np.isfinite(oi_chg[idx]) else 0.0
        exit_idx = min(idx + max_hold_bars, len(close) - 1)
        for step in range(1, min(max_hold_bars, len(close) - idx - 1) + 1):
            pos = idx + step
            triggered = False
            if policy == "funding_normalization":
                triggered = np.isfinite(funding_abs_pct[pos]) and funding_abs_pct[pos] < 70.0
            elif policy == "oi_delta_reversal":
                triggered = np.isfinite(oi_chg[pos]) and entry_oi != 0.0 and np.sign(oi_chg[pos]) != np.sign(entry_oi)
            elif policy == "failed_continuation_invalidation":
                triggered = bool(failed_up[pos]) if mult > 0 else bool(failed_down[pos])
            if triggered:
                exit_idx = pos
                break
        exit_bps = ((close[exit_idx] / entry) - 1.0) * 10000.0 * mult
        gross_returns.append(float(exit_bps))
        returns.append(float(exit_bps - cost_bps))
        holds.append(float(exit_idx - idx))
    return _finalize_returns(returns, holds, gross_returns)


def _best_exit_policy(
    df: pd.DataFrame,
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    indices: np.ndarray,
    direction: str,
    cost_bps: float,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    results: list[dict[str, Any]] = []
    for tp in TP_GRID:
        for sl in SL_GRID:
            for hold in MAX_HOLD_GRID:
                metrics = _simulate_tp_sl(close, high, low, indices, direction, tp_bps=tp, sl_bps=sl, max_hold_bars=hold, cost_bps=cost_bps)
                results.append({"policy": f"tp{int(tp)}_sl{int(sl)}_max{hold}", "policy_type": "tp_sl", **metrics})
    for activate in TRAIL_ACTIVATE_GRID:
        for trail in TRAIL_GRID:
            for hold in MAX_HOLD_GRID:
                metrics = _simulate_trailing(close, high, low, indices, direction, activate_bps=activate, trail_bps=trail, max_hold_bars=hold, cost_bps=cost_bps)
                results.append({"policy": f"trail_a{int(activate)}_t{int(trail)}_max{hold}", "policy_type": "trailing", **metrics})
    for stop in TIME_STOP_GRID:
        for hold in MAX_HOLD_GRID:
            if stop > hold:
                continue
            metrics = _simulate_time_stop(close, indices, direction, time_stop_bars=stop, max_hold_bars=hold, cost_bps=cost_bps)
            results.append({"policy": f"time_stop{stop}_max{hold}", "policy_type": "time_stop", **metrics})
    for policy in ("funding_normalization", "oi_delta_reversal", "failed_continuation_invalidation"):
        for hold in MAX_HOLD_GRID:
            metrics = _simulate_state_exit(df, close, indices, direction, policy=policy, max_hold_bars=hold, cost_bps=cost_bps)
            results.append({"policy": f"{policy}_max{hold}", "policy_type": "state_exit", **metrics})
    best = max(results, key=lambda item: (item.get("net_bps") or -10**9, item.get("t_stat") or -10**9), default=None)
    return best, sorted(results, key=lambda item: (item.get("net_bps") or -10**9, item.get("t_stat") or -10**9), reverse=True)[:8]


def _status(event_count: int, best_exit: dict[str, Any] | None) -> str:
    if best_exit is None:
        return "research_only"
    if (
        50 <= event_count <= 1500
        and (best_exit.get("net_bps") or -10**9) > 0.0
        and (best_exit.get("t_stat") or -10**9) > 2.0
        and (best_exit.get("cost_survival") or -10**9) >= 0.8
    ):
        return "exit_research_candidate_requires_fresh_validation"
    if 20 <= event_count < 50 and (best_exit.get("net_bps") or -10**9) > 0.0 and (best_exit.get("t_stat") or -10**9) > 2.0:
        return "rare_exit_research_candidate_needs_sample_expansion"
    if (best_exit.get("net_bps") or -10**9) > 0.0:
        return "exit_path_research_only"
    return "research_only"


def _family_robustness(rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["base_variant_family"]].append(row)
    out: dict[str, Any] = {}
    for family, family_rows in grouped.items():
        if not family_rows:
            continue
        positives = [row for row in family_rows if (row.get("best_exit", {}).get("net_bps") or -10**9) > 0.0]
        positive_rate = len(positives) / len(family_rows)
        adjacent_hits = 0
        for row in positives:
            params = row.get("params") or {}
            has_neighbor = False
            for other in positives:
                if other is row:
                    continue
                other_params = other.get("params") or {}
                shared = [key for key in params if key in other_params and isinstance(params[key], (int, float)) and isinstance(other_params[key], (int, float))]
                if not shared:
                    continue
                distance = sum(1 for key in shared if params[key] != other_params[key])
                if distance <= 1:
                    has_neighbor = True
                    break
            if has_neighbor:
                adjacent_hits += 1
        adjacent_positive_rate = adjacent_hits / len(positives) if positives else 0.0
        sorted_rows = sorted(family_rows, key=lambda item: item.get("params", {}).get("funding_abs_pct", item.get("params", {}).get("oi_pct", 0.0)))
        metrics = [row.get("best_exit", {}).get("net_bps") for row in sorted_rows]
        clean = [float(value) for value in metrics if value is not None and math.isfinite(float(value))]
        if len(clean) >= 3:
            x = np.arange(len(clean), dtype=float)
            y = np.asarray(clean, dtype=float)
            if float(np.std(y)) == 0.0:
                corr = 0.0
            else:
                corr = float(np.corrcoef(x, y)[0, 1])
                if not math.isfinite(corr):
                    corr = 0.0
        else:
            corr = 0.0
        best = max(family_rows, key=lambda item: item.get("best_exit", {}).get("net_bps") or -10**9)
        best_is_positive = (best.get("best_exit", {}).get("net_bps") or -10**9) > 0.0
        best_variant_is_isolated = bool(best_is_positive and adjacent_positive_rate == 0.0)
        out[family] = {
            "evaluated_variants": len(family_rows),
            "positive_exit_rate": positive_rate,
            "adjacent_positive_rate": adjacent_positive_rate,
            "monotonicity_score": max(0.0, corr),
            "best_variant_is_isolated": best_variant_is_isolated,
            "best_variant_id": best["variant_id"],
            "best_exit_net_bps": best.get("best_exit", {}).get("net_bps"),
            "best_exit_t_stat": best.get("best_exit", {}).get("t_stat"),
        }
    return out


def _main_failure(row: dict[str, Any]) -> str:
    event_count = int(row.get("event_count") or 0)
    best = row.get("best_exit") or {}
    if event_count < 50:
        return "event_count_below_50"
    if event_count > 1500:
        return "event_count_above_1500"
    if (best.get("net_bps") or -10**9) <= 0.0:
        return "exit_net_not_positive"
    if (best.get("t_stat") or -10**9) <= 2.0:
        return "exit_t_stat_not_above_2"
    if (best.get("cost_survival") or -10**9) < 0.8:
        return "exit_cost_survival_below_0_8"
    return "requires_fresh_validation"


def _candidate_dossiers(rows: list[dict[str, Any]], family_robustness: dict[str, Any], top_n: int) -> list[dict[str, Any]]:
    dossiers = []
    for row in rows[:top_n]:
        best = row.get("best_exit") or {}
        path = row.get("best_path") or {}
        robustness = family_robustness.get(row["base_variant_family"], {})
        reasons = row.get("selection_reasons") or []
        if "rare_but_interesting" in " ".join(reasons):
            why = "rare strict variant with positive path or return diagnostics"
        elif path.get("edge_ratio") is not None and path["edge_ratio"] >= 1.5:
            why = "positive MFE/MAE path shape despite weak fixed-horizon validation"
        elif (best.get("net_bps") or -10**9) > 0.0:
            why = "exit policy produced positive diagnostic net before fresh validation"
        else:
            why = "selected from tuning-lab path screen for exit diagnostics"
        dossiers.append(
            {
                "variant_id": row["variant_id"],
                "cooldown_bars": row["cooldown_bars"],
                "why_interesting": why,
                "main_failure": _main_failure(row),
                "best_fixed_horizon": row.get("best_horizon_bars"),
                "best_path_metric": "MFE/MAE" if path.get("edge_ratio") is not None else None,
                "best_exit_policy": best.get("policy"),
                "best_exit_net_bps": best.get("net_bps"),
                "best_exit_t_stat": best.get("t_stat"),
                "family_robustness": {
                    "adjacent_positive_rate": robustness.get("adjacent_positive_rate"),
                    "monotonicity_score": robustness.get("monotonicity_score"),
                    "best_variant_is_isolated": robustness.get("best_variant_is_isolated"),
                },
                "next_action": "fresh_validation_required" if row["status"].endswith("requires_fresh_validation") else "research_only_no_approval",
            }
        )
    return dossiers


def build_exit_lab_report(
    *,
    repo_root: Path,
    tuning_report_path: Path,
    tuning_csv_path: Path,
    json_output: Path,
    csv_output: Path,
    top_n_by_edge: int,
    dossier_n: int,
    max_raw_variants: int,
) -> tuple[dict[str, Any], pd.DataFrame]:
    tuning_report = _json_load(tuning_report_path)
    scope = tuning_report.get("scope", {})
    symbols = [str(symbol).upper() for symbol in scope.get("symbols", ["BTCUSDT", "ETHUSDT"])]
    years = [int(year) for year in scope.get("years", [2022, 2023, 2024, 2025])]
    horizons = [int(horizon) for horizon in scope.get("horizons", DEFAULT_HORIZONS)]
    cost_bps = float(scope.get("cost_round_trip_bps", 6.0))
    cooldowns = scope.get("cooldown_bars") or [12]
    if isinstance(cooldowns, int):
        cooldowns = [cooldowns]
    cooldowns = [int(item) for item in cooldowns]

    selected, source_rows = _load_candidate_keys(tuning_report, tuning_csv_path, top_n_by_edge=top_n_by_edge, include_rare_interesting=True)
    if not selected:
        raise RuntimeError("no exit-lab candidates selected from tuning report/csv")

    frames = []
    input_summary: dict[str, Any] = {}
    for symbol in symbols:
        frame = _add_features(_prepare_symbol_frame(repo_root, symbol, years))
        frame["symbol"] = symbol
        frames.append(frame)
        input_summary[symbol] = {
            "rows": int(len(frame)),
            "start": str(frame["timestamp"].min()),
            "end": str(frame["timestamp"].max()),
            "years": sorted(frame["shadow_year"].dropna().unique().tolist()),
        }
    df = pd.concat(frames, ignore_index=True).sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    close = pd.to_numeric(df["close"], errors="coerce").to_numpy()
    high = pd.to_numeric(df["high"], errors="coerce").to_numpy()
    low = pd.to_numeric(df["low"], errors="coerce").to_numpy()

    variants = {variant.variant_id: variant for variant in _generate_variants(df, max_raw_variants)}
    rows: list[dict[str, Any]] = []
    for key in sorted(selected, key=lambda item: (item.variant_id, item.cooldown_bars)):
        variant = variants.get(key.variant_id)
        if variant is None:
            continue
        indices = _cooldown_indices_by_symbol(df, variant.mask, key.cooldown_bars)
        if len(indices) < 20:
            continue
        horizon_metrics = {
            f"{horizon}b": _path_metrics(close, high, low, indices, variant.direction, horizon, cost_bps)
            for horizon in horizons
        }
        best_horizon = None
        best_path = {"edge_ratio": None}
        for horizon in horizons:
            metrics = horizon_metrics[f"{horizon}b"]
            edge = metrics.get("edge_ratio")
            fixed_net = metrics["forward_close_net_bps"].get("mean_bps")
            current_score = ((edge or 0.0) * 10.0) + max(0.0, fixed_net or 0.0)
            best_score = ((best_path.get("edge_ratio") or 0.0) * 10.0) + max(0.0, best_path.get("forward_close_net_bps", {}).get("mean_bps") or 0.0)
            if best_horizon is None or current_score > best_score:
                best_horizon = horizon
                best_path = metrics
        best_exit, top_exit_policies = _best_exit_policy(df, close, high, low, indices, variant.direction, cost_bps)
        source = source_rows.get(key, {})
        row = {
            "variant_id": key.variant_id,
            "base_variant_family": _variant_family(key.variant_id),
            "family": variant.family,
            "direction": variant.direction,
            "cooldown_bars": key.cooldown_bars,
            "bar_interval": "5m",
            "event_count": int(len(indices)),
            "params": variant.params,
            "selection_reasons": source.get("selection_reasons", []),
            "best_horizon_bars": best_horizon,
            "best_path": {
                "mfe_bps": best_path.get("max_favorable_bps"),
                "mae_bps": best_path.get("max_adverse_bps"),
                "edge_ratio": best_path.get("edge_ratio"),
                "mfe_hit_rate_after_cost": best_path.get("mfe_hit_rate_after_cost"),
                "mae_exceeds_cost_rate": best_path.get("mae_exceeds_cost_rate"),
                "forward_close_net_bps": best_path.get("forward_close_net_bps", {}).get("mean_bps"),
                "forward_close_t_stat": best_path.get("forward_close_net_bps", {}).get("t_stat"),
            },
            "best_exit": best_exit,
            "top_exit_policies": top_exit_policies,
            "horizon_diagnostics": horizon_metrics,
            "status": _status(int(len(indices)), best_exit),
            "paper_approved": False,
            "live_approved": False,
        }
        score = 0.0
        if best_exit:
            score += max(0.0, best_exit.get("net_bps") or 0.0)
            score += 8.0 * max(0.0, best_exit.get("t_stat") or 0.0)
            score += 25.0 * max(0.0, best_exit.get("cost_survival") or 0.0)
        score += 8.0 * max(0.0, row["best_path"].get("edge_ratio") or 0.0)
        if 50 <= row["event_count"] <= 1500:
            score += 20.0
        row["score"] = float(score)
        rows.append(row)

    rows.sort(key=lambda item: item["score"], reverse=True)
    family_robustness = _family_robustness(rows)
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "symbols": symbols,
            "years": years,
            "timeframe": "5m",
            "execution_timeframe": "5m",
            "cost_round_trip_bps": cost_bps,
            "cooldown_bars": cooldowns,
            "candidate_sources": ["path_research_candidate", "rare_but_interesting", "top_mfe_mae_edge_ratio"],
            "approval_policy": "research_only_outputs_require_fresh_validation",
        },
        "input_summary": input_summary,
        "exit_grid": {
            "tp_bps": TP_GRID,
            "sl_bps": SL_GRID,
            "max_hold_bars": MAX_HOLD_GRID,
            "trailing_activate_bps": TRAIL_ACTIVATE_GRID,
            "trailing_distance_bps": TRAIL_GRID,
            "time_stop_bars": TIME_STOP_GRID,
            "state_exits": ["funding_normalization", "oi_delta_reversal", "failed_continuation_invalidation"],
        },
        "candidate_count": len(rows),
        "status_counts": dict(Counter(row["status"] for row in rows)),
        "family_robustness": family_robustness,
        "candidate_dossiers": _candidate_dossiers(rows, family_robustness, dossier_n),
        "top_exit_variants": rows[:dossier_n],
        "paper_approved_events": [],
        "live_approved_events": [],
    }

    csv_rows = []
    for row in rows:
        best = row.get("best_exit") or {}
        path = row.get("best_path") or {}
        csv_rows.append(
            {
                "variant_id": row["variant_id"],
                "base_variant_family": row["base_variant_family"],
                "family": row["family"],
                "direction": row["direction"],
                "cooldown_bars": row["cooldown_bars"],
                "event_count": row["event_count"],
                "best_horizon_bars": row["best_horizon_bars"],
                "mfe_bps": path.get("mfe_bps"),
                "mae_bps": path.get("mae_bps"),
                "edge_ratio": path.get("edge_ratio"),
                "fixed_net_bps": path.get("forward_close_net_bps"),
                "fixed_t_stat": path.get("forward_close_t_stat"),
                "best_exit_policy": best.get("policy"),
                "best_exit_type": best.get("policy_type"),
                "exit_net_bps": best.get("net_bps"),
                "exit_gross_bps": best.get("gross_bps"),
                "exit_t_stat": best.get("t_stat"),
                "exit_hit_rate": best.get("hit_rate"),
                "avg_hold_bars": best.get("avg_hold_bars"),
                "max_drawdown_bps": best.get("max_drawdown_bps"),
                "cost_survival": best.get("cost_survival"),
                "selection_reasons": ",".join(row.get("selection_reasons", [])),
                "status": row["status"],
                "score": row["score"],
            }
        )
    csv = pd.DataFrame(csv_rows)
    json_output.parent.mkdir(parents=True, exist_ok=True)
    csv_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    csv.to_csv(csv_output, index=False)
    return report, csv


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Evaluate exit/path behavior for detector tuning-lab research candidates")
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--tuning-report", default="tuning_lab_report.json")
    parser.add_argument("--tuning-csv", default="top_event_variants.csv")
    parser.add_argument("--json-output", default="exit_lab_report.json")
    parser.add_argument("--csv-output", default="top_exit_variants.csv")
    parser.add_argument("--top-n-by-edge", type=int, default=100)
    parser.add_argument("--dossier-n", type=int, default=25)
    parser.add_argument("--max-raw-variants", type=int, default=20000)
    args = parser.parse_args(argv)

    repo_root = Path(__file__).resolve().parents[2]
    report_dir = repo_root / args.report_dir
    report, _ = build_exit_lab_report(
        repo_root=repo_root,
        tuning_report_path=report_dir / args.tuning_report,
        tuning_csv_path=report_dir / args.tuning_csv,
        json_output=report_dir / args.json_output,
        csv_output=report_dir / args.csv_output,
        top_n_by_edge=int(args.top_n_by_edge),
        dossier_n=int(args.dossier_n),
        max_raw_variants=int(args.max_raw_variants),
    )
    print(
        json.dumps(
            {
                "status": "pass",
                "json_output": str(report_dir / args.json_output),
                "csv_output": str(report_dir / args.csv_output),
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
