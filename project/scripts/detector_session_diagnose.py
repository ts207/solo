from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.scripts import detector_session_lab as session_lab
from project.scripts.detector_targeted_expansion import _parse_cost_overrides, _parse_exit_policy
from project.scripts.detector_tuning_lab import _parse_csv, _parse_ints

DEFAULT_REPORT_DIR = Path("data/reports/detectors/no_liquidations_v1")
DEFAULT_JSON_OUTPUT = DEFAULT_REPORT_DIR / "session_funding_window_diagnosis.json"
DEFAULT_EVENTS_OUTPUT = DEFAULT_REPORT_DIR / "session_funding_window_events.csv"
DEFAULT_VARIANT = "FUNDING_WINDOW_DRIFT"
FUNDING_WINDOW_HOURS = (0, 8, 16)
PLACEBO_SEED = 20260507
FINGERPRINT_COLUMNS = ("symbol", "event_ts", "direction", "exit_policy")


def _summary(values: pd.Series | np.ndarray | list[float]) -> dict[str, Any]:
    arr = pd.to_numeric(pd.Series(values), errors="coerce").dropna().to_numpy(dtype=float)
    return session_lab._return_stats(arr)


def _cost_survival(events: pd.DataFrame) -> float | None:
    if events.empty:
        return None
    gross = float(events["gross_bps"].mean())
    net = float(events["net_bps"].mean())
    if gross <= 0.0:
        return None
    return float(net / gross)


def _t_stat(values: pd.Series) -> float | None:
    return _summary(values).get("t_stat")


def _fingerprint_frame(events: pd.DataFrame) -> pd.Series:
    return events.loc[:, FINGERPRINT_COLUMNS].astype(str).agg("|".join, axis=1)


def _fingerprint_digest(fingerprints: list[str]) -> str:
    payload = "\n".join(sorted(fingerprints)).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _spec_for_variant(variant: str) -> dict[str, Any]:
    for spec in session_lab.VARIANT_SPECS:
        if spec["variant"] == variant:
            return spec
    raise ValueError(f"unknown session variant: {variant}")


def _variant_id(
    *,
    variant: str,
    range_session: str,
    trade_session: str,
    breakout_buffer_bps: float,
    volume_mode: str,
    oi_mode: str,
    trend_filter: str,
    exit_policy: str,
) -> str:
    return (
        f"{variant}__RANGE_{range_session.upper()}__TRADE_{trade_session.upper()}__"
        f"BUFFER_{breakout_buffer_bps:g}BPS__VOL_{volume_mode.upper()}__"
        f"OI_{oi_mode.upper()}__TREND_{trend_filter.upper()}__{exit_policy.upper()}"
    )


def _simulate_events(
    context: dict[str, Any],
    indices: np.ndarray,
    *,
    directions: np.ndarray,
    exit_policy: str,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
) -> pd.DataFrame:
    time_stop, max_hold = _parse_exit_policy(exit_policy)
    close = context["close"]
    symbols = context["symbols"]
    if len(indices) == 0:
        return _empty_events()

    idx = indices.astype(int)
    check_idx = idx + time_stop
    end_idx = idx + max_hold
    in_bounds = end_idx < len(close)
    idx = idx[in_bounds]
    check_idx = check_idx[in_bounds]
    end_idx = end_idx[in_bounds]
    directions = directions[in_bounds]
    if len(idx) == 0:
        return _empty_events()

    valid = (
        np.isfinite(close[idx])
        & (close[idx] > 0.0)
        & (symbols[check_idx] == symbols[idx])
        & (symbols[end_idx] == symbols[idx])
    )
    idx = idx[valid]
    check_idx = check_idx[valid]
    end_idx = end_idx[valid]
    directions = directions[valid]
    if len(idx) == 0:
        return _empty_events()

    mult = np.where(directions == "long", 1.0, -1.0)
    entry = close[idx]
    check_bps = ((close[check_idx] / entry) - 1.0) * 10000.0 * mult
    exit_idx = np.where(check_bps <= 0.0, check_idx, end_idx)
    gross = ((close[exit_idx] / entry) - 1.0) * 10000.0 * mult
    costs = np.asarray(
        [float(symbol_costs.get(str(symbol), 18.0)) for symbol in symbols[idx]], dtype=float
    )
    net = gross - costs
    plus_10 = net - extra_slippage_bps
    event_ts = pd.to_datetime(pd.Series(context["timestamp"][idx]), utc=True, errors="coerce")
    funding_bucket = event_ts.dt.strftime("%H:%M")
    symbol_labels = symbols[idx].astype(str)
    month_labels = context["months"][idx].astype(str)
    events = pd.DataFrame(
        {
            "symbol": symbol_labels,
            "event_ts": event_ts.dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "direction": directions.astype(str),
            "exit_policy": exit_policy,
            "exit_ts": pd.to_datetime(
                pd.Series(context["timestamp"][exit_idx]), utc=True, errors="coerce"
            ).dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "hold_bars": (exit_idx - idx).astype(int),
            "gross_bps": gross,
            "net_bps": net,
            "plus_10_bps_net_bps": plus_10,
            "year": context["years"][idx].astype(str),
            "month": month_labels,
            "symbol_month": np.asarray(
                [
                    f"{symbol}:{month}"
                    for symbol, month in zip(symbol_labels, month_labels, strict=False)
                ],
                dtype=object,
            ),
            "funding_sign": context["funding_sign"][idx].astype(str),
            "funding_timestamp_utc": funding_bucket,
        }
    )
    events["event_fingerprint"] = _fingerprint_frame(events)
    return events


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol",
            "event_ts",
            "direction",
            "exit_policy",
            "exit_ts",
            "hold_bars",
            "gross_bps",
            "net_bps",
            "plus_10_bps_net_bps",
            "year",
            "month",
            "symbol_month",
            "funding_sign",
            "funding_timestamp_utc",
            "event_fingerprint",
        ]
    )


def _build_events_for_params(
    context: dict[str, Any],
    *,
    spec: dict[str, Any],
    range_session: str,
    trade_session: str,
    breakout_buffer_bps: float,
    volume_mode: str,
    oi_mode: str,
    trend_filter: str,
    exit_policy: str,
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
    context_override: dict[str, Any] | None = None,
    invert_direction: bool = False,
    non_funding_hours_only: bool = False,
) -> pd.DataFrame:
    effective_context = context if context_override is None else context_override
    if non_funding_hours_only:
        mask, directions = _funding_drift_mask(
            effective_context,
            spec=spec,
            range_session=range_session,
            trade_session=trade_session,
            volume_mode=volume_mode,
            oi_mode=oi_mode,
            trend_filter=trend_filter,
            funding_hours_only=False,
        )
    else:
        mask, directions = session_lab._variant_mask(
            effective_context,
            spec=spec,
            range_session=range_session,
            trade_session=trade_session,
            breakout_buffer_bps=breakout_buffer_bps,
            volume_mode=volume_mode,
            oi_mode=oi_mode,
            trend_filter=trend_filter,
        )
    indices = effective_context["eval_indices"][mask]
    event_directions = directions[mask]
    if invert_direction:
        event_directions = np.where(event_directions == "long", "short", "long").astype(object)
    events = _simulate_events(
        effective_context,
        indices,
        directions=event_directions,
        exit_policy=exit_policy,
        symbol_costs=symbol_costs,
        extra_slippage_bps=extra_slippage_bps,
    )
    return events


def _funding_drift_mask(
    context: dict[str, Any],
    *,
    spec: dict[str, Any],
    range_session: str,
    trade_session: str,
    volume_mode: str,
    oi_mode: str,
    trend_filter: str,
    funding_hours_only: bool,
) -> tuple[np.ndarray, np.ndarray]:
    eval_indices = context["eval_indices"]
    close = context["close"][eval_indices]
    range_high, range_low = session_lab._range_values(context, range_session, eval_indices)
    session = context["session"][eval_indices]
    trend = context["trend"][eval_indices]
    mask = (
        (session == trade_session)
        & np.isfinite(close)
        & np.isfinite(range_high)
        & np.isfinite(range_low)
        & (range_high > range_low)
    )
    if (
        range_session not in spec["allowed_range_sessions"]
        or trade_session not in spec["allowed_trade_sessions"]
    ):
        mask &= False
    funding = context["funding_sign"][eval_indices]
    funding_clock = np.isin(context["hour"][eval_indices], FUNDING_WINDOW_HOURS) & (
        context["minute"][eval_indices] == 0
    )
    mask &= funding_clock if funding_hours_only else ~funding_clock
    mask &= (funding == "positive") | (funding == "negative")
    directions = np.where(funding == "negative", "long", "short").astype(object)
    if volume_mode == "required":
        mask &= context["volume_z"][eval_indices] >= 1.0
    elif volume_mode != "optional":
        raise ValueError(f"unsupported volume mode: {volume_mode}")
    if oi_mode == "aligned":
        mask &= context["oi_change"][eval_indices] > 0.0
    elif oi_mode != "optional":
        raise ValueError(f"unsupported OI mode: {oi_mode}")
    if trend_filter == "aligned":
        long_trade = directions == "long"
        aligned = (long_trade & (trend == "uptrend")) | (~long_trade & (trend == "downtrend"))
        mask &= aligned
    elif trend_filter != "any":
        raise ValueError(f"unsupported trend filter: {trend_filter}")
    return mask, directions


def _context_with_random_funding_signs(context: dict[str, Any]) -> dict[str, Any]:
    rng = np.random.default_rng(PLACEBO_SEED)
    out = dict(context)
    signs = context["funding_sign"].astype(object).copy()
    observed = signs[np.isin(signs, ["positive", "negative"])]
    if len(observed) == 0:
        observed = np.asarray(["positive", "negative"], dtype=object)
    signs[context["eval_indices"]] = rng.choice(observed, size=len(context["eval_indices"]))
    out["funding_sign"] = signs
    return out


def _context_with_shifted_funding_signs(context: dict[str, Any], *, bars: int) -> dict[str, Any]:
    out = dict(context)
    shifted = context["funding_sign"].astype(object).copy()
    for symbol in sorted(set(context["symbols"].astype(str).tolist())):
        loc = np.flatnonzero(context["symbols"].astype(str) == symbol)
        original = shifted[loc].copy()
        if len(original) <= bars:
            shifted[loc] = "zero"
            continue
        shifted[loc[:bars]] = "zero"
        shifted[loc[bars:]] = original[:-bars]
    out["funding_sign"] = shifted
    return out


def _variant_rows(
    *,
    variant: str,
    breakout_buffer_bps: list[float],
    exit_policies: list[str],
) -> list[dict[str, Any]]:
    spec = _spec_for_variant(variant)
    rows = []
    for range_session in ("asia", "europe"):
        for trade_session in ("europe", "us", "late_us"):
            for buffer_bps in breakout_buffer_bps:
                for volume_mode in ("optional", "required"):
                    for oi_mode in ("optional", "aligned"):
                        for trend_filter in ("any", "aligned"):
                            for exit_policy in exit_policies:
                                rows.append(  # noqa: PERF401
                                    {
                                        "spec": spec,
                                        "variant_id": _variant_id(
                                            variant=variant,
                                            range_session=range_session,
                                            trade_session=trade_session,
                                            breakout_buffer_bps=buffer_bps,
                                            volume_mode=volume_mode,
                                            oi_mode=oi_mode,
                                            trend_filter=trend_filter,
                                            exit_policy=exit_policy,
                                        ),
                                        "range_session": range_session,
                                        "trade_session": trade_session,
                                        "breakout_buffer_bps": buffer_bps,
                                        "volume_mode": volume_mode,
                                        "oi_mode": oi_mode,
                                        "trend_filter": trend_filter,
                                        "exit_policy": exit_policy,
                                    }
                                )
    return rows


def _metrics(events: pd.DataFrame, *, extra_slippage_bps: float) -> dict[str, Any]:
    del extra_slippage_bps
    if events.empty:
        return {
            "event_count": 0,
            "net_bps": None,
            "gross_bps": None,
            "t_stat": None,
            "cost_survival": None,
            "positive_symbols": [],
            "positive_symbol_count": 0,
            "top_symbol_month_share": None,
            "walk_forward_pass": False,
            "slippage_plus_10_bps": {"net_bps": None, "t_stat": None, "survives": False},
            "passes_core_gates": False,
        }
    net_summary = _summary(events["net_bps"])
    gross_summary = _summary(events["gross_bps"])
    by_symbol = _group_stats(events, "symbol")
    by_year = _group_stats(events, "year")
    positive_symbols = sorted(
        symbol for symbol, stats in by_symbol.items() if (stats.get("net_bps") or -1e9) > 0.0
    )
    top_share = float(events["symbol_month"].value_counts(normalize=True).iloc[0])
    plus_summary = _summary(events["plus_10_bps_net_bps"])
    result = {
        "event_count": len(events),
        "net_bps": net_summary.get("net_bps"),
        "gross_bps": gross_summary.get("net_bps"),
        "t_stat": net_summary.get("t_stat"),
        "total_net_bps": net_summary.get("total_net_bps"),
        "cost_survival": _cost_survival(events),
        "positive_symbols": positive_symbols,
        "positive_symbol_count": len(positive_symbols),
        "top_symbol_month_share": top_share,
        "walk_forward_pass": session_lab._walk_forward_pass(by_year),
        "slippage_plus_10_bps": {
            "net_bps": plus_summary.get("net_bps"),
            "t_stat": plus_summary.get("t_stat"),
            "survives": (plus_summary.get("net_bps") or -1e9) > 0.0,
        },
    }
    result["passes_core_gates"] = _passes_core_gates(result)
    return result


def _passes_core_gates(metrics: dict[str, Any]) -> bool:
    return (
        int(metrics.get("event_count") or 0) >= 100
        and (metrics.get("net_bps") or -1e9) > 0.0
        and (metrics.get("t_stat") or -1e9) > 2.0
        and (metrics.get("cost_survival") or -1e9) >= 0.8
        and int(metrics.get("positive_symbol_count") or 0) >= 3
        and (metrics.get("top_symbol_month_share") or 1.0) <= 0.35
        and bool(metrics.get("walk_forward_pass"))
        and bool((metrics.get("slippage_plus_10_bps") or {}).get("survives"))
    )


def _group_stats(events: pd.DataFrame, column: str) -> dict[str, Any]:
    if events.empty:
        return {}
    return {
        str(label): _summary(group["net_bps"])
        for label, group in events.groupby(column, sort=True, dropna=False)
    }


def _funding_timestamp_stats(events: pd.DataFrame) -> dict[str, Any]:
    stats = _group_stats(events, "funding_timestamp_utc")
    for bucket in ("00:00", "08:00", "16:00"):
        stats.setdefault(bucket, _summary([]))
    return dict(sorted(stats.items()))


def _robustness(events: pd.DataFrame, *, extra_slippage_bps: float) -> dict[str, Any]:
    if events.empty:
        return {
            "remove_best_month": {
                "removed": None,
                "metrics": _metrics(events, extra_slippage_bps=extra_slippage_bps),
            },
            "remove_top_symbol_month": {
                "removed": None,
                "metrics": _metrics(events, extra_slippage_bps=extra_slippage_bps),
            },
        }
    month_pnl = events.groupby("month", sort=True)["net_bps"].sum()
    best_month = str(month_pnl.idxmax())
    top_symbol_month = str(events["symbol_month"].value_counts().idxmax())
    without_best_month = events[events["month"] != best_month]
    without_top_symbol_month = events[events["symbol_month"] != top_symbol_month]
    return {
        "remove_best_month": {
            "removed": best_month,
            "removed_total_net_bps": float(month_pnl.loc[best_month]),
            "metrics": _metrics(without_best_month, extra_slippage_bps=extra_slippage_bps),
            "still_positive": (
                _metrics(without_best_month, extra_slippage_bps=extra_slippage_bps).get("net_bps")
                or -1e9
            )
            > 0.0,
        },
        "remove_top_symbol_month": {
            "removed": top_symbol_month,
            "removed_event_count": int((events["symbol_month"] == top_symbol_month).sum()),
            "metrics": _metrics(without_top_symbol_month, extra_slippage_bps=extra_slippage_bps),
            "still_positive": (
                _metrics(without_top_symbol_month, extra_slippage_bps=extra_slippage_bps).get(
                    "net_bps"
                )
                or -1e9
            )
            > 0.0,
        },
    }


def _dedupe_groups(variant_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_digest: dict[str, list[dict[str, Any]]] = {}
    for item in variant_events:
        by_digest.setdefault(item["fingerprint_digest"], []).append(item)
    groups = []
    for digest, items in sorted(
        by_digest.items(),
        key=lambda pair: (
            -max((item["metrics"].get("net_bps") or -1e9) for item in pair[1]),
            pair[0],
        ),
    ):
        sorted_items = sorted(
            items,
            key=lambda item: (
                -(item["metrics"].get("event_count") or 0),
                -(item["metrics"].get("net_bps") or -1e9),
                item["variant_id"],
            ),
        )
        canonical = sorted_items[0]
        groups.append(
            {
                "canonical_variant": canonical["variant_id"],
                "duplicate_variant_count": len(sorted_items) - 1,
                "duplicate_variant_ids": [item["variant_id"] for item in sorted_items[1:]],
                "same_event_set": True,
                "event_count": canonical["metrics"]["event_count"],
                "fingerprint_digest": digest,
                "metrics": canonical["metrics"],
                "params": canonical["params"],
            }
        )
    return groups


def _select_group(groups: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not groups:
        return None
    passing = [group for group in groups if group["metrics"].get("passes_core_gates")]
    pool = passing or groups
    return sorted(
        pool,
        key=lambda group: (
            -(group["metrics"].get("net_bps") or -1e9),
            -(group["metrics"].get("t_stat") or -1e9),
            group["canonical_variant"],
        ),
    )[0]


def _placebo_reports(
    context: dict[str, Any],
    *,
    selected_params: dict[str, Any],
    symbol_costs: dict[str, float],
    extra_slippage_bps: float,
) -> dict[str, Any]:
    spec = selected_params["spec"]
    base_kwargs = {
        "context": context,
        "spec": spec,
        "range_session": selected_params["range_session"],
        "trade_session": selected_params["trade_session"],
        "breakout_buffer_bps": selected_params["breakout_buffer_bps"],
        "volume_mode": selected_params["volume_mode"],
        "oi_mode": selected_params["oi_mode"],
        "trend_filter": selected_params["trend_filter"],
        "exit_policy": selected_params["exit_policy"],
        "symbol_costs": symbol_costs,
        "extra_slippage_bps": extra_slippage_bps,
    }
    random_events = _build_events_for_params(
        **base_kwargs,
        context_override=_context_with_random_funding_signs(context),
    )
    shifted_events = _build_events_for_params(
        **base_kwargs,
        context_override=_context_with_shifted_funding_signs(context, bars=288),
    )
    inverted_events = _build_events_for_params(**base_kwargs, invert_direction=True)
    non_funding_events = _build_events_for_params(**base_kwargs, non_funding_hours_only=True)
    return {
        "same_sessions_random_funding_signs": _metrics(
            random_events, extra_slippage_bps=extra_slippage_bps
        ),
        "same_funding_signs_shifted_plus_1_day": _metrics(
            shifted_events, extra_slippage_bps=extra_slippage_bps
        ),
        "same_timestamps_inverted_direction": _metrics(
            inverted_events, extra_slippage_bps=extra_slippage_bps
        ),
        "non_funding_hours_only": _metrics(
            non_funding_events, extra_slippage_bps=extra_slippage_bps
        ),
    }


def build_diagnosis(
    *,
    repo_root: Path,
    symbols: list[str],
    years: list[int],
    variant: str,
    breakout_buffer_bps: list[float],
    exit_policies: list[str],
    extra_slippage_bps: float,
    cost_overrides: dict[str, float],
    json_output: Path,
    events_output: Path,
) -> tuple[dict[str, Any], pd.DataFrame]:
    spec = _spec_for_variant(variant)
    if spec["shape"] != "funding_drift":
        raise ValueError(
            "detector-session-diagnose currently supports funding-window variants only"
        )
    raw, missing, input_summary = session_lab._load_frames(repo_root, symbols, years)
    context = session_lab._array_context(
        raw, candidate_stride_bars=session_lab.DEFAULT_COOLDOWN_BARS
    )
    symbol_costs = {
        symbol: session_lab._cost_for_symbol(symbol, cost_overrides) for symbol in symbols
    }

    variant_events = []
    for params in _variant_rows(
        variant=variant,
        breakout_buffer_bps=breakout_buffer_bps,
        exit_policies=exit_policies,
    ):
        events = _build_events_for_params(
            context,
            spec=params["spec"],
            range_session=params["range_session"],
            trade_session=params["trade_session"],
            breakout_buffer_bps=params["breakout_buffer_bps"],
            volume_mode=params["volume_mode"],
            oi_mode=params["oi_mode"],
            trend_filter=params["trend_filter"],
            exit_policy=params["exit_policy"],
            symbol_costs=symbol_costs,
            extra_slippage_bps=extra_slippage_bps,
        )
        fingerprints = sorted(events["event_fingerprint"].astype(str).tolist())
        metrics = _metrics(events, extra_slippage_bps=extra_slippage_bps)
        params_without_spec = {key: value for key, value in params.items() if key != "spec"}
        variant_events.append(
            {
                "variant_id": params["variant_id"],
                "fingerprint_digest": _fingerprint_digest(fingerprints),
                "fingerprints": fingerprints,
                "metrics": metrics,
                "params": params_without_spec,
                "events": events,
            }
        )

    groups = _dedupe_groups(variant_events)
    selected_group = _select_group(groups)
    selected_variant_id = selected_group["canonical_variant"] if selected_group else None
    selected_record = next(
        (item for item in variant_events if item["variant_id"] == selected_variant_id),
        None,
    )
    canonical_events = (
        _empty_events() if selected_record is None else selected_record["events"].copy()
    )
    if selected_record is not None:
        canonical_events.insert(0, "canonical_variant", selected_record["variant_id"])
        canonical_events["duplicate_variant_count"] = selected_group["duplicate_variant_count"]
        canonical_events["duplicate_variant_ids"] = ",".join(
            selected_group["duplicate_variant_ids"]
        )
        for key, value in selected_record["params"].items():
            canonical_events[key] = value

    characterizations = {
        "by_symbol": _group_stats(canonical_events, "symbol"),
        "by_year": _group_stats(canonical_events, "year"),
        "by_month": _group_stats(canonical_events, "month"),
        "by_funding_sign": _group_stats(canonical_events, "funding_sign"),
        "by_funding_timestamp": _funding_timestamp_stats(canonical_events),
    }
    robustness = _robustness(canonical_events, extra_slippage_bps=extra_slippage_bps)
    placebo = (
        {}
        if selected_record is None
        else _placebo_reports(
            context,
            selected_params={
                **selected_record["params"],
                "spec": spec,
            },
            symbol_costs=symbol_costs,
            extra_slippage_bps=extra_slippage_bps,
        )
    )
    placebo_does_not_pass = not any(
        bool(metrics.get("passes_core_gates")) for metrics in placebo.values()
    )
    canonical_metrics = _metrics(canonical_events, extra_slippage_bps=extra_slippage_bps)
    gate_check = {
        "deduped_candidate_still_passes": canonical_metrics["passes_core_gates"],
        "event_count": int(canonical_metrics["event_count"]),
        "event_count_gte_100": int(canonical_metrics["event_count"]) >= 100,
        "net_positive": (canonical_metrics.get("net_bps") or -1e9) > 0.0,
        "t_stat_gt_2": (canonical_metrics.get("t_stat") or -1e9) > 2.0,
        "cost_survival_gte_0_8": (canonical_metrics.get("cost_survival") or -1e9) >= 0.8,
        "positive_symbols_gte_3": int(canonical_metrics.get("positive_symbol_count") or 0) >= 3,
        "top_symbol_month_share_lte_0_35": (canonical_metrics.get("top_symbol_month_share") or 1.0)
        <= 0.35,
        "walk_forward_passes": bool(canonical_metrics.get("walk_forward_pass")),
        "plus_10_bps_survives": bool(
            (canonical_metrics.get("slippage_plus_10_bps") or {}).get("survives")
        ),
        "minus_top_month_still_positive": bool(
            robustness["remove_best_month"].get("still_positive")
        ),
        "minus_top_symbol_month_still_positive": bool(
            robustness["remove_top_symbol_month"].get("still_positive")
        ),
        "placebo_does_not_pass": placebo_does_not_pass,
    }
    gate_check["all_pass"] = all(
        bool(value) for key, value in gate_check.items() if key not in {"event_count"}
    )
    regime_artifact = (
        (canonical_metrics.get("top_symbol_month_share") or 0.0) > 0.35
        or not gate_check["minus_top_month_still_positive"]
        or not gate_check["minus_top_symbol_month_still_positive"]
    )

    report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "variant": variant,
        "family": session_lab.FAMILY,
        "decision": "research_only_do_not_promote_do_not_trade",
        "scope": {
            "symbols_requested": symbols,
            "symbols_evaluated": sorted(raw["symbol"].unique().tolist()),
            "years": years,
            "timeframe": "5m",
            "data_scope": ["OHLCV", "open_interest", "funding"],
            "funding_window_logic": "actual UTC timestamp hour/minute in {00:00,08:00,16:00}",
            "event_fingerprint": list(FINGERPRINT_COLUMNS),
            "extra_slippage_bps": extra_slippage_bps,
            "cost_bps_by_symbol": symbol_costs,
        },
        "input_summary": input_summary,
        "missing_symbol_data": missing,
        "dedupe": {
            "variant_count": len(variant_events),
            "deduped_group_count": len(groups),
            "groups": groups,
            "selected_group": selected_group,
        },
        "canonical_metrics": canonical_metrics,
        "characterizations": characterizations,
        "top_symbol_month_share": canonical_metrics.get("top_symbol_month_share"),
        "validation_by_year": characterizations["by_year"],
        "remove_best_month_robustness": robustness["remove_best_month"],
        "remove_top_symbol_month_robustness": robustness["remove_top_symbol_month"],
        "placebos": placebo,
        "regime_artifact_research_only": bool(regime_artifact),
        "promotion_gate_check": gate_check,
        "paper_approved_events": [],
        "live_approved_events": [],
    }

    json_output.parent.mkdir(parents=True, exist_ok=True)
    events_output.parent.mkdir(parents=True, exist_ok=True)
    json_output.write_text(
        json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8"
    )
    canonical_events.to_csv(events_output, index=False)
    return report, canonical_events


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose session funding-window drift lead")
    parser.add_argument("--variant", default=DEFAULT_VARIANT)
    parser.add_argument("--symbols", default=",".join(session_lab.DEFAULT_SYMBOLS))
    parser.add_argument(
        "--years", default=",".join(str(year) for year in session_lab.DEFAULT_YEARS)
    )
    parser.add_argument(
        "--breakout-buffer-bps",
        default=",".join(str(value) for value in session_lab.DEFAULT_BREAKOUT_BUFFER_BPS),
    )
    parser.add_argument("--exit-policies", default=",".join(session_lab.DEFAULT_EXIT_POLICIES))
    parser.add_argument(
        "--extra-slippage-bps", type=float, default=session_lab.DEFAULT_EXTRA_SLIPPAGE_BPS
    )
    parser.add_argument("--cost-overrides", default="")
    parser.add_argument("--json-output", default=str(DEFAULT_JSON_OUTPUT))
    parser.add_argument("--events-output", default=str(DEFAULT_EVENTS_OUTPUT))
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    report, events = build_diagnosis(
        repo_root=repo_root,
        symbols=[item.upper() for item in _parse_csv(args.symbols)],
        years=_parse_ints(args.years),
        variant=args.variant,
        breakout_buffer_bps=[float(item) for item in _parse_csv(args.breakout_buffer_bps)],
        exit_policies=_parse_csv(args.exit_policies),
        extra_slippage_bps=args.extra_slippage_bps,
        cost_overrides=_parse_cost_overrides(args.cost_overrides),
        json_output=repo_root / args.json_output,
        events_output=repo_root / args.events_output,
    )
    print(
        json.dumps(
            {
                "status": "pass",
                "json_output": str(repo_root / args.json_output),
                "events_output": str(repo_root / args.events_output),
                "variant": report["variant"],
                "canonical_variant": (report["dedupe"]["selected_group"] or {}).get(
                    "canonical_variant"
                ),
                "event_count": len(events),
                "deduped_group_count": report["dedupe"]["deduped_group_count"],
                "promotion_gate_all_pass": report["promotion_gate_check"]["all_pass"],
                "placebo_does_not_pass": report["promotion_gate_check"]["placebo_does_not_pass"],
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
