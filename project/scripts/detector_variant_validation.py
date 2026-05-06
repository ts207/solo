from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from project.events.data_capabilities import load_data_capability_profile
from project.scripts.detector_shadow_report import (
    DEFAULT_HORIZONS,
    _attach_forward_returns,
    _build_composites,
    _mark_event_column,
    _prepare_symbol_frame,
    _return_summary,
    _run_detector,
)
from project.events.detectors.exhaustion import FailedContinuationDetector
from project.events.detectors.liquidity_base import LiquidityVacuumDetectorV2, LiquidityVacuumRecoveryDetectorV2
from project.events.detectors.positioning_base import (
    FundingExtremeOnsetDetectorV2,
    FundingNegExtremeOnsetDetectorV2,
    FundingPosExtremeOnsetDetectorV2,
    OIExpansionStressDetectorV2,
    OIFlushDetectorV2,
    OISpikeNegativeDetectorV2,
)
from project.events.detectors.volatility_base import VolShockDetectorV2


FIRST_PASS = [
    ("VOL_SHOCK", VolShockDetectorV2),
    ("FAILED_CONTINUATION", FailedContinuationDetector),
    ("LIQUIDITY_VACUUM", LiquidityVacuumDetectorV2),
    ("LIQUIDITY_VACUUM_RECOVERY", LiquidityVacuumRecoveryDetectorV2),
]
SECOND_PASS = [
    ("OI_FLUSH", OIFlushDetectorV2),
    ("OI_EXPANSION_STRESS", OIExpansionStressDetectorV2),
    ("OI_SPIKE_NEGATIVE", OISpikeNegativeDetectorV2),
    ("FUNDING_EXTREME_ONSET", FundingExtremeOnsetDetectorV2),
    ("FUNDING_POS_EXTREME_ONSET", FundingPosExtremeOnsetDetectorV2),
    ("FUNDING_NEG_EXTREME_ONSET", FundingNegExtremeOnsetDetectorV2),
]
DIRECTIONS = ("configured", "inverted", "long", "short")


def _parse_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _parse_years(value: str) -> list[int]:
    return [int(item) for item in _parse_csv(value)]


def _direction_net_key(direction: str, horizon: int) -> str:
    token = str(direction).strip().lower()
    if token == "inverted":
        return f"fwd_{horizon}b_inverted_net_bps"
    if token == "long":
        return f"fwd_{horizon}b_long_net_bps"
    if token == "short":
        return f"fwd_{horizon}b_short_net_bps"
    return f"fwd_{horizon}b_net_bps"


def _direction_gross_key(direction: str, horizon: int) -> str:
    token = str(direction).strip().lower()
    if token == "inverted":
        return f"fwd_{horizon}b_inverted_bps"
    if token == "long":
        return f"fwd_{horizon}b_long_bps"
    if token == "short":
        return f"fwd_{horizon}b_short_bps"
    return f"fwd_{horizon}b_bps"


def _top_share(values: list[str]) -> float | None:
    if not values:
        return None
    return float(Counter(values).most_common(1)[0][1] / len(values))


def _build_events(
    *,
    repo_root: Path,
    profile: Any,
    symbols: list[str],
    years: list[int],
    horizons: list[int],
    cost_bps: float,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, list[dict[str, str]]], list[dict[str, Any]]]:
    all_events: list[dict[str, Any]] = []
    input_summary: dict[str, Any] = {}
    detector_errors: dict[str, list[dict[str, str]]] = defaultdict(list)
    missing_feed_blockers: list[dict[str, Any]] = []
    for symbol in symbols:
        frame = _prepare_symbol_frame(repo_root, symbol, years)
        input_summary[symbol] = {
            "rows": int(len(frame)),
            "start": str(frame["timestamp"].min()),
            "end": str(frame["timestamp"].max()),
            "has_depth_usd": bool("depth_usd" in frame.columns),
            "has_spread_bps": bool("spread_bps" in frame.columns),
        }
        symbol_events: list[dict[str, Any]] = []
        for event_id, detector_cls in FIRST_PASS:
            rows, error = _run_detector(detector_cls, event_id, frame, symbol)
            if error:
                detector_errors[event_id].append({"symbol": symbol, "error": error})
            symbol_events.extend(rows)
        _mark_event_column(frame, symbol_events, "VOL_SHOCK", "vol_shock")
        _mark_event_column(frame, symbol_events, "FAILED_CONTINUATION", "failed_continuation")
        _mark_event_column(frame, symbol_events, "LIQUIDITY_VACUUM", "liquidity_vacuum")
        _mark_event_column(frame, symbol_events, "LIQUIDITY_VACUUM_RECOVERY", "liquidity_vacuum_recovery")
        for event_id, detector_cls in SECOND_PASS:
            rows, error = _run_detector(detector_cls, event_id, frame, symbol)
            if error:
                detector_errors[event_id].append({"symbol": symbol, "error": error})
            symbol_events.extend(rows)
        symbol_events.extend(
            _build_composites(
                symbol=symbol,
                events=symbol_events,
                profile=profile,
                data_feeds_available={column for column in ("depth_usd", "spread_bps") if column in frame.columns},
                missing_feed_blockers=missing_feed_blockers,
            )
        )
        _attach_forward_returns(symbol_events, frame, horizons, cost_bps)
        all_events.extend(symbol_events)
    return all_events, input_summary, detector_errors, missing_feed_blockers


def _subset_summary(rows: list[dict[str, Any]], net_key: str, gross_key: str) -> dict[str, Any]:
    net_values = [event.get(net_key, np.nan) for event in rows]
    gross_values = [event.get(gross_key, np.nan) for event in rows]
    net_summary = _return_summary(net_values)
    gross_summary = _return_summary(gross_values)
    net_mean = net_summary.get("mean_bps")
    gross_mean = gross_summary.get("mean_bps")
    cost_survival = (
        float(net_mean / gross_mean)
        if gross_mean is not None and gross_mean > 0.0 and net_mean is not None
        else None
    )
    by_symbol = {
        key: _return_summary([event.get(net_key, np.nan) for event in rows if event["symbol"] == key])
        for key in sorted({event["symbol"] for event in rows})
    }
    by_year = {
        key: _return_summary([event.get(net_key, np.nan) for event in rows if event["year"] == key])
        for key in sorted({event["year"] for event in rows})
    }
    by_month = {
        key: _return_summary([event.get(net_key, np.nan) for event in rows if event["month"] == key])
        for key in sorted({event["month"] for event in rows})
    }
    data_quality = dict(Counter(event["data_quality_flag"] for event in rows))
    return {
        "event_count": len(rows),
        "net_bps": net_mean,
        "gross_bps": gross_mean,
        "t_stat": net_summary.get("t_stat"),
        "cost_survival_ratio": cost_survival,
        "top_month_share": _top_share([event["month"] for event in rows]),
        "data_quality_flag_distribution": data_quality,
        "by_symbol": by_symbol,
        "by_year": by_year,
        "by_month": by_month,
    }


def _gate(summary: dict[str, Any], *, years: list[int], min_events: int, multi_symbol: bool) -> tuple[bool, list[str]]:
    failures: list[str] = []
    if int(summary.get("event_count") or 0) < min_events:
        failures.append("insufficient_events")
    if summary.get("net_bps") is None or float(summary["net_bps"]) <= 0.0:
        failures.append("net_bps_not_positive")
    if summary.get("t_stat") is None or float(summary["t_stat"]) <= 2.0:
        failures.append("t_stat_not_above_2")
    if summary.get("cost_survival_ratio") is None or float(summary["cost_survival_ratio"]) < 0.8:
        failures.append("cost_survival_below_0_8")
    if summary.get("top_month_share") is None or float(summary["top_month_share"]) > 0.5:
        failures.append("month_concentration_too_high")
    if set(summary.get("data_quality_flag_distribution", {}).keys()) - {"ok"}:
        failures.append("data_quality_not_clean")
    if multi_symbol:
        by_symbol = dict(summary.get("by_symbol") or {})
        if not by_symbol or not all(float((value or {}).get("mean_bps") or 0.0) > 0.0 for value in by_symbol.values()):
            failures.append("symbol_scope_not_positive")
    by_year = dict(summary.get("by_year") or {})
    if len(years) > 1:
        if sum(1 for value in by_year.values() if float((value or {}).get("mean_bps") or 0.0) > 0.0) < 2:
            failures.append("year_split_not_positive")
    elif not by_year or not all(float((value or {}).get("mean_bps") or 0.0) > 0.0 for value in by_year.values()):
        failures.append("year_split_not_positive")
    return not failures, failures


def _filter_specs(discovery_events: list[dict[str, Any]], base_event: str) -> list[dict[str, str]]:
    rows = [event for event in discovery_events if event["event_id"] == base_event]
    specs = [{"type": "none", "value": "all"}]
    for field, label in (("regime", "vol_regime"), ("funding_sign", "funding_sign"), ("oi_subtype", "oi_subtype")):
        counts = Counter(str(event.get(field, "unknown")) for event in rows)
        for value, count in sorted(counts.items()):
            if value and value != "unknown" and count >= 100:
                specs.append({"type": label, "field": field, "value": value})
    return specs


def _apply_filter(rows: list[dict[str, Any]], spec: dict[str, str]) -> list[dict[str, Any]]:
    if spec["type"] == "none":
        return rows
    return [event for event in rows if str(event.get(spec["field"], "unknown")) == spec["value"]]


def _status(discovery_pass: bool, validation_pass: bool, direction: str) -> str:
    if discovery_pass and validation_pass and direction == "configured":
        return "paper_eligible_candidate"
    if discovery_pass and validation_pass:
        return "fresh_validation_passed_needs_explicit_variant"
    if discovery_pass:
        return "discovery_only_failed_holdout"
    if validation_pass:
        return "validation_only_no_discovery"
    return "failed_strict_gates"


def build_validation_report(
    *,
    repo_root: Path,
    profile_name: str,
    symbols: list[str],
    discovery_years: list[int],
    validation_years: list[int],
    horizons: list[int],
    cost_bps: float,
    min_events: int,
) -> dict[str, Any]:
    profile = load_data_capability_profile(profile_name)
    active_events = [event_id for event_id in sorted(profile.trade_candidate_events) if profile.trade_candidate(event_id)]
    discovery_events, discovery_inputs, discovery_errors, discovery_blockers = _build_events(
        repo_root=repo_root,
        profile=profile,
        symbols=symbols,
        years=discovery_years,
        horizons=horizons,
        cost_bps=cost_bps,
    )
    validation_events, validation_inputs, validation_errors, validation_blockers = _build_events(
        repo_root=repo_root,
        profile=profile,
        symbols=symbols,
        years=validation_years,
        horizons=horizons,
        cost_bps=cost_bps,
    )
    symbol_scopes = [[symbol] for symbol in symbols]
    if len(symbols) > 1:
        symbol_scopes.append(symbols)

    candidates: list[dict[str, Any]] = []
    for event_id in active_events:
        filters = _filter_specs(discovery_events, event_id)
        for symbol_scope in symbol_scopes:
            multi_symbol = len(symbol_scope) > 1
            for horizon in horizons:
                for direction in DIRECTIONS:
                    net_key = _direction_net_key(direction, horizon)
                    gross_key = _direction_gross_key(direction, horizon)
                    for filter_spec in filters:
                        discovery_rows = [
                            event
                            for event in discovery_events
                            if event["event_id"] == event_id and event["symbol"] in set(symbol_scope)
                        ]
                        validation_rows = [
                            event
                            for event in validation_events
                            if event["event_id"] == event_id and event["symbol"] in set(symbol_scope)
                        ]
                        discovery_rows = _apply_filter(discovery_rows, filter_spec)
                        validation_rows = _apply_filter(validation_rows, filter_spec)
                        discovery_summary = _subset_summary(discovery_rows, net_key, gross_key)
                        validation_summary = _subset_summary(validation_rows, net_key, gross_key)
                        discovery_pass, discovery_failures = _gate(
                            discovery_summary,
                            years=discovery_years,
                            min_events=min_events,
                            multi_symbol=multi_symbol,
                        )
                        validation_pass, validation_failures = _gate(
                            validation_summary,
                            years=validation_years,
                            min_events=min_events,
                            multi_symbol=multi_symbol,
                        )
                        status = _status(discovery_pass, validation_pass, direction)
                        if status == "failed_strict_gates":
                            continue
                        variant_id = "_".join(
                            [
                                event_id,
                                f"H{horizon}",
                                direction.upper(),
                                "_".join(symbol_scope),
                                filter_spec["type"].upper(),
                                str(filter_spec["value"]).upper(),
                            ]
                        )
                        candidates.append(
                            {
                                "variant_id": variant_id,
                                "base_event": event_id,
                                "symbol_scope": symbol_scope,
                                "horizon_bars": horizon,
                                "bar_interval": "5m",
                                "direction": direction,
                                "filter": filter_spec,
                                "status": status,
                                "paper_approved": False,
                                "live_approved": False,
                                "gate": {
                                    "min_events": min_events,
                                    "net_bps_gt": 0,
                                    "t_stat_gt": 2,
                                    "cost_survival_ratio_gte": 0.8,
                                    "top_month_share_lte": 0.5,
                                },
                                "discovery": {
                                    **discovery_summary,
                                    "pass": discovery_pass,
                                    "failures": discovery_failures,
                                },
                                "validation": {
                                    **validation_summary,
                                    "pass": validation_pass,
                                    "failures": validation_failures,
                                },
                            }
                        )
    order = {
        "paper_eligible_candidate": 0,
        "fresh_validation_passed_needs_explicit_variant": 1,
        "discovery_only_failed_holdout": 2,
        "validation_only_no_discovery": 3,
    }
    candidates.sort(
        key=lambda item: (
            order.get(item["status"], 9),
            -(float(item["validation"].get("net_bps") or -math.inf)),
            -(float(item["validation"].get("t_stat") or -math.inf)),
        )
    )
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "profile": profile.name,
        "scope": {
            "symbols": symbols,
            "discovery_years": discovery_years,
            "validation_years": validation_years,
            "timeframe": "5m",
            "horizons": horizons,
            "cost_bps": cost_bps,
        },
        "input_summary": {"discovery": discovery_inputs, "validation": validation_inputs},
        "errors_or_skips": {"discovery": dict(discovery_errors), "validation": dict(validation_errors)},
        "missing_feed_blockers": {"discovery": discovery_blockers, "validation": validation_blockers},
        "killed_events": sorted(profile.killed_events),
        "rejected_events": sorted(profile.rejected_events),
        "active_trade_candidate_events": active_events,
        "candidate_count": len(candidates),
        "strict_pass_count": sum(1 for item in candidates if item["status"] == "paper_eligible_candidate"),
        "explicit_variant_pass_count": sum(
            1 for item in candidates if item["status"] == "fresh_validation_passed_needs_explicit_variant"
        ),
        "candidates": candidates[:500],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate detector research variants across a fixed discovery/holdout split")
    parser.add_argument("--profile", default="no_liquidations_v1")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT")
    parser.add_argument("--discovery-years", default="2022,2023")
    parser.add_argument("--validation-years", default="2024")
    parser.add_argument("--horizons", default=",".join(str(horizon) for horizon in DEFAULT_HORIZONS))
    parser.add_argument("--cost-bps", type=float, default=6.0)
    parser.add_argument("--min-events", type=int, default=100)
    parser.add_argument("--output", default="data/reports/detectors/no_liquidations_v1/variant_validation_2022_2023_to_2024.json")
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    report = build_validation_report(
        repo_root=repo_root,
        profile_name=args.profile,
        symbols=[item.upper() for item in _parse_csv(args.symbols)],
        discovery_years=_parse_years(args.discovery_years),
        validation_years=_parse_years(args.validation_years),
        horizons=[int(item) for item in _parse_csv(args.horizons)],
        cost_bps=float(args.cost_bps),
        min_events=int(args.min_events),
    )
    output = repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True, default=str), encoding="utf-8")
    print(
        json.dumps(
            {
                "status": "pass",
                "output": str(output),
                "candidate_count": report["candidate_count"],
                "strict_pass_count": report["strict_pass_count"],
                "explicit_variant_pass_count": report["explicit_variant_pass_count"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
