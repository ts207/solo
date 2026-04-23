from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd

from project.core.coercion import safe_float, safe_int, as_bool
from project.io.utils import list_parquet_files, read_parquet
from project.strategy.dsl import is_executable_action, is_executable_condition

from project.research.candidates.shaping import (
    route_event_family,
    risk_controls_from_action,
    infer_condition_from_blueprint,
    infer_action_from_blueprint,
    symbol_scope_from_row,
    sanitize_id,
)

_LOG = logging.getLogger(__name__)


def build_promoted_strategy_candidate(
    blueprint: Dict[str, Any],
    promotion: Dict[str, Any],
    symbols: List[str],
) -> Dict[str, Any] | None:
    event = str(blueprint.get("event_type", "")).strip()
    blueprint_id = str(blueprint.get("id", "")).strip()
    candidate_id = str(blueprint.get("candidate_id", "")).strip() or blueprint_id or "promoted"
    condition = infer_condition_from_blueprint(blueprint)
    action = infer_action_from_blueprint(blueprint)
    controls = risk_controls_from_action(action)

    stressed_split = (
        promotion.get("stressed_split_pnl", {})
        if isinstance(promotion.get("stressed_split_pnl"), dict)
        else {}
    )
    split_pnl = (
        promotion.get("split_pnl", {}) if isinstance(promotion.get("split_pnl"), dict) else {}
    )
    selection_score = safe_float(
        stressed_split.get("validation"),
        safe_float(split_pnl.get("validation"), np.nan),
    )
    if not np.isfinite(selection_score):
        selection_score = safe_float(promotion.get("symbol_pass_rate"), np.nan)
    expectancy_after_multiplicity = safe_float(split_pnl.get("validation"), selection_score)
    symbol_pass_rate = safe_float(promotion.get("symbol_pass_rate"), np.nan)
    trades = safe_int(promotion.get("trades"), 0)
    oos_sign_consistency = safe_float(
        promotion.get("oos_sign_consistency"),
        safe_float(promotion.get("sign_consistency"), np.nan),
    )

    route = route_event_family(event)
    execution_family = route["execution_family"] if route else "unmapped"
    base_strategy = route["base_strategy"] if route else "unmapped"
    routing_reason = (
        "" if route else f"Unknown event family `{event}`; no strategy routing is defined."
    )

    run_symbols = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]
    candidate_symbol, rollout_eligible = _candidate_symbol_from_blueprint(
        blueprint=blueprint, symbols=run_symbols
    )
    deployment_scope = _resolve_deployment_scope(
        candidate_symbol=candidate_symbol,
        run_symbols=run_symbols,
        symbol_scores={},
        rollout_eligible=rollout_eligible,
    )
    deployment_symbols = deployment_scope["deployment_symbols"]

    if as_bool(promotion.get("bridge_certified", False)) == False:
        _LOG.error(f"Candidate {candidate_id} lacks bridge certification. Rejecting compile.")
        return None

    executable_condition = bool(is_executable_condition(condition, run_symbols=run_symbols))
    executable_action = bool(is_executable_action(action))
    if not executable_condition or not executable_action:
        reasons: List[str] = []
        if not executable_condition:
            reasons.append(f"Non-executable condition per DSL contract: `{condition}`")
        if not executable_action:
            reasons.append(f"Non-executable action per DSL contract: `{action}`")
        routing_reason = (
            routing_reason + ("; " if routing_reason else "") + "; ".join(reasons)
        ).strip()

    strategy_instances = [
        {
            "strategy_id": f"{base_strategy}_{symbol}",
            "base_strategy": base_strategy,
            "symbol": symbol,
            "strategy_params": {
                "promotion_thresholds": {
                    "selection_score": selection_score,
                    "symbol_pass_rate": symbol_pass_rate,
                    "trades": trades,
                },
                "risk_controls": controls,
                "condition": condition,
                "action": action,
                "blueprint_id": blueprint_id,
            },
        }
        for symbol in deployment_symbols
    ]

    return {
        "strategy_candidate_id": sanitize_id(f"{event}_{condition}_{action}_{candidate_id}"),
        "candidate_id": candidate_id,
        "source_type": "promoted_blueprint",
        "execution_family": execution_family,
        "base_strategy": base_strategy,
        "event": event,
        "condition": condition,
        "action": action,
        "executable_condition": executable_condition,
        "executable_action": executable_action,
        "status": str(promotion.get("status", "CANDIDATE")).strip().upper() or "CANDIDATE",
        "n_events": trades,
        "edge_score": safe_float(split_pnl.get("validation"), selection_score),
        "expectancy_per_trade": safe_float(split_pnl.get("validation"), selection_score),
        "expectancy_after_multiplicity": expectancy_after_multiplicity,
        "stability_proxy": symbol_pass_rate,
        "robustness_score": symbol_pass_rate,
        "quality_score": selection_score if np.isfinite(selection_score) else np.nan,
        "selection_score": selection_score,
        "oos_sign_consistency": oos_sign_consistency,
        "symbols": run_symbols,
        "candidate_symbol": candidate_symbol,
        "run_symbols": run_symbols,
        "rollout_eligible": deployment_scope["rollout_eligible"],
        "deployment_type": deployment_scope["deployment_type"],
        "deployment_symbols": deployment_symbols,
        "allocation_policy": {
            "mode": "full",
            "signal_take_rate": 1.0,
            "max_participation_rate": 1.0,
            "allocation_viable": True,
        },
        "fractional_allocation_applied": False,
        "strategy_instances": strategy_instances,
        "risk_controls": controls,
        "notes": [f"Derived from promoted blueprint {blueprint_id}."],
    }


def build_compiled_blueprint_strategy_candidate(
    blueprint: Dict[str, Any],
    metrics: Dict[str, Any],
    symbols: List[str],
) -> Dict[str, Any] | None:
    if not metrics:
        return None

    lineage = blueprint.get("lineage", {}) if isinstance(blueprint.get("lineage"), dict) else {}
    bridge_certified = as_bool(metrics.get("bridge_certified", False))
    status = str(metrics.get("status", "")).strip().upper()
    if not bridge_certified or status not in {"PROMOTED", "PROMOTED_RESEARCH"}:
        return None

    validation_return = safe_float(metrics.get("selection_score"), np.nan)
    if not np.isfinite(validation_return):
        bridge_bps = safe_float(metrics.get("bridge_validation_after_cost_bps"), np.nan)
        if np.isfinite(bridge_bps):
            validation_return = bridge_bps / 1e4
    if not np.isfinite(validation_return):
        validation_return = safe_float(
            metrics.get("expectancy_after_multiplicity"),
            safe_float(metrics.get("expectancy_per_trade"), 0.0),
        )

    stressed_validation = safe_float(
        metrics.get("expectancy_after_multiplicity"),
        validation_return,
    )
    n_events = safe_int(
        metrics.get("n_events"),
        safe_int(lineage.get("events_count_used_for_gate"), 0),
    )
    symbol_pass_rate = safe_float(
        metrics.get("robustness_score"),
        safe_float(metrics.get("symbol_pass_rate"), 1.0),
    )

    promotion = {
        "bridge_certified": bridge_certified,
        "status": status,
        "trades": n_events,
        "symbol_pass_rate": symbol_pass_rate,
        "split_pnl": {"validation": validation_return},
        "stressed_split_pnl": {"validation": stressed_validation},
        "oos_sign_consistency": safe_float(
            metrics.get("oos_sign_consistency"),
            safe_float(metrics.get("sign_consistency"), np.nan),
        ),
    }
    candidate = build_promoted_strategy_candidate(
        blueprint=blueprint,
        promotion=promotion,
        symbols=symbols,
    )
    if candidate is None:
        return None

    candidate["n_events"] = n_events
    candidate["status"] = promotion["status"]
    candidate["selection_score"] = safe_float(
        metrics.get("selection_score"), candidate.get("selection_score")
    )
    candidate["quality_score"] = safe_float(
        metrics.get("quality_score"), candidate.get("quality_score")
    )
    candidate["edge_score"] = safe_float(metrics.get("edge_score"), candidate.get("edge_score"))
    candidate["expectancy_per_trade"] = safe_float(
        metrics.get("expectancy_per_trade"),
        candidate.get("expectancy_per_trade"),
    )
    candidate["expectancy_after_multiplicity"] = safe_float(
        metrics.get("expectancy_after_multiplicity"),
        candidate.get("expectancy_after_multiplicity"),
    )
    candidate["stability_proxy"] = safe_float(
        metrics.get("stability_proxy"), candidate.get("stability_proxy")
    )
    candidate["robustness_score"] = safe_float(
        metrics.get("robustness_score"), candidate.get("robustness_score")
    )
    candidate["oos_sign_consistency"] = safe_float(
        metrics.get("oos_sign_consistency"),
        safe_float(metrics.get("sign_consistency"), candidate.get("oos_sign_consistency")),
    )
    if "notes" in candidate and isinstance(candidate["notes"], list):
        candidate["notes"].append("Derived from compiled blueprint artifact.")
    return candidate


def _candidate_symbol_from_blueprint(
    blueprint: Dict[str, Any], symbols: List[str]
) -> Tuple[str, bool]:
    scope = (
        blueprint.get("symbol_scope", {}) if isinstance(blueprint.get("symbol_scope"), dict) else {}
    )
    mode = str(scope.get("mode", "")).strip().lower()
    run_symbols = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]
    if mode == "single_symbol":
        scope_symbols = scope.get("symbols", []) if isinstance(scope.get("symbols"), list) else []
        for symbol in scope_symbols:
            normalized = str(symbol).strip().upper()
            if normalized in run_symbols:
                return normalized, False
        if run_symbols:
            return run_symbols[0], False
    candidate_symbol = str(scope.get("candidate_symbol", "")).strip().upper()
    if candidate_symbol and candidate_symbol != "ALL":
        if candidate_symbol in run_symbols:
            return candidate_symbol, False
        if run_symbols:
            return run_symbols[0], False
    rollout = mode == "multi_symbol" and len(run_symbols) > 1
    return "ALL" if rollout else (run_symbols[0] if run_symbols else "ALL"), rollout


def _resolve_deployment_scope(
    candidate_symbol: str,
    run_symbols: List[str],
    symbol_scores: Dict[str, float],
    rollout_eligible: bool,
) -> Dict[str, Any]:
    normalized_run_symbols = [
        str(symbol).strip().upper() for symbol in run_symbols if str(symbol).strip()
    ]
    if not normalized_run_symbols:
        return {
            "deployment_type": "single_symbol",
            "deployment_symbols": [],
            "rollout_eligible": False,
        }

    candidate_symbol = str(candidate_symbol).strip().upper()
    if candidate_symbol and candidate_symbol != "ALL":
        target = (
            candidate_symbol
            if candidate_symbol in normalized_run_symbols
            else normalized_run_symbols[0]
        )
        return {
            "deployment_type": "single_symbol",
            "deployment_symbols": [target],
            "rollout_eligible": False,
        }

    if len(normalized_run_symbols) == 1:
        return {
            "deployment_type": "single_symbol",
            "deployment_symbols": normalized_run_symbols,
            "rollout_eligible": False,
        }

    if rollout_eligible:
        return {
            "deployment_type": "multi_symbol",
            "deployment_symbols": normalized_run_symbols,
            "rollout_eligible": True,
        }

    best_symbol = normalized_run_symbols[0]
    if symbol_scores:
        ranked = sorted(
            (
                (symbol, score)
                for symbol, score in symbol_scores.items()
                if symbol in normalized_run_symbols
            ),
            key=lambda item: item[1],
            reverse=True,
        )
        if ranked:
            best_symbol = ranked[0][0]
    return {
        "deployment_type": "single_symbol",
        "deployment_symbols": [best_symbol],
        "rollout_eligible": False,
    }


def build_edge_strategy_candidate(
    row: Dict[str, Any],
    detail: Dict[str, Any],
    symbols: List[str],
) -> Dict[str, Any]:
    event = str(row.get("event", "")).strip()
    candidate_id = str(row.get("candidate_id", "")).strip()
    edge_score = safe_float(row.get("edge_score"), 0.0)
    stability_proxy = safe_float(row.get("stability_proxy"), 0.0)
    expectancy_per_trade = safe_float(
        row.get("expectancy_per_trade"), safe_float(row.get("expected_return_proxy"), 0.0)
    )
    expectancy_after_multiplicity = safe_float(
        row.get("expectancy_after_multiplicity"), expectancy_per_trade
    )
    robustness_score = safe_float(row.get("robustness_score"), stability_proxy)
    event_frequency = safe_float(row.get("event_frequency"), 0.0)
    capacity_proxy = safe_float(row.get("capacity_proxy"), 0.0)
    profit_density_score = safe_float(
        row.get("profit_density_score"),
        max(0.0, expectancy_per_trade) * max(0.0, robustness_score) * max(0.0, event_frequency),
    )
    delay_robustness_score = safe_float(row.get("delay_robustness_score"), 0.0)
    selection_score_executed = safe_float(row.get("selection_score_executed"), 0.0)
    quality_score = safe_float(
        row.get("quality_score"),
        (
            selection_score_executed
            if selection_score_executed > 0.0
            else (
                profit_density_score
                if profit_density_score > 0.0
                else (
                    0.35 * max(0.0, expectancy_after_multiplicity)
                    + 0.25 * max(0.0, robustness_score)
                    + 0.20 * delay_robustness_score
                    + 0.20 * profit_density_score
                )
            )
        ),
    )
    n_events = safe_int(row.get("n_events"), 0)
    status = str(row.get("status", "PROMOTED")).strip().upper()

    condition = str(detail.get("condition", "all"))
    action = str(detail.get("action", "no_action"))
    selection_score = selection_score_executed if selection_score_executed > 0.0 else quality_score
    if selection_score <= 0.0:
        selection_score = (0.65 * edge_score) + (0.35 * stability_proxy)
    controls = risk_controls_from_action(action)
    route = route_event_family(event)
    execution_family = route["execution_family"] if route else "unmapped"
    base_strategy = route["base_strategy"] if route else "unmapped"

    executable_condition = bool(is_executable_condition(condition, run_symbols=symbols))
    executable_action = bool(is_executable_action(action))

    strategy_candidate_id = sanitize_id(f"{event}_{condition}_{action}_{candidate_id}")
    symbol_scope = symbol_scope_from_row(row=row, symbols=symbols)
    symbol_scores = _parse_symbol_scores(row.get("symbol_scores", {}))
    rollout_eligible = bool(row.get("rollout_eligible", False))
    deployment_scope = _resolve_deployment_scope(
        candidate_symbol=str(symbol_scope["candidate_symbol"]),
        run_symbols=symbol_scope["run_symbols"],
        symbol_scores=symbol_scores,
        rollout_eligible=rollout_eligible,
    )
    deployment_symbols = deployment_scope["deployment_symbols"]

    allocation_policy: Dict[str, Any]
    raw_policy = str(row.get("allocation_policy_json", "")).strip()
    if raw_policy:
        try:
            allocation_policy = json.loads(raw_policy)
        except Exception:
            allocation_policy = {}
    else:
        allocation_policy = {}

    if not allocation_policy:
        allocation_policy = {
            "mode": "full",
            "signal_take_rate": 1.0,
            "max_participation_rate": 1.0,
            "allocation_viable": True,
        }

    policy_mode = str(allocation_policy.get("mode", "")).strip().lower()
    fractional_applied = bool(
        as_bool(row.get("fractional_allocation_applied", False))
        or policy_mode == "fractional_top_quantile"
    )
    if fractional_applied:
        controls = dict(controls)
        controls["size_scale"] = float(
            safe_float(allocation_policy.get("signal_take_rate"), controls.get("size_scale", 1.0))
        )
        controls["max_participation_rate"] = float(
            safe_float(allocation_policy.get("max_participation_rate"), 0.25)
        )

    strategy_instances = [
        {
            "strategy_id": f"{base_strategy}_{symbol}",
            "base_strategy": base_strategy,
            "symbol": symbol,
            "strategy_params": {
                "promotion_thresholds": {
                    "edge_score": edge_score,
                    "expectancy_per_trade": expectancy_per_trade,
                    "stability_proxy": stability_proxy,
                    "robustness_score": robustness_score,
                    "event_frequency": event_frequency,
                    "capacity_proxy": capacity_proxy,
                    "profit_density_score": profit_density_score,
                    "selection_score": selection_score,
                    "symbol_score": safe_float(symbol_scores.get(symbol), selection_score),
                },
                "risk_controls": controls,
                "condition": condition,
                "action": action,
            },
        }
        for symbol in deployment_symbols
    ]

    return {
        "strategy_candidate_id": strategy_candidate_id,
        "candidate_id": candidate_id,
        "source_type": "edge_candidate",
        "execution_family": execution_family,
        "base_strategy": base_strategy,
        "event": event,
        "action": action,
        "executable_condition": executable_condition,
        "executable_action": executable_action,
        "status": status,
        "n_events": n_events,
        "edge_score": edge_score,
        "expectancy_per_trade": expectancy_per_trade,
        "expectancy_after_multiplicity": expectancy_after_multiplicity,
        "stability_proxy": stability_proxy,
        "robustness_score": robustness_score,
        "quality_score": quality_score,
        "selection_score": selection_score,
        "oos_sign_consistency": safe_float(
            row.get("oos_sign_consistency"),
            safe_float(row.get("sign_consistency"), np.nan),
        ),
        "symbols": symbols,
        "candidate_symbol": symbol_scope["candidate_symbol"],
        "run_symbols": symbol_scope["run_symbols"],
        "symbol_scores": symbol_scores,
        "rollout_eligible": deployment_scope["rollout_eligible"],
        "deployment_type": deployment_scope["deployment_type"],
        "deployment_symbols": deployment_symbols,
        "allocation_policy": allocation_policy,
        "fractional_allocation_applied": bool(fractional_applied),
        "strategy_instances": strategy_instances,
        "risk_controls": controls,
        "notes": [f"Derived from promoted edge candidate {candidate_id} ({event})."],
    }


def _parse_symbol_scores(value: Any) -> Dict[str, float]:
    if isinstance(value, dict):
        parsed = value
    else:
        text = str(value).strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except Exception:
            return {}
    out: Dict[str, float] = {}
    for symbol, score in parsed.items():
        symbol_key = str(symbol).strip().upper()
        if not symbol_key:
            continue
        out[symbol_key] = safe_float(score, 0.0)
    return out
