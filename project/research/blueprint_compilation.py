from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Tuple

import numpy as np

from project.core.coercion import as_bool, safe_float, safe_int
from project.research.blueprint_policy import load_blueprint_policy
from project.spec_registry import resolve_relative_spec_path
from project.strategy.dsl import (
    Blueprint,
    EntrySpec,
    EvaluationSpec,
    ExecutionSpec,
    ExitSpec,
    LineageSpec,
    OverlaySpec,
    SizingSpec,
    SymbolScopeSpec,
    action_to_overlays,
    event_policy,
    normalize_entry_condition,
    overlay_defaults,
    validate_feature_references,
)

LOGGER = logging.getLogger(__name__)


def _sanitize(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(value).strip().lower()).strip("_")


def _normalize_gate_audit_value(value: Any) -> str:
    if value is None:
        return "missing_evidence"
    if isinstance(value, bool):
        return "pass" if value else "fail"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return "pass" if bool(int(float(value))) else "fail"
        except (TypeError, ValueError):
            return "missing_evidence"
    normalized = str(value).strip().lower()
    if not normalized:
        return "missing_evidence"
    if normalized in {"pass", "fail", "missing_evidence"}:
        return normalized
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return "pass"
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return "fail"
    return normalized


def _coerce_bps(value: Any) -> float | None:
    """Coerce a value to bps, converting decimal returns (abs ≤ 1.0) to bps."""
    numeric = safe_float(value, np.nan)
    if not np.isfinite(numeric):
        return None
    if abs(float(numeric)) <= 1.0:
        return float(numeric) * 10_000.0
    return float(numeric)


def _resolve_expected_sizing_inputs(row: Dict[str, Any]) -> tuple[float | None, float | None]:
    """Extract expected_return_bps and expected_adverse_bps from a candidate row.

    Returns (expected_return_bps, expected_adverse_bps).
    expected_adverse_bps = stressed_after_cost_expectancy × 1.5.
    """
    expected_return_bps = _coerce_bps(row.get("mean_return_bps", row.get("expectancy")))
    stressed_bps = _coerce_bps(
        row.get(
            "stressed_after_cost_expectancy",
            row.get("stressed_after_cost_expectancy_per_trade"),
        )
    )
    if expected_return_bps is None:
        return None, None
    if stressed_bps is None:
        return expected_return_bps, None
    expected_adverse_bps = abs(float(stressed_bps)) * 1.5
    return float(expected_return_bps), float(expected_adverse_bps)


def _parse_symbol_scope(
    row: Dict[str, Any],
    run_symbols: List[str],
    condition_symbol_override: str | None = None,
) -> SymbolScopeSpec:
    candidate_symbol = (
        str(condition_symbol_override).strip().upper()
        if condition_symbol_override is not None
        else str(row.get("candidate_symbol", "")).strip().upper()
    ) or "ALL"
    if candidate_symbol != "ALL":
        target = (
            candidate_symbol
            if candidate_symbol in run_symbols
            else (run_symbols[0] if run_symbols else candidate_symbol)
        )
        return SymbolScopeSpec(
            mode="single_symbol", symbols=[target], candidate_symbol=candidate_symbol
        )
    rollout = as_bool(row.get("rollout_eligible", False))
    if rollout and len(run_symbols) > 1:
        return SymbolScopeSpec(
            mode="multi_symbol", symbols=list(run_symbols), candidate_symbol="ALL"
        )
    return SymbolScopeSpec(
        mode="all" if len(run_symbols) > 1 else "single_symbol",
        symbols=list(run_symbols) if run_symbols else ["ALL"],
        candidate_symbol="ALL",
    )


def _derive_time_stop(half_life: np.ndarray, row: Dict[str, Any]) -> int:
    policy = load_blueprint_policy().get("time_stop", {})
    if half_life.size:
        val = int(round(float(np.nanmedian(half_life))))
        return int(min(int(policy.get("max_bars", 192)), max(int(policy.get("min_bars", 4)), val)))
    base = safe_int(row.get("sample_size", row.get("n_events", 24)), 24)
    frac = float(policy.get("sample_size_fraction", 0.1))
    return int(
        min(
            int(policy.get("fallback_max_bars", 96)),
            max(int(policy.get("fallback_min_bars", 8)), int(round(base * frac))),
        )
    )


def _derive_stop_target(stats: Dict[str, np.ndarray], row: Dict[str, Any]) -> Tuple[float, float]:
    policy = load_blueprint_policy().get("stop_target", {})
    adverse = stats.get("adverse", np.array([]))
    favorable = stats.get("favorable", np.array([]))

    if adverse.size:
        stop = float(np.nanpercentile(adverse, float(policy.get("stop_percentile", 75))))
    else:
        stop = max(
            0.001,
            abs(safe_float(row.get("delta_adverse_mean"), 0.01))
            * float(policy.get("fallback_stop_multiplier", 1.5)),
        )

    if favorable.size:
        target = float(np.nanpercentile(favorable, float(policy.get("target_percentile", 60))))
    else:
        target = max(
            stop * float(policy.get("target_to_stop_min_ratio", 1.1)),
            abs(safe_float(row.get("delta_opportunity_mean"), 0.02))
            * float(policy.get("fallback_target_multiplier", 1.25)),
        )

    stop = float(
        min(
            float(policy.get("stop_ceiling", 5.0)),
            max(float(policy.get("stop_floor", 0.0005)), stop),
        )
    )
    target = float(
        min(
            float(policy.get("target_ceiling", 8.0)),
            max(float(policy.get("target_floor", 0.0005)), target),
        )
    )
    return stop, target


def _entry_from_row(
    row: Dict[str, Any],
    event_type: str,
    time_stop_bars: int,
    run_symbols: List[str],
    candidate_id: str,
) -> Tuple[EntrySpec, str | None, int]:
    policy = event_policy(event_type)
    robustness = safe_float(row.get("robustness_score"), np.nan)

    effective_lag = safe_int(row.get("effective_lag_bars"), 1)
    delay = effective_lag

    cooldown = max(8, delay * 3, int(round(time_stop_bars / 3.0)))
    if np.isfinite(robustness) and robustness < 0.75:
        cooldown = max(cooldown, 12)
    if np.isfinite(robustness) and robustness < 0.60:
        cooldown = max(cooldown, 16)
    confirmations = [str(x) for x in policy.get("confirmations", [])]
    oos_gate = as_bool(row.get("gate_oos_validation", row.get("gate_oos_validation_test", True)))
    if not oos_gate and "oos_validation_pass" in confirmations:
        confirmations = [x for x in confirmations if x != "oos_validation_pass"]
    condition, condition_nodes, condition_symbol = normalize_entry_condition(
        row.get("condition", "all"),
        event_type=event_type,
        candidate_id=candidate_id,
        run_symbols=run_symbols,
    )

    return (
        EntrySpec(
            triggers=[str(x) for x in policy.get("triggers", ["event_detected"])],
            conditions=[condition],
            confirmations=confirmations,
            delay_bars=delay,
            cooldown_bars=cooldown,
            condition_logic="all",
            condition_nodes=condition_nodes,
            arm_bars=delay,
            reentry_lockout_bars=max(cooldown, delay),
        ),
        condition_symbol,
        delay,
    )


def _sizing_from_row(row: Dict[str, Any]) -> SizingSpec:
    policy = load_blueprint_policy().get("sizing", {})
    robustness = safe_float(row.get("robustness_score"), np.nan)
    capacity = safe_float(row.get("capacity_proxy"), 0.0)
    event_type = str(row.get("event_type", ""))

    signal_scaling = {}
    if event_type in {"VOL_SHOCK", "LIQUIDITY_VACUUM", "OI_FLUSH", "LIQUIDATION_CASCADE"}:
        signal_scaling = {
            "method": "linear",
            "intensity_col": "evt_signal_intensity",
            "min_intensity": 1.5,
            "max_intensity": 5.0,
            "min_scale": 0.5,
            "max_scale": 1.5,
        }

    if (
        np.isfinite(robustness)
        and np.isfinite(capacity)
        and robustness >= float(policy.get("high_robustness_threshold", 0.75))
        and capacity >= float(policy.get("high_capacity_threshold", 0.5))
    ):
        return SizingSpec(
            mode="vol_target",
            risk_per_trade=None,
            target_vol=float(policy.get("vol_target", 0.12)),
            max_gross_leverage=1.0,
            max_position_scale=1.0,
            portfolio_risk_budget=1.0,
            symbol_risk_budget=1.0,
            signal_scaling=signal_scaling,
        )
    risk = (
        float(policy.get("high_risk_per_trade", 0.004))
        if robustness >= 0.7
        else float(policy.get("base_risk_per_trade", 0.003))
    )
    return SizingSpec(
        mode="fixed_risk",
        risk_per_trade=risk,
        target_vol=None,
        max_gross_leverage=1.0,
        max_position_scale=1.0,
        portfolio_risk_budget=1.0,
        symbol_risk_budget=1.0,
        signal_scaling=signal_scaling,
    )


def _evaluation_from_row(
    row: Dict[str, Any], fees_bps: float, slippage_bps: float
) -> EvaluationSpec:
    n_events = safe_int(row.get("n_events", row.get("sample_size", 0)), 0)
    min_trades = int(max(20, min(200, n_events // 2 if n_events else 20)))
    return EvaluationSpec(
        min_trades=min_trades,
        cost_model={
            "fees_bps": float(fees_bps),
            "slippage_bps": float(slippage_bps),
            "slippage_model": str(row.get("slippage_model", "fixed")),
            "impact_scaling": bool(row.get("impact_scaling", False)),
            "funding_included": True,
        },
        robustness_flags={
            "oos_required": as_bool(
                row.get("gate_oos_validation", row.get("gate_oos_validation_test", True))
            ),
            "multiplicity_required": as_bool(row.get("gate_multiplicity", True)),
            "regime_stability_required": as_bool(row.get("gate_c_regime_stable", True)),
        },
    )


def _execution_from_row(row: Dict[str, Any]) -> ExecutionSpec:
    """
    Derive execution parameters from row or use defaults.
    """
    policy = load_blueprint_policy().get("execution", {})
    return ExecutionSpec(
        mode=str(row.get("execution_mode", policy.get("default_mode", "market"))).lower(),
        urgency=str(row.get("urgency", policy.get("default_urgency", "aggressive"))).lower(),
        max_slippage_bps=safe_float(
            row.get("max_slippage_bps", float(policy.get("default_max_slippage_bps", 100.0))),
            float(policy.get("default_max_slippage_bps", 100.0)),
        ),
        fill_profile=str(
            row.get("fill_model_profile", policy.get("default_fill_profile", "base"))
        ).lower(),
        retry_logic=dict(row.get("retry_cancel_logic", {})),
    )


def _merge_overlays(
    policy_overlays: List[OverlaySpec], action_overlays: List[OverlaySpec]
) -> List[OverlaySpec]:
    by_name: Dict[str, OverlaySpec] = {}
    order: List[str] = []

    for overlay in policy_overlays:
        if overlay.name not in by_name:
            order.append(overlay.name)
        by_name[overlay.name] = overlay
    for overlay in action_overlays:
        if overlay.name not in by_name:
            order.append(overlay.name)
        by_name[overlay.name] = overlay

    return [by_name[name] for name in order]


def compile_blueprint(
    merged_row: Dict[str, Any],
    run_id: str,
    *,
    run_symbols: List[str],
    stats: Dict[str, np.ndarray],
    fees_bps: float,
    slippage_bps: float,
    ontology_spec_hash_value: str,
    cost_config_digest: str,
    operator_registry: Dict[str, Dict[str, Any]] | None = None,
    min_events: int = 100,
    compiler_version: str = "strategy_dsl_v1",
    deterministic_ts: str = "1970-01-01T00:00:00Z",
) -> Tuple[Blueprint, int]:
    """
    Compiles a research candidate row into a validated Strategy DSL Blueprint.
    This is the canonical business logic for blueprint generation.
    """
    event_type = str(merged_row.get("event_type", "UNKNOWN"))
    candidate_id = str(merged_row.get("candidate_id", "UNKNOWN"))

    time_stop_bars = _derive_time_stop(stats.get("half_life", np.array([])), merged_row)
    stop_value, target_value = _derive_stop_target(stats=stats, row=merged_row)

    entry, condition_symbol_override, effective_lag_used = _entry_from_row(
        merged_row,
        event_type=event_type,
        time_stop_bars=time_stop_bars,
        run_symbols=run_symbols,
        candidate_id=candidate_id,
    )

    symbol_scope = _parse_symbol_scope(
        merged_row,
        run_symbols=run_symbols,
        condition_symbol_override=condition_symbol_override,
    )

    sizing = _sizing_from_row(merged_row)
    evaluation = _evaluation_from_row(merged_row, fees_bps=fees_bps, slippage_bps=slippage_bps)

    policy = event_policy(event_type)
    overlay_rows = overlay_defaults(
        names=[str(x) for x in policy.get("overlays", [])],
        robustness_score=safe_float(merged_row.get("robustness_score"), np.nan),
    )
    policy_overlays = [
        OverlaySpec(name=str(item["name"]), params=dict(item["params"])) for item in overlay_rows
    ]
    overlays = _merge_overlays(
        policy_overlays, action_to_overlays(str(merged_row.get("action", "")))
    )

    bp_id = _sanitize(f"bp_{run_id}_{event_type}_{candidate_id}_{symbol_scope.mode}")
    template_verb = str(
        merged_row.get("template_verb", merged_row.get("rule_template", ""))
    ).strip()
    state_id = str(merged_row.get("state_id", "")).strip()
    canonical_event_type = (
        str(merged_row.get("canonical_event_type", event_type)).strip() or event_type
    )
    research_family = str(
        merged_row.get("research_family", merged_row.get("canonical_family", ""))
    ).strip()
    canonical_family = str(merged_row.get("canonical_family", "")).strip()
    if not canonical_family:
        canonical_family = research_family
    if not research_family:
        research_family = canonical_family
    canonical_regime = str(
        merged_row.get("canonical_regime", canonical_family or canonical_event_type)
    ).strip()
    subtype = str(merged_row.get("subtype", "")).strip()
    phase = str(merged_row.get("phase", "")).strip()
    evidence_mode = str(merged_row.get("evidence_mode", "")).strip()
    regime_bucket = str(merged_row.get("regime_bucket", "")).strip()
    recommended_bucket = str(merged_row.get("recommended_bucket", "")).strip()
    routing_profile_id = str(merged_row.get("routing_profile_id", "")).strip()
    proposal_id = str(merged_row.get("proposal_id", "")).strip()
    hypothesis_id = str(merged_row.get("hypothesis_id", "")).strip()

    operator_version = "unknown"
    if operator_registry is not None:
        op = operator_registry.get(template_verb, {})
        if op:
            operator_version = str(op.get("operator_version", "unknown")).strip()

    expected_return_bps, expected_adverse_bps = _resolve_expected_sizing_inputs(merged_row)

    blueprint = Blueprint(
        id=bp_id,
        run_id=run_id,
        event_type=event_type,
        candidate_id=candidate_id,
        symbol_scope=symbol_scope,
        direction=str(policy.get("direction", "conditional")),
        entry=entry,
        exit=ExitSpec(
            time_stop_bars=time_stop_bars,
            invalidation={
                "metric": "adverse_proxy",
                "operator": ">",
                "value": round(stop_value, 6),
            },
            stop_type=str(policy.get("stop_type", "range_pct")),
            stop_value=round(stop_value, 6),
            target_type=str(policy.get("target_type", "range_pct")),
            target_value=round(target_value, 6),
            trailing_stop_type=str(policy.get("stop_type", "range_pct")),
            trailing_stop_value=round(stop_value * 0.75, 6),
            break_even_r=1.0,
        ),
        execution=_execution_from_row(merged_row),
        sizing=sizing,
        overlays=overlays,
        evaluation=evaluation,
        lineage=LineageSpec(
            proposal_id=proposal_id,
            hypothesis_id=hypothesis_id,
            source_path=str(merged_row.get("source_path", "")),
            compiler_version=compiler_version,
            generated_at_utc=deterministic_ts,
            bridge_embargo_days_used=(
                None
                if merged_row.get("bridge_embargo_days_used") in (None, "")
                else safe_int(merged_row.get("bridge_embargo_days_used"), -1)
            ),
            events_count_used_for_gate=safe_int(
                merged_row.get("n_events", merged_row.get("sample_size", 0)), 0
            ),
            min_events_threshold=int(min_events),
            cost_config_digest=cost_config_digest,
            promotion_track=str(merged_row.get("promotion_track", "fallback_only")),
            discovery_start=str(merged_row.get("discovery_start", "")),
            discovery_end=str(merged_row.get("discovery_end", "")),
            ontology_spec_hash=ontology_spec_hash_value,
            canonical_event_type=canonical_event_type,
            research_family=research_family,
            canonical_family=canonical_family,
            canonical_regime=canonical_regime,
            subtype=subtype,
            phase=phase,
            evidence_mode=evidence_mode,
            regime_bucket=regime_bucket,
            recommended_bucket=recommended_bucket,
            routing_profile_id=routing_profile_id,
            state_id=state_id,
            template_verb=template_verb,
            operator_version=operator_version,
            constraints={
                "gate_after_cost_positive": as_bool(
                    merged_row.get("gate_after_cost_positive", False)
                ),
                "gate_after_cost_stressed_positive": as_bool(
                    merged_row.get("gate_after_cost_stressed_positive", False)
                ),
                "gate_bridge_tradable": as_bool(merged_row.get("gate_bridge_tradable", False)),
                "expected_return_bps": expected_return_bps,
                "expected_adverse_bps": expected_adverse_bps,
                "blueprint_effective_lag_bars_used": int(effective_lag_used),
                "policy_variant_id": str(merged_row.get("policy_variant_id", "")).strip(),
                "variant_entry_delay_bars": safe_int(
                    merged_row.get("variant_entry_delay_bars", effective_lag_used),
                    int(effective_lag_used),
                ),
                "variant_one_trade_per_episode": as_bool(
                    merged_row.get("variant_one_trade_per_episode", False)
                ),
                "variant_cooldown_bars": safe_int(merged_row.get("variant_cooldown_bars", 0), 0),
                "fallback_used": not as_bool(
                    merged_row.get("promotion_track", "standard") == "standard"
                ),
                "fallback_reason": str(
                    merged_row.get(
                        "promotion_fail_gate_primary", merged_row.get("fallback_reason", "")
                    )
                ).strip(),
                "gate_audit_trail": {
                    k: _normalize_gate_audit_value(v)
                    for k, v in merged_row.items()
                    if str(k).startswith("gate_")
                },
                "policy_spec_path": str(resolve_relative_spec_path("spec/blueprint_policies.yaml")),
            },
        ),
    )

    blueprint.validate()
    validate_feature_references(blueprint.to_dict())

    return blueprint, effective_lag_used
