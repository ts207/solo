from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Sequence

import numpy as np
import pandas as pd
from pydantic import BaseModel

from project.core.coercion import as_bool, safe_float, safe_int
from project.domain.compiled_registry import get_domain_registry
from project.domain.promotion.promotion_policy import PromotionPolicy
from project.research.utils.returns_oos import normalize_returns_oos_combined
from project.research.validation.falsification import evaluate_negative_controls
from project.research.validation.regime_tests import build_stability_result_from_row
from project.research.validation.schemas import EvidenceBundle, PromotionDecision, SearchBurden
from project.research.contracts.search_burden import (
    SEARCH_BURDEN_FIELDS,
    default_search_burden_dict,
)


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (np.floating, np.integer)):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _event_family(event_type: str) -> str:
    token = str(event_type or "").strip()
    if not token:
        return ""
    spec = get_domain_registry().get_event(token)
    if spec is None:
        return token.upper()
    return spec.research_family or spec.canonical_family or spec.canonical_regime or spec.event_type


def _bool_gate_value(row: Dict[str, Any], key: str, default: bool = True) -> bool:
    if key not in row:
        return bool(default)
    return bool(as_bool(row.get(key, default)))


def _row_value(row: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    return default


def _normalize_bundle_row_aliases(row: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(row)
    alias_map = {
        "gate_delay_robustness": ("delay_robustness_pass",),
        "gate_timeframe_consensus": ("gate_promo_timeframe_consensus", "timeframe_consensus_pass"),
        "gate_bridge_microstructure": ("microstructure_pass",),
        "gate_regime_stability": ("regime_stability_pass",),
    }
    for canonical, aliases in alias_map.items():
        if canonical in normalized:
            continue
        for alias in aliases:
            if alias in normalized:
                normalized[canonical] = normalized[alias]
                break
    return normalized


def _looks_like_evidence_bundle(payload: Dict[str, Any]) -> bool:
    required = {
        "candidate_id",
        "event_type",
        "sample_definition",
        "effect_estimates",
        "uncertainty_estimates",
        "stability_tests",
        "falsification_results",
        "cost_robustness",
        "multiplicity_adjustment",
    }
    return required.issubset(payload.keys())


def _coerce_sparse_evidence_bundle(bundle: Dict[str, Any]) -> Dict[str, Any]:
    """Backfill reduced/legacy evidence bundles to the current schema.

    Some governance tests and older artifacts provide only the subset of fields
    required for promotion-gate evaluation. Newer bundle schemas require a
    fuller nested shape. This helper preserves the caller-provided signal while
    filling omitted fields with neutral defaults so policy evaluation remains
    backward compatible.
    """
    normalized = _json_safe(dict(bundle))

    event_type = str(normalized.get("event_type") or normalized.get("primary_event_id") or "").strip()
    lineage = dict(normalized.get("lineage") or {})
    normalized.setdefault("primary_event_id", event_type)
    normalized.setdefault(
        "run_id",
        str(normalized.get("run_id") or lineage.get("run_id") or "__adhoc__").strip() or "__adhoc__",
    )

    sample = dict(normalized.get("sample_definition") or {})
    n_events = int(safe_int(sample.get("n_events", 0), 0))
    normalized["sample_definition"] = {
        "n_events": n_events,
        "validation_samples": int(safe_int(sample.get("validation_samples", 0), 0)),
        "test_samples": int(safe_int(sample.get("test_samples", 0), 0)),
        "symbol": str(sample.get("symbol", "") or "").strip(),
    }

    split = dict(normalized.get("split_definition") or {})
    normalized["split_definition"] = {
        "split_scheme_id": str(split.get("split_scheme_id", "") or "").strip(),
        "purge_bars": int(safe_int(split.get("purge_bars", 0), 0)),
        "embargo_bars": int(safe_int(split.get("embargo_bars", 0), 0)),
        "bar_duration_minutes": int(safe_int(split.get("bar_duration_minutes", 5), 5)),
    }

    effect = dict(normalized.get("effect_estimates") or {})
    estimate_bps = safe_float(effect.get("estimate_bps", np.nan), np.nan)
    normalized["effect_estimates"] = {
        "estimate": safe_float(effect.get("estimate", estimate_bps), np.nan),
        "estimate_bps": estimate_bps,
        "stderr": safe_float(effect.get("stderr", np.nan), np.nan),
        "stderr_bps": safe_float(effect.get("stderr_bps", np.nan), np.nan),
    }

    uncertainty = dict(normalized.get("uncertainty_estimates") or {})
    q_value = safe_float(uncertainty.get("q_value", np.nan), np.nan)
    q_value_by = safe_float(uncertainty.get("q_value_by", q_value), np.nan)
    q_value_cluster = safe_float(uncertainty.get("q_value_cluster", q_value), np.nan)
    normalized["uncertainty_estimates"] = {
        "ci_low": safe_float(uncertainty.get("ci_low", np.nan), np.nan),
        "ci_high": safe_float(uncertainty.get("ci_high", np.nan), np.nan),
        "ci_low_bps": safe_float(uncertainty.get("ci_low_bps", np.nan), np.nan),
        "ci_high_bps": safe_float(uncertainty.get("ci_high_bps", np.nan), np.nan),
        "p_value_raw": safe_float(uncertainty.get("p_value_raw", q_value), np.nan),
        "q_value": q_value,
        "q_value_by": q_value_by,
        "q_value_cluster": q_value_cluster,
        "n_obs": int(safe_int(uncertainty.get("n_obs", n_events), n_events)),
        "n_clusters": int(safe_int(uncertainty.get("n_clusters", 0), 0)),
    }

    stability = dict(normalized.get("stability_tests") or {})
    stability.setdefault("stability_score", safe_float(stability.get("stability_score", 0.0), 0.0))
    stability.setdefault("sign_consistency", safe_float(stability.get("sign_consistency", 0.0), 0.0))
    normalized["stability_tests"] = stability

    falsification = dict(normalized.get("falsification_results") or {})
    falsification.setdefault("negative_control_pass", bool(as_bool(falsification.get("negative_control_pass", False))))
    falsification.setdefault("passes_control", bool(as_bool(falsification.get("passes_control", False))))
    falsification.setdefault("shift_placebo_pass", bool(as_bool(falsification.get("passes_control", False))))
    falsification.setdefault("random_placebo_pass", bool(as_bool(falsification.get("passes_control", False))))
    falsification.setdefault("direction_reversal_pass", bool(as_bool(falsification.get("passes_control", False))))
    normalized["falsification_results"] = falsification

    cost = dict(normalized.get("cost_robustness") or {})
    tob_coverage = safe_float(cost.get("tob_coverage", np.nan), np.nan)
    micro_pass = bool(as_bool(cost.get("microstructure_pass", cost.get("tob_coverage_pass", False))))
    normalized["cost_robustness"] = {
        "cost_survival_ratio": safe_float(cost.get("cost_survival_ratio", np.nan), np.nan),
        "net_expectancy_bps": safe_float(cost.get("net_expectancy_bps", estimate_bps), np.nan),
        "effective_cost_bps": safe_float(cost.get("effective_cost_bps", np.nan), np.nan),
        "turnover_proxy_mean": safe_float(cost.get("turnover_proxy_mean", np.nan), np.nan),
        "tob_coverage": tob_coverage,
        "tob_coverage_pass": bool(as_bool(cost.get("tob_coverage_pass", micro_pass))),
        "stressed_cost_pass": bool(as_bool(cost.get("stressed_cost_pass", True))),
        "retail_net_expectancy_pass": bool(as_bool(cost.get("retail_net_expectancy_pass", True))),
        "retail_cost_budget_pass": bool(as_bool(cost.get("retail_cost_budget_pass", True))),
        "retail_turnover_pass": bool(as_bool(cost.get("retail_turnover_pass", True))),
        **{k: v for k, v in cost.items() if k == "microstructure_pass"},
    }

    mult = dict(normalized.get("multiplicity_adjustment") or {})
    q_program = safe_float(mult.get("q_value_program", q_value), np.nan)
    normalized["multiplicity_adjustment"] = {
        "correction_family_id": str(mult.get("correction_family_id", "") or "").strip(),
        "correction_method": str(mult.get("correction_method", "bh") or "bh"),
        "p_value_adj": safe_float(mult.get("p_value_adj", q_value), np.nan),
        "p_value_adj_by": safe_float(mult.get("p_value_adj_by", q_value_by), np.nan),
        "p_value_adj_holm": safe_float(mult.get("p_value_adj_holm", q_value_cluster), np.nan),
        "q_value_program": q_program,
        "q_value_scope": safe_float(mult.get("q_value_scope", q_program), np.nan),
        "effective_q_value": safe_float(mult.get("effective_q_value", q_program), np.nan),
        "num_tests_scope": int(safe_int(mult.get("num_tests_scope", 0), 0)),
        "multiplicity_scope_mode": str(mult.get("multiplicity_scope_mode", "") or ""),
        "multiplicity_scope_key": str(mult.get("multiplicity_scope_key", "") or ""),
        "multiplicity_scope_version": str(mult.get("multiplicity_scope_version", "") or ""),
        "multiplicity_scope_degraded": bool(as_bool(mult.get("multiplicity_scope_degraded", False))),
    }

    metadata = dict(normalized.get("metadata") or {})
    metadata.setdefault("tob_coverage", tob_coverage)
    metadata.setdefault("repeated_fold_consistency", safe_float(stability.get("sign_consistency", 0.0), 0.0))
    metadata.setdefault("structural_robustness_score", safe_float(stability.get("stability_score", 0.0), 0.0))
    normalized["metadata"] = metadata

    promotion_decision = dict(normalized.get("promotion_decision") or {})
    status = str(promotion_decision.get("promotion_status", "") or "").strip() or "pending"
    promotion_decision.setdefault("promotion_status", status)
    promotion_decision.setdefault("eligible", status == "promoted")
    promotion_decision.setdefault(
        "promotion_track",
        "standard" if status == "promoted" else "restricted",
    )
    promotion_decision.setdefault("rank_score", 0.0)
    normalized["promotion_decision"] = promotion_decision
    normalized.setdefault("rejection_reasons", [])
    normalized.setdefault("artifacts", {})
    normalized.setdefault("search_burden", default_search_burden_dict())
    normalized.setdefault("policy_version", "phase4_pr5_v1")
    normalized.setdefault("bundle_version", "phase4_bundle_v1")
    return normalized


def _optional_bool_gate(row: Dict[str, Any], *keys: str) -> bool | None:
    value = _row_value(row, *keys, default=None)
    if value is None:
        return None
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return bool(as_bool(value))


def _set_optional_extra_bool(target: BaseModel, row: Dict[str, Any], key: str, *aliases: str) -> None:
    """Set an optional boolean extra field on a Pydantic model.

    For models with extra="allow", this sets arbitrary boolean gates
    when the value is present in the row data.
    """
    value = _optional_bool_gate(row, key, *aliases)
    if value is not None:
        setattr(target, key, value)


def _clear_optional_extra(target: BaseModel, key: str) -> None:
    """Remove an optional extra field from a Pydantic model.

    For models with extra="allow", this removes the key from the
    model_extra / __pydantic_extra__ dict if present.
    """
    extra = getattr(target, "model_extra", None)
    if isinstance(extra, dict):
        extra.pop(key, None)
        return

    pextra = getattr(target, "__pydantic_extra__", None)
    if isinstance(pextra, dict):
        pextra.pop(key, None)


def _normalize_returns_oos_combined(value: Any) -> list[float]:
    return normalize_returns_oos_combined(value)


def build_evidence_bundle(
    row: Dict[str, Any],
    *,
    control_rate: float | None = None,
    max_negative_control_pass_rate: float = 0.01,
    allow_missing_negative_controls: bool = False,
    policy_version: str = "phase4_pr5_v1",
    bundle_version: str = "phase4_bundle_v1",
) -> Dict[str, Any]:
    row = _normalize_bundle_row_aliases(row)
    candidate_id = (
        str(row.get("candidate_id", "")).strip()
        or str(row.get("plan_row_id", "")).strip()
        or str(row.get("hypothesis_id", "")).strip()
        or "__adhoc_candidate__"
    )
    event_type = (
        str(row.get("event_type", row.get("primary_event_id", row.get("event", "")))).strip()
        or "UNKNOWN_EVENT"
    )
    run_id = str(row.get("run_id", "")).strip() or "__adhoc__"
    stability = build_stability_result_from_row(row)
    falsification = evaluate_negative_controls(
        row=row,
        control_rate=control_rate,
        max_negative_control_pass_rate=max_negative_control_pass_rate,
        allow_missing_negative_controls=allow_missing_negative_controls,
    )
    tob_coverage = safe_float(row.get("tob_coverage", np.nan), np.nan)
    tob_gate_default = bool(np.isfinite(tob_coverage) and tob_coverage >= 0.0)
    microstructure_pass = _optional_bool_gate(
        row, "gate_bridge_microstructure", "microstructure_pass"
    )
    gate_delay_robustness = _optional_bool_gate(
        row, "gate_delay_robustness", "delay_robustness_pass"
    )
    gate_timeframe_consensus = _optional_bool_gate(
        row,
        "gate_timeframe_consensus",
        "gate_promo_timeframe_consensus",
        "timeframe_consensus_pass",
    )
    gate_bridge_microstructure = _optional_bool_gate(
        row, "gate_bridge_microstructure", "microstructure_pass"
    )
    gate_regime_stability = _optional_bool_gate(
        row, "gate_regime_stability", "regime_stability_pass"
    )
    gate_structural_break = _optional_bool_gate(
        row, "gate_structural_break", "structural_break_pass"
    )
    returns_oos_combined = _normalize_returns_oos_combined(row.get("returns_oos_combined"))
    has_realized_oos_path = bool(len(returns_oos_combined) >= 10)
    bundle = EvidenceBundle(
        candidate_id=candidate_id,
        primary_event_id=event_type,
        event_family=_event_family(event_type),
        event_type=event_type,
        run_id=run_id,
        sample_definition={
            "n_events": int(safe_int(row.get("n_events", row.get("sample_size", 0)), 0)),
            "validation_samples": int(safe_int(row.get("validation_samples", 0), 0)),
            "test_samples": int(safe_int(row.get("test_samples", 0), 0)),
            "symbol": str(row.get("symbol", "")).strip(),
        },
        split_definition={
            "split_scheme_id": str(row.get("split_scheme_id", "")).strip(),
            "purge_bars": int(safe_int(row.get("purge_bars_used", 0), 0)),
            "embargo_bars": int(safe_int(row.get("embargo_bars_used", 0), 0)),
            "bar_duration_minutes": int(safe_int(row.get("bar_duration_minutes", 5), 5)),
        },
        effect_estimates={
            "estimate": safe_float(
                row.get("estimate", row.get("effect_shrunk_state", row.get("expectancy", np.nan))),
                np.nan,
            ),
            "estimate_bps": safe_float(
                row.get(
                    "estimate_bps",
                    row.get(
                        "bridge_validation_after_cost_bps", row.get("net_expectancy_bps", np.nan)
                    ),
                ),
                np.nan,
            ),
            "stderr": safe_float(row.get("stderr", np.nan), np.nan),
            "stderr_bps": safe_float(row.get("stderr_bps", np.nan), np.nan),
        },
        uncertainty_estimates={
            "ci_low": safe_float(row.get("ci_low", np.nan), np.nan),
            "ci_high": safe_float(row.get("ci_high", np.nan), np.nan),
            "ci_low_bps": safe_float(row.get("ci_low_bps", np.nan), np.nan),
            "ci_high_bps": safe_float(row.get("ci_high_bps", np.nan), np.nan),
            "p_value_raw": safe_float(row.get("p_value_raw", row.get("p_value", np.nan)), np.nan),
            "q_value": safe_float(row.get("q_value", row.get("p_value_adj", np.nan)), np.nan),
            "q_value_by": safe_float(
                row.get("q_value_by", row.get("p_value_adj_by", np.nan)), np.nan
            ),
            "q_value_cluster": safe_float(
                row.get("q_value_cluster", row.get("p_value_adj_holm", np.nan)), np.nan
            ),
            "n_obs": int(safe_int(row.get("n_obs", row.get("n_events", 0)), 0)),
            "n_clusters": int(safe_int(row.get("n_clusters", 0), 0)),
        },
        stability_tests=stability.to_dict(),
        falsification_results=falsification.to_dict(),
        cost_robustness={
            "cost_survival_ratio": safe_float(row.get("cost_survival_ratio", np.nan), np.nan),
            "net_expectancy_bps": safe_float(
                row.get("net_expectancy_bps", row.get("bridge_validation_after_cost_bps", np.nan)),
                np.nan,
            ),
            "effective_cost_bps": safe_float(row.get("effective_cost_bps", np.nan), np.nan),
            "turnover_proxy_mean": safe_float(row.get("turnover_proxy_mean", np.nan), np.nan),
            "tob_coverage": tob_coverage,
            "tob_coverage_pass": _bool_gate_value(row, "gate_promo_tob_coverage", tob_gate_default),
            "stressed_cost_pass": _bool_gate_value(row, "gate_after_cost_stressed_positive", True),
            "retail_net_expectancy_pass": _bool_gate_value(
                row, "gate_promo_retail_net_expectancy", True
            ),
            "retail_cost_budget_pass": _bool_gate_value(row, "gate_promo_retail_cost_budget", True),
            "retail_turnover_pass": _bool_gate_value(row, "gate_promo_retail_turnover", True),
        },
        multiplicity_adjustment={
            "correction_family_id": str(
                row.get("correction_family_id", row.get("q_value_family", ""))
            ),
            "correction_method": str(row.get("correction_method", "bh")),
            "p_value_adj": safe_float(row.get("p_value_adj", row.get("q_value", np.nan)), np.nan),
            "p_value_adj_by": safe_float(
                row.get("p_value_adj_by", row.get("q_value_by", np.nan)), np.nan
            ),
            "p_value_adj_holm": safe_float(
                row.get("p_value_adj_holm", row.get("q_value_cluster", np.nan)), np.nan
            ),
            "q_value_program": safe_float(row.get("q_value_program", np.nan), np.nan),
            "q_value_scope": safe_float(row.get("q_value_scope", np.nan), np.nan),
            "effective_q_value": safe_float(row.get("effective_q_value", np.nan), np.nan),
            "num_tests_scope": safe_int(row.get("num_tests_scope", 0), 0),
            "multiplicity_scope_mode": str(row.get("multiplicity_scope_mode", "")),
            "multiplicity_scope_key": str(row.get("multiplicity_scope_key", "")),
            "multiplicity_scope_version": str(row.get("multiplicity_scope_version", "")),
            "multiplicity_scope_degraded": bool(as_bool(row.get("multiplicity_scope_degraded", False))),
        },
        metadata={
            "hypothesis_id": str(row.get("hypothesis_id", "")).strip(),
            "plan_row_id": str(row.get("plan_row_id", "")).strip(),
            "tob_coverage": tob_coverage,
            "event_is_descriptive": bool(as_bool(row.get("event_is_descriptive", False))),
            "event_is_trade_trigger": bool(as_bool(row.get("event_is_trade_trigger", True))),
            "event_contract_tier": str(row.get("event_contract_tier", "")).strip(),
            "event_operational_role": str(row.get("event_operational_role", "")).strip(),
            "event_deployment_disposition": str(row.get("event_deployment_disposition", "")).strip(),
            "event_runtime_category": str(row.get("event_runtime_category", "")).strip(),
            "event_requires_stronger_evidence": bool(as_bool(row.get("event_requires_stronger_evidence", False))),
            "is_reduced_evidence": bool(as_bool(row.get("is_reduced_evidence", False))),
            "bridge_certified": bool(as_bool(row.get("bridge_certified", False))),
            "has_realized_oos_path": bool(has_realized_oos_path),
            "repeated_fold_consistency": safe_float(
                row.get("repeated_fold_consistency", np.nan), np.nan
            ),
            "structural_robustness_score": safe_float(
                row.get("structural_robustness_score", np.nan), np.nan
            ),
            "robustness_panel_complete": bool(as_bool(row.get("robustness_panel_complete", False))),
            "num_regimes_supported": int(safe_int(row.get("num_regimes", 0), 0)),
            "promotion_track_hint": "standard"
            if _bool_gate_value(row, "gate_promo_tob_coverage", tob_gate_default)
            else "fallback_only",
        },

        policy_version=policy_version,
        bundle_version=bundle_version,
    )
    if microstructure_pass is not None:
        bundle.cost_robustness.microstructure_pass = microstructure_pass
    else:
        _clear_optional_extra(bundle.cost_robustness, "microstructure_pass")
    _set_optional_extra_bool(bundle.metadata, row, "gate_stability")
    _set_optional_extra_bool(bundle.metadata, row, "gate_after_cost_stressed_positive")
    _set_optional_extra_bool(bundle.metadata, row, "gate_delayed_entry_stress")
    _set_optional_extra_bool(bundle.metadata, row, "gate_promo_hypothesis_audit")
    _set_optional_extra_bool(bundle.metadata, row, "gate_promo_oos_validation")
    _set_optional_extra_bool(bundle.metadata, row, "gate_promo_retail_viability")
    _set_optional_extra_bool(bundle.metadata, row, "gate_promo_low_capital_viability")
    _set_optional_extra_bool(bundle.metadata, row, "gate_promo_negative_control")
    _set_optional_extra_bool(bundle.metadata, row, "gate_promo_falsification")
    _set_optional_extra_bool(bundle.metadata, row, "gate_promo_baseline_beats_complexity")
    _set_optional_extra_bool(bundle.metadata, row, "gate_promo_placebo_controls")
    _set_optional_extra_bool(bundle.metadata, row, "gate_promo_tob_coverage")
    _set_optional_extra_bool(bundle.metadata, row, "gate_promo_dsr")
    _set_optional_extra_bool(bundle.metadata, row, "gate_promo_robustness")
    _set_optional_extra_bool(bundle.metadata, row, "gate_promo_regime")
    _set_optional_extra_bool(bundle.metadata, row, "gate_promo_multiplicity_confirmatory")
    if gate_delay_robustness is not None:
        bundle.metadata.gate_delay_robustness = gate_delay_robustness
    else:
        _clear_optional_extra(bundle.metadata, "gate_delay_robustness")
    if gate_timeframe_consensus is not None:
        bundle.metadata.gate_timeframe_consensus = gate_timeframe_consensus
    else:
        _clear_optional_extra(bundle.metadata, "gate_timeframe_consensus")
    if gate_bridge_microstructure is not None:
        bundle.metadata.gate_bridge_microstructure = gate_bridge_microstructure
    else:
        _clear_optional_extra(bundle.metadata, "gate_bridge_microstructure")
    if gate_regime_stability is not None:
        bundle.metadata.gate_regime_stability = gate_regime_stability
    else:
        _clear_optional_extra(bundle.metadata, "gate_regime_stability")
    if gate_structural_break is not None:
        bundle.metadata.gate_structural_break = gate_structural_break
    else:
        _clear_optional_extra(bundle.metadata, "gate_structural_break")
    
    # Workstream B: Add search burden to bundle
    bundle.search_burden = SearchBurden(
        search_proposals_attempted=safe_int(row.get("search_proposals_attempted", 0), 0),
        search_candidates_generated=safe_int(row.get("search_candidates_generated", 0), 0),
        search_candidates_scored=safe_int(row.get("search_candidates_scored", 0), 0),
        search_candidates_eligible=safe_int(row.get("search_candidates_eligible", 0), 0),
        search_parameterizations_attempted=safe_int(row.get("search_parameterizations_attempted", 0), 0),
        search_mutations_attempted=safe_int(row.get("search_mutations_attempted", 0), 0),
        search_directions_tested=safe_int(row.get("search_directions_tested", 0), 0),
        search_confirmations_attempted=safe_int(row.get("search_confirmations_attempted", 0), 0),
        search_trigger_variants_attempted=safe_int(row.get("search_trigger_variants_attempted", 0), 0),
        search_family_count=safe_int(row.get("search_family_count", 0), 0),
        search_lineage_count=safe_int(row.get("search_lineage_count", 0), 0),
        search_scope_version=str(row.get("search_scope_version", "phase1_v1")),
        search_burden_estimated=bool(as_bool(row.get("search_burden_estimated", False))),
    )
    
    return bundle.to_dict()


def validate_evidence_bundle(bundle: Dict[str, Any]) -> None:
    from project.research.validation.schemas import EvidenceBundle as _EvidenceBundle
    try:
        _EvidenceBundle.model_validate(_coerce_sparse_evidence_bundle(bundle))
    except Exception as exc:
        raise ValueError(f"Evidence bundle validation failed: {exc}") from exc


def evaluate_promotion_bundle(bundle: Dict[str, Any], policy: PromotionPolicy) -> Dict[str, Any]:
    if not _looks_like_evidence_bundle(bundle):
        bundle = build_evidence_bundle(
            bundle,
            max_negative_control_pass_rate=policy.max_negative_control_pass_rate,
            allow_missing_negative_controls=policy.allow_missing_negative_controls,
            policy_version=policy.policy_version,
            bundle_version=policy.bundle_version,
        )
    else:
        bundle = _coerce_sparse_evidence_bundle(bundle)
    validate_evidence_bundle(bundle)
    sample = bundle["sample_definition"]
    uncertainty = bundle["uncertainty_estimates"]
    stability = bundle["stability_tests"]
    falsification = bundle["falsification_results"]
    cost = bundle["cost_robustness"]
    meta = bundle.get("metadata", {})
    n_events = int(safe_int(sample.get("n_events", 0), 0))
    q_value = safe_float(uncertainty.get("q_value", np.nan), np.nan)
    q_value_program = safe_float(
        bundle.get("multiplicity_adjustment", {}).get("q_value_program", np.nan), np.nan
    )
    q_value_scope = safe_float(
        bundle.get("multiplicity_adjustment", {}).get("q_value_scope", np.nan), np.nan
    )
    upstream_effective_q_value = safe_float(
        bundle.get("multiplicity_adjustment", {}).get("effective_q_value", np.nan), np.nan
    )
    q_value_by = safe_float(uncertainty.get("q_value_by", np.nan), np.nan)
    q_value_cluster = safe_float(uncertainty.get("q_value_cluster", np.nan), np.nan)
    
    values_for_effective_q = [
        v
        for v in [q_value, q_value_program, q_value_scope, upstream_effective_q_value]
        if np.isfinite(v)
    ]
    if values_for_effective_q:
        effective_q_value = max(values_for_effective_q)
    else:
        effective_q_value = q_value
    effective_q_value_for_check = (
        effective_q_value if bool(policy.use_effective_q_value)
        else (q_value if np.isfinite(q_value) else effective_q_value)
    )
    multiplicity_adjustment = bundle.get("multiplicity_adjustment", {}) or {}
    scope_degraded = bool(as_bool(multiplicity_adjustment.get("multiplicity_scope_degraded", False)))
    scope_metadata_present = any(
        [
            np.isfinite(q_value_scope),
            bool(str(multiplicity_adjustment.get("multiplicity_scope_mode", "")).strip()),
            bool(str(multiplicity_adjustment.get("multiplicity_scope_key", "")).strip()),
            bool(str(multiplicity_adjustment.get("multiplicity_scope_version", "")).strip()),
            scope_degraded,
        ]
    )
    tob_coverage = safe_float(cost.get("tob_coverage", meta.get("tob_coverage", np.nan)), np.nan)
    if not np.isfinite(tob_coverage):
        tob_coverage = safe_float(bundle.get("tob_coverage", np.nan), np.nan)

    negative_control_pass = bool(
        as_bool(
            meta.get(
                "gate_promo_negative_control", falsification.get("negative_control_pass", False)
            )
        )
    )
    placebo_controls_pass = bool(as_bool(meta.get("gate_promo_placebo_controls", False)))
    falsification_pass = bool(
        as_bool(meta.get("gate_promo_falsification", falsification.get("passes_control", False)))
    )
    if "gate_promo_falsification" not in meta:
        falsification_pass = bool(placebo_controls_pass and negative_control_pass)

    gate_results = {}

    # helper for 3-state boolean gates
    def _gate_state(row_val: Any, default_if_missing: str = "missing_evidence") -> str:
        if row_val is None:
            return default_if_missing
        if isinstance(row_val, float) and not np.isfinite(row_val):
            return default_if_missing
        return "pass" if as_bool(row_val) else "fail"

    def _range_gate(val: float, threshold: float, mode: str = "ge") -> str:
        if not np.isfinite(val):
            return "missing_evidence"
        if mode == "ge":
            return "pass" if val >= threshold else "fail"
        return "pass" if val <= threshold else "fail"

    gate_results = {
        "statistical": (
            "pass"
            if (
                np.isfinite(q_value)
                and effective_q_value_for_check <= float(policy.max_q_value)
                and n_events >= int(policy.min_events)
            )
            else ("fail" if np.isfinite(q_value) else "missing_evidence")
        ),
        "multiplicity_scope": (
            "pass"
            if (
                (not policy.require_scope_level_multiplicity)
                or (not scope_metadata_present)
                or (
                    np.isfinite(q_value_scope)
                    and q_value_scope <= float(policy.max_q_value)
                )
                or (
                    scope_degraded
                    and bool(policy.allow_multiplicity_scope_degraded)
                )
            )
            else (
                "fail"
                if scope_metadata_present
                else "missing_evidence"
            )
        ),
        "multiplicity_diagnostics": (
            "pass"
            if (not policy.require_multiplicity_diagnostics)
            or (np.isfinite(q_value_by) and np.isfinite(q_value_cluster))
            else "missing_evidence"
        ),
        "multiplicity_confirmatory": _gate_state(
            meta.get("gate_promo_multiplicity_confirmatory"), "missing_evidence"
        ),
        "stability": (
            "pass"
            if (
                safe_float(stability.get("stability_score", np.nan), np.nan)
                >= float(policy.min_stability_score)
                and safe_float(stability.get("sign_consistency", np.nan), np.nan)
                >= float(policy.min_sign_consistency)
                and meta.get("gate_stability") is True
                and as_bool(stability.get("delay_robustness_pass", False))
            )
            else (
                "missing_evidence"
                if (
                    not np.isfinite(safe_float(stability.get("stability_score", np.nan), np.nan))
                    or "gate_stability" not in meta
                    or "delay_robustness_pass" not in stability
                )
                else "fail"
            )
        ),
        "negative_control": _gate_state(
            meta.get("gate_promo_negative_control", falsification.get("negative_control_pass"))
        ),
        "falsification": _gate_state(
            meta.get("gate_promo_falsification", falsification.get("passes_control"))
        ),
        "cost_survival": _range_gate(
            safe_float(cost.get("cost_survival_ratio", np.nan), np.nan),
            float(policy.min_cost_survival_ratio),
        ),
        "microstructure": _gate_state(cost.get("microstructure_pass"), "missing_evidence"),
        "stressed_cost_survival": _gate_state(
            meta.get("gate_after_cost_stressed_positive", cost.get("stressed_cost_pass")),
            "missing_evidence",
        ),
        "delayed_entry_stress": _gate_state(
            meta.get("gate_delayed_entry_stress"), "missing_evidence"
        ),
        "baseline_beats_complexity": _gate_state(
            meta.get("gate_promo_baseline_beats_complexity"),
            "pass" if not policy.enforce_baseline_beats_complexity else "missing_evidence",
        ),
        "placebo_controls": _gate_state(
            meta.get("gate_promo_placebo_controls", falsification.get("placebo_pass")),
            "missing_evidence",
        ),
        "timeframe_consensus": _gate_state(
            stability.get("timeframe_consensus_pass", meta.get("gate_timeframe_consensus")),
            "missing_evidence",
        ),
        "oos_validation": _gate_state(meta.get("gate_promo_oos_validation"), "missing_evidence"),
        "hypothesis_audit": (
            "pass"
            if (not policy.require_hypothesis_audit)
            or as_bool(meta.get("gate_promo_hypothesis_audit", False))
            else ("fail" if "gate_promo_hypothesis_audit" in meta else "missing_evidence")
        ),
        "retail_viability": (
            "pass"
            if (not policy.require_retail_viability)
            or as_bool(meta.get("gate_promo_retail_viability", False))
            else ("fail" if "gate_promo_retail_viability" in meta else "missing_evidence")
        ),
        "low_capital_viability": (
            "pass"
            if (not policy.require_low_capital_viability)
            or as_bool(meta.get("gate_promo_low_capital_viability", False))
            else ("fail" if "gate_promo_low_capital_viability" in meta else "missing_evidence")
        ),
        "event_discipline": (
            "pass"
            if (
                (not as_bool(meta.get("event_is_descriptive", False)))
                and as_bool(meta.get("event_is_trade_trigger", True))
                and not (
                    as_bool(meta.get("event_requires_stronger_evidence", False))
                    and as_bool(meta.get("is_reduced_evidence", False))
                )
            )
            else "fail"
        ),
        "tob_coverage": _range_gate(tob_coverage, float(policy.min_tob_coverage))
        if np.isfinite(tob_coverage)
        else "missing_evidence",
        "dsr": _gate_state(meta.get("gate_promo_dsr"), "missing_evidence"),
        "robustness": _gate_state(meta.get("gate_promo_robustness"), "missing_evidence"),
        "regime": _gate_state(meta.get("gate_promo_regime"), "missing_evidence"),
    }
    required_for_eligibility = [
        "statistical",
        "multiplicity_scope",
        "multiplicity_diagnostics",
        "multiplicity_confirmatory",
        "stability",
        "falsification",
        "cost_survival",
        "baseline_beats_complexity",
        "timeframe_consensus",
        "oos_validation",
        "microstructure",
        "stressed_cost_survival",
        "delayed_entry_stress",
        "hypothesis_audit",
        "event_discipline",
        "dsr",
        "robustness",
        "regime",
    ]
    if policy.require_retail_viability:
        required_for_eligibility.append("retail_viability")
    if policy.require_low_capital_viability:
        required_for_eligibility.append("low_capital_viability")
    if (
        not policy.require_scope_level_multiplicity
        and "multiplicity_scope" in required_for_eligibility
    ):
        required_for_eligibility.remove("multiplicity_scope")
    if (
        not policy.enforce_baseline_beats_complexity
        and "baseline_beats_complexity" in required_for_eligibility
    ):
        required_for_eligibility.remove("baseline_beats_complexity")
    if not policy.enforce_placebo_controls and "falsification" in required_for_eligibility:
        required_for_eligibility.remove("falsification")
    if not policy.enforce_timeframe_consensus and "timeframe_consensus" in required_for_eligibility:
        required_for_eligibility.remove("timeframe_consensus")
    if not policy.enforce_regime_stability and "regime" in required_for_eligibility:
        required_for_eligibility.remove("regime")
    promoted = bool(all(gate_results.get(name) == "pass" for name in required_for_eligibility))
    reasons = [name for name in required_for_eligibility if gate_results.get(name) != "pass"]
    track = "standard" if (promoted and gate_results["tob_coverage"] == "pass") else "fallback_only"
    score_axes = [
        "statistical",
        "stability",
        "cost_survival",
        "falsification",
        "timeframe_consensus",
        "oos_validation",
        "microstructure",
        "baseline_beats_complexity",
    ]
    scores = [
        1.0 if gate_results.get(name) == "pass" else 0.0
        for name in score_axes
        if name in gate_results
    ]
    rank_score = float(np.mean(scores)) if scores else 0.0
    decision = PromotionDecision(
        eligible=promoted,
        promotion_status="promoted" if promoted else "rejected",
        promotion_track=track,
        rank_score=rank_score,
        rejection_reasons=reasons,
        gate_results=gate_results,
        policy_version=policy.policy_version,
        bundle_version=policy.bundle_version,
    )
    return decision.to_dict()


def bundle_to_flat_record(bundle: Dict[str, Any]) -> Dict[str, Any]:
    stability = bundle.get("stability_tests", {})
    falsification = bundle.get("falsification_results", {})
    cost = bundle.get("cost_robustness", {})
    uncertainty = bundle.get("uncertainty_estimates", {})
    decision = bundle.get("promotion_decision", {})
    meta = bundle.get("metadata", {})
    search_burden = bundle.get("search_burden", {}) or {}
    return {
        "candidate_id": bundle.get("candidate_id", ""),
        "event_type": bundle.get("event_type", ""),
        "run_id": bundle.get("run_id", ""),
        "n_events": safe_int(bundle.get("sample_definition", {}).get("n_events", 0), 0),
        "estimate_bps": safe_float(
            bundle.get("effect_estimates", {}).get("estimate_bps", np.nan), np.nan
        ),
        "q_value": safe_float(uncertainty.get("q_value", np.nan), np.nan),
        "q_value_by": safe_float(uncertainty.get("q_value_by", np.nan), np.nan),
        "q_value_cluster": safe_float(uncertainty.get("q_value_cluster", np.nan), np.nan),
        "q_value_program": safe_float(
            bundle.get("multiplicity_adjustment", {}).get("q_value_program", np.nan), np.nan
        ),
        "stability_score": safe_float(stability.get("stability_score", np.nan), np.nan),
        "sign_consistency": safe_float(stability.get("sign_consistency", np.nan), np.nan),
        "regime_flip_flag": bool(as_bool(stability.get("regime_flip_flag", False))),
        "cross_symbol_sign_consistency": safe_float(
            stability.get("cross_symbol_sign_consistency", np.nan), np.nan
        ),
        "rolling_instability_score": safe_float(
            stability.get("rolling_instability_score", np.nan), np.nan
        ),
        "passes_control": bool(as_bool(falsification.get("passes_control", False))),
        "control_pass_rate": safe_float(falsification.get("control_pass_rate", np.nan), np.nan),
        "negative_control_pass_rate": safe_float(
            falsification.get("control_pass_rate", np.nan), np.nan
        ),
        "cost_survival_ratio": safe_float(cost.get("cost_survival_ratio", np.nan), np.nan),
        "plan_row_id": str(meta.get("plan_row_id", "")).strip(),
        "hypothesis_id": str(meta.get("hypothesis_id", "")).strip(),
        "bridge_certified": bool(as_bool(meta.get("bridge_certified", False))),
        "has_realized_oos_path": bool(as_bool(meta.get("has_realized_oos_path", False))),
        "repeated_fold_consistency": safe_float(
            meta.get("repeated_fold_consistency", np.nan), np.nan
        ),
        "structural_robustness_score": safe_float(
            meta.get("structural_robustness_score", np.nan), np.nan
        ),
        "robustness_panel_complete": bool(as_bool(meta.get("robustness_panel_complete", False))),
        "gate_regime_stability": _bool_gate_value(meta, "gate_regime_stability", False),
        "gate_structural_break": _bool_gate_value(meta, "gate_structural_break", False),
        "num_regimes_supported": safe_int(meta.get("num_regimes_supported", 0), 0),
        "promotion_decision": decision.get("promotion_status", ""),
        "promotion_track": decision.get("promotion_track", ""),
        "rank_score": safe_float(decision.get("rank_score", np.nan), np.nan),
        "is_reduced_evidence": bool(bundle.get("metadata", {}).get("is_reduced_evidence", False)),
        "rejection_reasons": "|".join(map(str, decision.get("rejection_reasons", []))),
        "policy_version": bundle.get("policy_version", ""),
        "bundle_version": bundle.get("bundle_version", ""),
        "search_proposals_attempted": safe_int(search_burden.get("search_proposals_attempted", 0), 0),
        "search_candidates_generated": safe_int(search_burden.get("search_candidates_generated", 0), 0),
        "search_candidates_eligible": safe_int(search_burden.get("search_candidates_eligible", 0), 0),
        "search_mutations_attempted": safe_int(search_burden.get("search_mutations_attempted", 0), 0),
        "search_family_count": safe_int(search_burden.get("search_family_count", 0), 0),
        "search_lineage_count": safe_int(search_burden.get("search_lineage_count", 0), 0),
        "search_burden_estimated": bool(as_bool(search_burden.get("search_burden_estimated", False))),
        "search_scope_version": str(search_burden.get("search_scope_version", "phase1_v1")),
    }


def serialize_evidence_bundles(bundles: Sequence[Dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        for bundle in bundles:
            fh.write(json.dumps(_json_safe(bundle), sort_keys=True) + "\n")
