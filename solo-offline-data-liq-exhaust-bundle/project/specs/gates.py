from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from project.spec_registry import load_family_spec as _registry_load_family_spec
from project.spec_registry import load_family_specs as _registry_load_family_specs
from project.spec_registry import load_gates_spec as _registry_load_gates_spec


def load_gates_spec(repo_root: Path) -> Dict[str, Any]:
    return dict(_registry_load_gates_spec())


def load_all_families_spec(repo_root: Path) -> Dict[str, Any]:
    return dict(_registry_load_family_specs())


def load_family_spec(family_id: str, repo_root: Path) -> Dict[str, Any]:
    """
    Load a specific family's spec from families.yaml.
    """
    return dict(_registry_load_family_spec(family_id))


def select_phase2_gate_spec(
    gates_spec: Dict[str, Any],
    *,
    mode: str,
    gate_profile: str,
) -> Dict[str, Any]:
    base_cfg = gates_spec.get("gate_v1_phase2", {}) if isinstance(gates_spec, dict) else {}
    if not isinstance(base_cfg, dict):
        base_cfg = {}

    profile_choice = str(gate_profile or "auto").strip().lower()
    mode_choice = str(mode or "production").strip().lower()
    if profile_choice == "auto":
        resolved_profile = "discovery" if mode_choice == "research" else "promotion"
    else:
        resolved_profile = profile_choice

    profiles = gates_spec.get("gate_v1_phase2_profiles", {}) if isinstance(gates_spec, dict) else {}
    profile_cfg = profiles.get(resolved_profile, {}) if isinstance(profiles, dict) else {}
    if not isinstance(profile_cfg, dict):
        profile_cfg = {}

    merged = dict(base_cfg)
    for key, value in profile_cfg.items():
        if key == "event_overrides" and isinstance(value, dict):
            merged_overrides = merged.get("event_overrides", {})
            if not isinstance(merged_overrides, dict):
                merged_overrides = {}
            merged_overrides = dict(merged_overrides)
            for event_type, override_cfg in value.items():
                if isinstance(override_cfg, dict):
                    prior = merged_overrides.get(event_type, {})
                    if not isinstance(prior, dict):
                        prior = {}
                    merged_overrides[event_type] = {**prior, **override_cfg}
                else:
                    merged_overrides[event_type] = override_cfg
            merged["event_overrides"] = merged_overrides
        else:
            merged[key] = value
    merged["_resolved_profile"] = resolved_profile
    return merged


def resolve_phase2_gate_params(
    gate_v1_phase2: Dict[str, Any],
    event_type: str,
) -> Dict[str, Any]:
    event_overrides = (
        gate_v1_phase2.get("event_overrides", {}) if isinstance(gate_v1_phase2, dict) else {}
    )
    per_event = event_overrides.get(event_type, {}) if isinstance(event_overrides, dict) else {}
    if not isinstance(per_event, dict):
        per_event = {}

    def _pick(key: str, default: Any) -> Any:
        if key in per_event:
            return per_event[key]
        if isinstance(gate_v1_phase2, dict) and key in gate_v1_phase2:
            return gate_v1_phase2[key]
        return default

    conservative_mult = float(_pick("conservative_cost_multiplier", 1.5))
    conservative_mult = max(1.0, conservative_mult)
    conditioned_bucket_hard_floor = int(_pick("conditioned_bucket_hard_floor", 30) or 30)
    conditioned_bucket_hard_floor = max(1, conditioned_bucket_hard_floor)
    conditioned_bucket_min_samples_override_raw = _pick(
        "conditioned_bucket_min_samples_override", None
    )
    conditioned_bucket_min_samples_override = None
    if conditioned_bucket_min_samples_override_raw is not None:
        try:
            conditioned_bucket_min_samples_override = int(
                conditioned_bucket_min_samples_override_raw
            )
        except (TypeError, ValueError):
            conditioned_bucket_min_samples_override = None
    if (
        conditioned_bucket_min_samples_override is not None
        and conditioned_bucket_min_samples_override <= 0
    ):
        conditioned_bucket_min_samples_override = None
    timeframe_consensus_timeframes = _pick("timeframe_consensus_timeframes", ["1m", "5m", "15m"])
    if isinstance(timeframe_consensus_timeframes, str):
        timeframe_consensus_timeframes = [
            token.strip() for token in timeframe_consensus_timeframes.split(",") if token.strip()
        ]
    if not isinstance(timeframe_consensus_timeframes, list):
        timeframe_consensus_timeframes = ["1m", "5m", "15m"]
    return {
        "min_t_stat": float(_pick("min_t_stat", 1.5)),
        "max_q_value": float(_pick("max_q_value", 0.05)),
        "min_after_cost_expectancy_bps": float(_pick("min_after_cost_expectancy_bps", 0.1)),
        "min_sample_size": int(_pick("min_sample_size", 0) or 0),
        "require_sign_stability": bool(_pick("require_sign_stability", True)),
        "quality_floor_fallback": float(_pick("quality_floor_fallback", 0.66)),
        "min_events_fallback": int(_pick("min_events_fallback", 100)),
        "conservative_cost_multiplier": conservative_mult,
        "conditioned_bucket_hard_floor": conditioned_bucket_hard_floor,
        "conditioned_bucket_min_samples_override": conditioned_bucket_min_samples_override,
        "allow_conditioned_bucket_floor_override": bool(
            _pick("allow_conditioned_bucket_floor_override", False)
        ),
        "regime_ess_min_per_regime": float(_pick("regime_ess_min_per_regime", 1.0)),
        "regime_ess_min_regimes": int(_pick("regime_ess_min_regimes", 1) or 1),
        "timeframe_consensus_timeframes": timeframe_consensus_timeframes,
        "timeframe_consensus_min_ratio": float(_pick("timeframe_consensus_min_ratio", 0.0)),
        "timeframe_consensus_min_timeframes": int(
            _pick("timeframe_consensus_min_timeframes", 1) or 1
        ),
        "multiplicity_enable_cluster_adjusted": bool(
            _pick("multiplicity_enable_cluster_adjusted", True)
        ),
        "multiplicity_cluster_threshold": float(_pick("multiplicity_cluster_threshold", 0.85)),
        "multiplicity_enable_by_diagnostic": bool(_pick("multiplicity_enable_by_diagnostic", True)),
    }


def select_fallback_gate_spec(gates_spec: Dict[str, Any]) -> Dict[str, Any]:
    fallback_cfg = gates_spec.get("gate_v1_fallback", {}) if isinstance(gates_spec, dict) else {}
    if not isinstance(fallback_cfg, dict):
        fallback_cfg = {}
    return {
        "min_t_stat": float(fallback_cfg.get("min_t_stat", 2.5)),
        "min_after_cost_expectancy_bps": float(
            fallback_cfg.get("min_after_cost_expectancy_bps", 1.0)
        ),
        "min_sample_size": int(fallback_cfg.get("min_sample_size", 100) or 100),
        "min_stability_score": float(fallback_cfg.get("min_stability_score", 0.7)),
        "promotion_eligible_regardless_of_fdr": bool(
            fallback_cfg.get("promotion_eligible_regardless_of_fdr", False)
        ),
    }


def select_bridge_gate_spec(gates_spec: Dict[str, Any]) -> Dict[str, Any]:
    bridge_cfg = gates_spec.get("gate_v1_bridge", {}) if isinstance(gates_spec, dict) else {}
    if not isinstance(bridge_cfg, dict):
        bridge_cfg = {}
    return {
        "edge_cost_k": float(bridge_cfg.get("edge_cost_k", 2.0)),
        "stressed_cost_multiplier": float(bridge_cfg.get("stressed_cost_multiplier", 1.5)),
        "min_validation_trades": int(bridge_cfg.get("min_validation_trades", 20) or 20),
        "search_bridge_min_t_stat": float(bridge_cfg.get("search_bridge_min_t_stat", 2.0)),
        "search_bridge_min_robustness_score": float(
            bridge_cfg.get("search_bridge_min_robustness_score", 0.7)
        ),
        "search_bridge_min_regime_stability_score": float(
            bridge_cfg.get("search_bridge_min_regime_stability_score", 0.6)
        ),
        "search_bridge_min_stress_survival": float(
            bridge_cfg.get("search_bridge_min_stress_survival", 0.5)
        ),
        "search_bridge_stress_cost_buffer_bps": float(
            bridge_cfg.get("search_bridge_stress_cost_buffer_bps", 2.0)
        ),
        "micro_max_spread_stress": float(bridge_cfg.get("micro_max_spread_stress", 2.0)),
        "micro_max_depth_depletion": float(bridge_cfg.get("micro_max_depth_depletion", 0.70)),
        "micro_max_sweep_pressure": float(bridge_cfg.get("micro_max_sweep_pressure", 2.5)),
        "micro_max_abs_imbalance": float(bridge_cfg.get("micro_max_abs_imbalance", 0.90)),
        "micro_min_feature_coverage": float(bridge_cfg.get("micro_min_feature_coverage", 0.25)),
    }


def resolve_promotion_base_min_events(
    gates_spec: Dict[str, Any],
    *,
    cli_min_events: int,
    contract_min_trade_count: int,
) -> int:
    phase2_cfg = gates_spec.get("gate_v1_phase2", {}) if isinstance(gates_spec, dict) else {}
    if not isinstance(phase2_cfg, dict):
        phase2_cfg = {}
    spec_min = int(phase2_cfg.get("min_sample_size", cli_min_events) or cli_min_events)
    base_min = max(int(cli_min_events), spec_min)
    return max(base_min, int(contract_min_trade_count))
