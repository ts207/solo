from __future__ import annotations

from pathlib import Path

from project.research import phase2_spec_registry as p2_spec
from project.specs.gates import load_gates_spec, resolve_phase2_gate_params, select_phase2_gate_spec


def test_resolve_phase2_gate_params_applies_event_override():
    gate_v1_phase2 = {
        "min_t_stat": 1.5,
        "max_q_value": 0.05,
        "min_after_cost_expectancy_bps": 0.1,
        "min_sample_size": 50,
        "quality_floor_fallback": 0.66,
        "min_events_fallback": 100,
        "conservative_cost_multiplier": 1.5,
        "event_overrides": {
            "OI_SPIKE_NEGATIVE": {
                "min_after_cost_expectancy_bps": 0.0,
                "conservative_cost_multiplier": 1.1,
            }
        },
    }

    cfg = resolve_phase2_gate_params(gate_v1_phase2, "OI_SPIKE_NEGATIVE")
    assert cfg["min_t_stat"] == 1.5
    assert cfg["max_q_value"] == 0.05
    assert cfg["min_after_cost_expectancy_bps"] == 0.0
    assert cfg["conservative_cost_multiplier"] == 1.1
    assert cfg["min_sample_size"] == 50
    assert cfg["conditioned_bucket_hard_floor"] == 30
    assert cfg["conditioned_bucket_min_samples_override"] is None
    assert cfg["allow_conditioned_bucket_floor_override"] is False


def test_select_phase2_gate_spec_auto_research_uses_discovery_profile():
    gates_spec = {
        "gate_v1_phase2": {"max_q_value": 0.05},
        "gate_v1_phase2_profiles": {
            "discovery": {"max_q_value": 0.10},
            "promotion": {"max_q_value": 0.01},
        },
    }
    selected = p2_spec._select_phase2_gate_spec(gates_spec, mode="research", gate_profile="auto")
    assert selected["max_q_value"] == 0.10
    assert selected["_resolved_profile"] == "discovery"


def test_select_phase2_gate_spec_auto_production_uses_promotion_profile():
    gates_spec = {
        "gate_v1_phase2": {"max_q_value": 0.05},
        "gate_v1_phase2_profiles": {
            "discovery": {"max_q_value": 0.10},
            "promotion": {"max_q_value": 0.01},
        },
    }
    selected = p2_spec._select_phase2_gate_spec(gates_spec, mode="production", gate_profile="auto")
    assert selected["max_q_value"] == 0.01
    assert selected["_resolved_profile"] == "promotion"


def test_select_phase2_gate_spec_supports_synthetic_profile():
    gates_spec = {
        "gate_v1_phase2": {"min_t_stat": 1.5, "max_q_value": 0.05, "min_sample_size": 50},
        "gate_v1_phase2_profiles": {
            "synthetic": {"min_t_stat": 0.25, "max_q_value": 0.35, "min_sample_size": 8},
        },
    }
    selected = p2_spec._select_phase2_gate_spec(
        gates_spec, mode="research", gate_profile="synthetic"
    )
    assert selected["min_t_stat"] == 0.25
    assert selected["max_q_value"] == 0.35
    assert selected["min_sample_size"] == 8
    assert selected["_resolved_profile"] == "synthetic"


def test_load_family_spec_reads_repo_spec():
    REPO_ROOT = Path(__file__).resolve().parents[3]
    spec = p2_spec._load_family_spec(REPO_ROOT)
    families = spec.get("families", {})
    assert "OI_FLUSH" in families
    assert families["OI_FLUSH"]["templates"] == [
        "reversal_or_squeeze",
        "mean_reversion",
        "continuation",
        "exhaustion_reversal",
        "convexity_capture",
        "only_if_funding",
        "only_if_oi",
        "tail_risk_avoid",
    ]


def test_resolve_phase2_gate_params_liquidity_vacuum_tuned_override():
    REPO_ROOT = Path(__file__).resolve().parents[3]
    gates_spec = load_gates_spec(REPO_ROOT)
    gates = select_phase2_gate_spec(gates_spec, mode="research", gate_profile="auto")
    cfg = resolve_phase2_gate_params(gates, "LIQUIDITY_VACUUM")
    assert cfg["max_q_value"] == 0.20
    assert cfg["min_after_cost_expectancy_bps"] == -5.0
    assert cfg["conservative_cost_multiplier"] == 1.0
    assert cfg["require_sign_stability"] is False


def test_discovery_profile_relaxes_structural_phase2_gates():
    REPO_ROOT = Path(__file__).resolve().parents[3]
    gates_spec = load_gates_spec(REPO_ROOT)
    gates = select_phase2_gate_spec(gates_spec, mode="research", gate_profile="auto")
    cfg = resolve_phase2_gate_params(gates, "VOL_SHOCK")
    assert cfg["regime_ess_min_regimes"] == 1
    assert cfg["timeframe_consensus_min_timeframes"] == 1


def test_promotion_profile_does_not_include_discovery_event_relaxation():
    REPO_ROOT = Path(__file__).resolve().parents[3]
    gates_spec = load_gates_spec(REPO_ROOT)
    gates = select_phase2_gate_spec(gates_spec, mode="production", gate_profile="auto")
    cfg = resolve_phase2_gate_params(gates, "LIQUIDITY_VACUUM")
    assert cfg["max_q_value"] == 0.05
    assert cfg["min_after_cost_expectancy_bps"] == 0.1
    assert cfg["conservative_cost_multiplier"] == 1.5
    assert cfg["require_sign_stability"] is True


def test_promotion_profile_keeps_strict_structural_phase2_gates():
    REPO_ROOT = Path(__file__).resolve().parents[3]
    gates_spec = load_gates_spec(REPO_ROOT)
    gates = select_phase2_gate_spec(gates_spec, mode="production", gate_profile="auto")
    cfg = resolve_phase2_gate_params(gates, "VOL_SHOCK")
    assert cfg["regime_ess_min_regimes"] == 2
    assert cfg["timeframe_consensus_min_timeframes"] == 2


def test_load_family_spec_supports_conditioning_cols_override():
    REPO_ROOT = Path(__file__).resolve().parents[3]
    spec = p2_spec._load_family_spec(REPO_ROOT)
    families = spec.get("families", {})
    assert families["LIQUIDITY_VACUUM"]["templates"] == [
        "mean_reversion",
        "stop_run_repair",
        "overshoot_repair",
        "continuation",
        "only_if_liquidity",
        "slippage_aware_filter",
    ]
    assert families["LIQUIDITY_VACUUM"]["horizons"] == ["15m"]
    assert families["LIQUIDITY_VACUUM"]["conditioning_cols"] == []
    assert "5m" in families["LIQUIDATION_CASCADE"]["horizons"]
    assert families["LIQUIDATION_CASCADE"].get("conditioning_cols", []) == []
