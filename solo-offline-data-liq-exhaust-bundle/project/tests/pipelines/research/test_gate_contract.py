from __future__ import annotations

from pathlib import Path

from project.specs import gates


def test_select_phase2_gate_spec_auto_profiles():
    spec = {
        "gate_v1_phase2": {"max_q_value": 0.05},
        "gate_v1_phase2_profiles": {
            "discovery": {"max_q_value": 0.10},
            "promotion": {"max_q_value": 0.01},
        },
    }
    discovery = gates.select_phase2_gate_spec(spec, mode="research", gate_profile="auto")
    promotion = gates.select_phase2_gate_spec(spec, mode="production", gate_profile="auto")

    assert discovery["max_q_value"] == 0.10
    assert discovery["_resolved_profile"] == "discovery"
    assert promotion["max_q_value"] == 0.01
    assert promotion["_resolved_profile"] == "promotion"


def test_resolve_phase2_gate_params_applies_event_override():
    phase2_cfg = {
        "min_t_stat": 1.5,
        "max_q_value": 0.05,
        "min_after_cost_expectancy_bps": 0.1,
        "min_sample_size": 50,
        "quality_floor_fallback": 0.66,
        "min_events_fallback": 100,
        "conservative_cost_multiplier": 1.5,
        "event_overrides": {
            "LIQUIDITY_VACUUM": {
                "min_after_cost_expectancy_bps": -5.0,
                "conservative_cost_multiplier": 1.0,
            }
        },
    }

    cfg = gates.resolve_phase2_gate_params(phase2_cfg, "LIQUIDITY_VACUUM")
    assert cfg["min_t_stat"] == 1.5
    assert cfg["max_q_value"] == 0.05
    assert cfg["min_after_cost_expectancy_bps"] == -5.0
    assert cfg["conservative_cost_multiplier"] == 1.0
    assert cfg["min_sample_size"] == 50
    assert cfg["conditioned_bucket_hard_floor"] == 30
    assert cfg["conditioned_bucket_min_samples_override"] is None
    assert cfg["allow_conditioned_bucket_floor_override"] is False


def test_resolve_phase2_gate_params_reads_conditioned_bucket_override_policy():
    phase2_cfg = {
        "conditioned_bucket_hard_floor": 35,
        "allow_conditioned_bucket_floor_override": True,
        "event_overrides": {
            "OI_SPIKE_POSITIVE": {
                "conditioned_bucket_min_samples_override": 20,
            }
        },
    }

    cfg = gates.resolve_phase2_gate_params(phase2_cfg, "OI_SPIKE_POSITIVE")
    assert cfg["conditioned_bucket_hard_floor"] == 35
    assert cfg["conditioned_bucket_min_samples_override"] == 20
    assert cfg["allow_conditioned_bucket_floor_override"] is True


def test_select_fallback_gate_spec_defaults_when_missing():
    out = gates.select_fallback_gate_spec({})
    assert out["min_t_stat"] == 2.5
    assert out["min_after_cost_expectancy_bps"] == 1.0
    assert out["min_sample_size"] == 100


def test_select_bridge_gate_spec_reads_override():
    out = gates.select_bridge_gate_spec(
        {
            "gate_v1_bridge": {
                "edge_cost_k": 1.75,
                "stressed_cost_multiplier": 1.25,
                "min_validation_trades": 12,
                "search_bridge_min_t_stat": 1.9,
                "search_bridge_min_robustness_score": 0.65,
                "search_bridge_min_regime_stability_score": 0.55,
                "search_bridge_min_stress_survival": 0.4,
                "search_bridge_stress_cost_buffer_bps": 1.5,
            }
        }
    )
    assert out["edge_cost_k"] == 1.75
    assert out["stressed_cost_multiplier"] == 1.25
    assert out["min_validation_trades"] == 12
    assert out["search_bridge_min_t_stat"] == 1.9
    assert out["search_bridge_min_robustness_score"] == 0.65
    assert out["search_bridge_min_regime_stability_score"] == 0.55
    assert out["search_bridge_min_stress_survival"] == 0.4
    assert out["search_bridge_stress_cost_buffer_bps"] == 1.5


def test_resolve_promotion_base_min_events_uses_spec_and_contract_floor():
    out = gates.resolve_promotion_base_min_events(
        {"gate_v1_phase2": {"min_sample_size": 50}},
        cli_min_events=80,
        contract_min_trade_count=120,
    )
    assert out == 120


def test_load_gates_spec_reads_repo_spec():
    repo_root = Path(__file__).resolve().parents[3]
    spec = gates.load_gates_spec(repo_root)
    assert "gate_v1_phase2" in spec
