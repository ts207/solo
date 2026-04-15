from __future__ import annotations

from project.spec_registry import (
    load_blueprint_policy_spec,
    load_gates_spec,
    load_template_registry,
)


def test_registry_loads_gates_from_single_authority():
    gates = load_gates_spec()
    assert isinstance(gates, dict)
    assert "gate_v1_phase2" in gates


def test_registry_loads_blueprint_policy_with_defaults():
    policy = load_blueprint_policy_spec()
    assert policy["execution"]["default_mode"]
    assert policy["time_stop"]["min_bars"] >= 1
    assert policy["stop_target"]["target_to_stop_min_ratio"] >= 1.0


def test_template_registry_loads_from_canonical_template_source():
    template_registry = load_template_registry()
    assert template_registry.get("kind") == "event_template_registry"
    assert template_registry.get("metadata", {}).get("status") == "authoritative"
    assert "operators" in template_registry
