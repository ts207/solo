from __future__ import annotations

from project.events.registry import EVENT_REGISTRY_SPECS
from project.pipelines import run_all
from project.strategy.dsl.policies import DEFAULT_POLICY, event_policy


def test_phase2_chain_covers_all_registry_event_families():
    chain_events = {str(event).strip() for event, _, _ in run_all.PHASE2_EVENT_CHAIN}
    registry_events = set(EVENT_REGISTRY_SPECS.keys())
    assert chain_events == registry_events


def test_every_chain_family_has_explicit_policy_and_signal_trigger():
    for event_type, _, _ in run_all.PHASE2_EVENT_CHAIN:
        spec = EVENT_REGISTRY_SPECS[event_type]
        policy = event_policy(event_type)
        assert policy != DEFAULT_POLICY, f"event_type={event_type} is using DEFAULT_POLICY"
        triggers = [str(x) for x in policy.get("triggers", [])]
        assert spec.signal_column in triggers, (
            f"event_type={event_type} missing required trigger signal_column={spec.signal_column}; "
            f"got triggers={triggers}"
        )
