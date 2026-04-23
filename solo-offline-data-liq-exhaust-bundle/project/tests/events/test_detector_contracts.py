import pytest
from project.events.registry import (
    load_milestone_event_registry,
    get_detector_contract,
    list_trigger_detectors,
    list_context_detectors,
    list_runtime_eligible_detectors,
    list_promotion_eligible_detectors,
)
from project.events.detector_contract import DetectorContract, DetectorContractError

def test_all_registered_detectors_load_under_new_contract():
    registry = load_milestone_event_registry()
    assert len(registry) > 0, "Registry should not be empty"
    for event_name in registry.keys():
        contract = get_detector_contract(event_name)
        assert isinstance(contract, DetectorContract)
        assert contract.event_name == event_name or contract.event_name in contract.aliases or event_name in contract.aliases

def test_filtered_views_work():
    triggers = list_trigger_detectors()
    assert len(triggers) > 0
    assert all(c.role == "trigger" for c in triggers)

    contexts = list_context_detectors()
    assert len(contexts) >= 0
    assert all(c.role == "context" for c in contexts)

    runtime_eligible = list_runtime_eligible_detectors()
    assert all(c.runtime_default for c in runtime_eligible)

def test_governance_rules():
    registry = load_milestone_event_registry()
    for event_name in registry.keys():
        contract = get_detector_contract(event_name)
        if contract.role == "context":
            assert not contract.primary_anchor_eligible, f"{event_name} is context but primary_anchor_eligible"
        if contract.role == "research_only":
            assert not contract.runtime_default, f"{event_name} is research_only but runtime_default"
        if contract.maturity == "deprecated":
            assert not contract.runtime_default, f"{event_name} is deprecated but runtime_default"
