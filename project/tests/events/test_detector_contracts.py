from project.events.detector_contract import DetectorContract
from project.events.detectors.registry import get_detector_class
from project.events.registry import (
    get_detector_contract,
    list_context_detectors,
    list_runtime_eligible_detectors,
    list_trigger_detectors,
    load_milestone_event_registry,
)


def test_all_registered_detectors_load_under_new_contract():
    registry = load_milestone_event_registry()
    assert len(registry) > 0, "Registry should not be empty"
    for event_name in registry:
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
    for event_name in registry:
        contract = get_detector_contract(event_name)
        if contract.role == "context":
            assert not contract.primary_anchor_eligible, f"{event_name} is context but primary_anchor_eligible"
        if contract.role == "research_only":
            assert not contract.runtime_default, f"{event_name} is research_only but runtime_default"
        if contract.maturity == "deprecated":
            assert not contract.runtime_default, f"{event_name} is deprecated but runtime_default"


def test_composite_detector_children_are_instantiable():
    for event_name in (
        "LIQUIDATION_EXHAUSTION_REVERSAL",
        "POST_DELEVERAGING_REBOUND",
        "TREND_EXHAUSTION_TRIGGER",
        "ORDERFLOW_IMBALANCE_SHOCK",
        "FLOW_EXHAUSTION_PROXY",
        "FORCED_FLOW_EXHAUSTION",
    ):
        detector_cls = get_detector_class(event_name)
        assert detector_cls is not None
        assert not getattr(detector_cls, "__abstractmethods__", None)
        assert detector_cls() is not None
