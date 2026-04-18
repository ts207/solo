from __future__ import annotations

from copy import deepcopy

from project.events.detectors.registry import (
    get_detector_class,
    get_detector_metadata,
    get_detector_metadata_adapter_class,
)
from project.events.registry import (
    get_detector_contract,
    load_milestone_event_registry,
    validate_detector_contract_implementation_parity,
    validate_detector_registry_implementation_parity,
)


def test_detector_contract_implementation_parity_is_clean() -> None:
    assert validate_detector_contract_implementation_parity() == {}


def test_detector_registry_implementation_parity_is_clean() -> None:
    assert validate_detector_registry_implementation_parity() == {}


def test_all_detector_contracts_use_registered_class_metadata() -> None:
    for event_name in load_milestone_event_registry():
        contract = get_detector_contract(event_name)
        detector_cls = get_detector_class(event_name)
        assert detector_cls is not None, event_name
        _, metadata = get_detector_metadata(event_name, load_milestone_event_registry()[event_name])
        assert metadata is not None
        assert contract.required_columns == metadata.required_columns
        assert contract.supports_confidence == metadata.supports_confidence
        assert contract.supports_severity == metadata.supports_severity
        assert contract.supports_quality_flag == metadata.supports_quality_flag
        assert contract.cooldown_semantics == metadata.cooldown_semantics
        assert contract.merge_key_strategy == metadata.merge_key_strategy


def test_registered_detector_metadata_protocol_is_clean() -> None:
    rows = load_milestone_event_registry()
    for event_name, row in rows.items():
        adapter_cls = get_detector_metadata_adapter_class(event_name, row)
        assert adapter_cls is not None, event_name
        _, metadata = get_detector_metadata(event_name, row)
        assert metadata is not None, event_name
        assert metadata.event_name == event_name
        assert metadata.detector_class
        assert metadata.required_columns
        assert metadata.cooldown_semantics
        assert metadata.merge_key_strategy


def test_v1_detector_metadata_adapter_applies_registry_governance_fields() -> None:
    row = load_milestone_event_registry()["SESSION_OPEN_EVENT"]
    raw_cls = get_detector_class("SESSION_OPEN_EVENT")
    adapter_cls = get_detector_metadata_adapter_class("SESSION_OPEN_EVENT", row)

    assert raw_cls is not None
    assert adapter_cls is not None
    assert adapter_cls is not raw_cls

    metadata = adapter_cls.detector_metadata(event_name="SESSION_OPEN_EVENT")
    assert metadata.role == "context"
    assert metadata.detector_band == "context_only"
    assert metadata.planning_default is True
    assert metadata.runtime_default is False
    assert metadata.primary_anchor_eligible is False


def test_registry_parity_detects_required_columns_drift() -> None:
    rows = deepcopy(load_milestone_event_registry())
    rows["VOL_SPIKE"]["detector"] = {
        **dict(rows["VOL_SPIKE"].get("detector", {})),
        "required_columns": ["timestamp", "bogus_column"],
    }

    mismatches = validate_detector_registry_implementation_parity(rows)
    assert mismatches["VOL_SPIKE"]["required_columns"]["registry"] == (
        "timestamp",
        "bogus_column",
    )


def test_registry_parity_detects_capability_drift() -> None:
    rows = deepcopy(load_milestone_event_registry())
    rows["VOL_SPIKE"]["supports_confidence"] = False
    rows["VOL_SPIKE"]["supports_quality_flag"] = False

    mismatches = validate_detector_registry_implementation_parity(rows)
    assert mismatches["VOL_SPIKE"]["supports_confidence"]["registry"] is False
    assert mismatches["VOL_SPIKE"]["supports_quality_flag"]["registry"] is False


def test_runtime_contracts_have_complete_implementation_fields() -> None:
    runtime_contracts = [
        get_detector_contract(event_name)
        for event_name in load_milestone_event_registry()
        if get_detector_contract(event_name).runtime_default
    ]
    assert runtime_contracts
    for contract in runtime_contracts:
        assert contract.event_version == "v2"
        assert contract.required_columns
        assert contract.supports_confidence is True
        assert contract.supports_severity is True
        assert contract.supports_quality_flag is True
        assert contract.cooldown_semantics
        assert contract.merge_key_strategy
