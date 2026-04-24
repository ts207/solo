from __future__ import annotations

from project.events.contract_registry import build_event_contract
from project.spec_validation.governance import validate_governance_consistency


def test_trade_runtime_is_generated_from_detector_governance() -> None:
    contract = build_event_contract("LIQUIDATION_EXHAUSTION_REVERSAL")

    assert contract["trade_runtime"]["source_of_truth"] == (
        "docs/generated/detector_eligibility_matrix.json"
    )
    assert contract["trade_runtime"]["eligible"] is False
    assert contract["trade_runtime"]["reason"] == "generated_governance_runtime_false"


def test_local_trade_runtime_lint_matches_generated_governance() -> None:
    errors = validate_governance_consistency()

    assert not [
        err
        for err in errors
        if err[0].endswith("LIQUIDATION_EXHAUSTION_REVERSAL.yaml")
    ]
