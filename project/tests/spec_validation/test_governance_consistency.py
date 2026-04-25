from __future__ import annotations

from project.events.contract_registry import build_event_contract
from project.spec_validation import governance as governance_lint
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


def test_local_governance_lint_detects_non_runtime_mismatch(monkeypatch) -> None:
    monkeypatch.setattr(
        governance_lint,
        "_load_json",
        lambda _: [
            {
                "event_name": "TEST_EVENT",
                "runtime": False,
                "promotion": False,
                "anchor": False,
                "planning": False,
                "detector_band": "research_trigger",
            }
        ],
    )
    monkeypatch.setattr(
        governance_lint,
        "_load_active_event_contracts",
        lambda **_: {
            "TEST_EVENT": {
                "raw": {
                    "governance": {
                        "promotion_eligible": True,
                        "primary_anchor_eligible": True,
                        "detector_band": "deployable_core",
                    },
                    "trade_runtime": {"eligible": False},
                }
            }
        },
    )

    errors = validate_governance_consistency()

    messages = [message for _, message in errors]
    assert any("governance.promotion_eligible" in message for message in messages)
    assert any("governance.primary_anchor_eligible" in message for message in messages)
    assert any("governance.detector_band" in message for message in messages)
