from pathlib import Path

from project.research.campaign_contract import load_campaign_contract


def test_load_campaign_contract_from_legacy_payload():
    contract = load_campaign_contract(
        {
            "campaign_id": "camp_1",
            "program_id": "prog_1",
            "initial_proposal": "proposal.yaml",
            "max_cycles": 3,
            "stop_conditions": {"max_fail_streak": 2},
        }
    )

    assert contract.campaign_id == "camp_1"
    assert contract.program_id == "prog_1"
    assert contract.stop_conditions.max_cycles == 3
    assert contract.stop_conditions.max_fail_streak == 2
    assert contract.mode == "operator_guided"
