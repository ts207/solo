from __future__ import annotations

import pytest

from project.research.agent_io.hypothesis_contract import (
    UNSUPPORTED_SAMPLING_POLICY_EXECUTION,
    normalize_structured_proposal,
)
from project.research.agent_io.proposal_schema import compile_structured_proposal_to_agent_proposal


def _valid_structured_payload() -> dict:
    return {
        "program_id": "test_program",
        "start": "2026-01-01",
        "end": "2026-01-31",
        "symbols": ["BTCUSDT"],
        "timeframe": "1h",
        "hypothesis": {
            "anchor": {
                "type": "event",
                "event_id": "TEST_EVENT",
            },
            "filters": {},
            "sampling_policy": {
                "mode": "episodic",
                "entry_lag_bars": 1,
                "overlap_policy": "suppress",
            },
            "template": {
                "id": "test_template",
            },
            "direction": "long",
            "horizon_bars": 10,
        },
    }


def test_episodic_policy_accepted() -> None:
    payload = _valid_structured_payload()
    proposal, _ = normalize_structured_proposal(payload)
    # Should not raise
    compile_structured_proposal_to_agent_proposal(proposal)


def test_continuous_policy_rejected() -> None:
    payload = _valid_structured_payload()
    payload["hypothesis"]["sampling_policy"]["mode"] = "continuous"
    proposal, _ = normalize_structured_proposal(payload)

    with pytest.raises(ValueError, match=UNSUPPORTED_SAMPLING_POLICY_EXECUTION):
        compile_structured_proposal_to_agent_proposal(proposal)


def test_onset_only_rejected() -> None:
    payload = _valid_structured_payload()
    payload["hypothesis"]["sampling_policy"]["mode"] = "onset_only"
    proposal, _ = normalize_structured_proposal(payload)

    with pytest.raises(ValueError, match=UNSUPPORTED_SAMPLING_POLICY_EXECUTION):
        compile_structured_proposal_to_agent_proposal(proposal)
