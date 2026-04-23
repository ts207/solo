from __future__ import annotations

import pytest
from project.research.agent_io.hypothesis_contract import (
    normalize_structured_proposal,
    DEPRECATED_STATE_ANCHOR,
    UNSUPPORTED_STATE_ANCHOR_EXECUTION,
)
from project.research.agent_io.proposal_schema import compile_structured_proposal_to_agent_proposal


def _state_anchor_payload() -> dict:
    return {
        "program_id": "test_state_anchor",
        "start": "2026-01-01",
        "end": "2026-01-31",
        "symbols": ["BTCUSDT"],
        "timeframe": "1h",
        "hypothesis": {
            "anchor": {
                "type": "state",
                "state_id": "HIGH_VOL",
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


def test_state_anchor_normalization_emits_warning() -> None:
    payload = _state_anchor_payload()
    proposal, warnings = normalize_structured_proposal(payload)
    assert any(w.code == DEPRECATED_STATE_ANCHOR for w in warnings)


def test_state_anchor_execution_rejected() -> None:
    payload = _state_anchor_payload()
    proposal, _ = normalize_structured_proposal(payload)
    
    with pytest.raises(ValueError, match=UNSUPPORTED_STATE_ANCHOR_EXECUTION):
        compile_structured_proposal_to_agent_proposal(proposal)
