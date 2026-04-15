from __future__ import annotations

import pytest
from project.research.agent_io.proposal_schema import (
    detect_operator_proposal_format,
    load_normalized_operator_proposal,
    load_operator_proposal,
)


def _legacy_payload() -> dict:
    return {
        "program_id": "legacy_program",
        "start": "2026-01-01",
        "end": "2026-01-31",
        "symbols": ["BTCUSDT"],
        "trigger_space": {
            "allowed_trigger_types": ["EVENT"],
            "events": {"include": ["BASIS_DISLOC"]},
        },
        "templates": ["continuation"],
        "horizons_bars": [12],
        "directions": ["long"],
        "entry_lags": [1],
    }


def _single_hypo_payload() -> dict:
    return {
        "program_id": "single_hypo_program",
        "start": "2026-01-01",
        "end": "2026-01-31",
        "symbols": ["BTCUSDT"],
        "timeframe": "5m",
        "hypothesis": {
            "trigger": {
                "type": "event",
                "event_id": "BASIS_DISLOC",
            },
            "template": "continuation",
            "direction": "long",
            "horizon_bars": 12,
            "entry_lag_bars": 1,
        },
    }


def _structured_payload() -> dict:
    return {
        "program_id": "structured_program",
        "start": "2026-01-01",
        "end": "2026-01-31",
        "symbols": ["BTCUSDT"],
        "timeframe": "1h",
        "hypothesis": {
            "anchor": {
                "type": "event",
                "event_id": "TEST_EVENT",
            },
            "filters": {
                "states": ["NORMAL"],
            },
            "sampling_policy": {
                "mode": "episodic",
                "entry_lag_bars": 1,
            },
            "template": {
                "id": "test_template",
            },
            "direction": "long",
            "horizon_bars": 10,
        },
    }


def test_detect_format() -> None:
    assert detect_operator_proposal_format(_legacy_payload()) == "legacy"
    assert (
        detect_operator_proposal_format(_single_hypo_payload()) == "single_hypothesis"
    )
    assert (
        detect_operator_proposal_format(_structured_payload()) == "structured_hypothesis"
    )


def test_load_normalized_from_legacy() -> None:
    proposal = load_normalized_operator_proposal(_legacy_payload())
    assert proposal.program_id == "legacy_program"
    assert proposal.hypothesis.anchor.type == "event"
    assert proposal.hypothesis.anchor.event_id == "BASIS_DISLOC"


def test_load_normalized_from_single_hypo() -> None:
    proposal = load_normalized_operator_proposal(_single_hypo_payload())
    assert proposal.program_id == "single_hypo_program"
    assert proposal.hypothesis.anchor.type == "event"
    assert proposal.hypothesis.anchor.event_id == "BASIS_DISLOC"


def test_load_operator_proposal_from_structured() -> None:
    # This should return an AgentProposal (the executable shape)
    agent_proposal = load_operator_proposal(_structured_payload())
    assert agent_proposal.program_id == "structured_program"
    assert agent_proposal.templates == ["test_template"]
    assert agent_proposal.trigger_space["allowed_trigger_types"] == ["EVENT"]
    assert agent_proposal.trigger_space["events"]["include"] == ["TEST_EVENT"]
    assert agent_proposal.trigger_space["states"]["include"] == ["NORMAL"]


def test_legacy_multi_hypo_normalization_fails() -> None:
    payload = _legacy_payload()
    payload["templates"] = ["T1", "T2"]
    with pytest.raises(ValueError, match="multiple hypotheses"):
        load_normalized_operator_proposal(payload)
