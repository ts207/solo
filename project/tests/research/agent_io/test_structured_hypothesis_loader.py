from __future__ import annotations

import pytest
from project.research.agent_io.hypothesis_contract import (
    normalize_structured_proposal,
    DEPRECATED_STATE_ANCHOR,
)


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
            "filters": {
                "states": ["NORMAL"],
                "regimes": ["UP"],
            },
            "sampling_policy": {
                "mode": "episodic",
                "entry_lag_bars": 1,
                "overlap_policy": "suppress",
            },
            "template": {
                "id": "test_template",
                "params": {"p1": 1},
            },
            "direction": "long",
            "horizon_bars": 10,
        },
    }


def test_normalize_valid_structured_proposal() -> None:
    payload = _valid_structured_payload()
    proposal, warnings = normalize_structured_proposal(payload)

    assert proposal.program_id == "test_program"
    assert proposal.hypothesis.anchor.type == "event"
    assert proposal.hypothesis.anchor.event_id == "TEST_EVENT"
    assert proposal.hypothesis.filters.states == ["NORMAL"]
    assert proposal.hypothesis.direction == "long"
    assert proposal.hypothesis.horizon_bars == 10
    assert not warnings


def test_normalize_structured_proposal_missing_required_fields() -> None:
    payload = _valid_structured_payload()
    del payload["program_id"]
    with pytest.raises(ValueError, match="program_id is required"):
        normalize_structured_proposal(payload)


def test_normalize_anchor_validation() -> None:
    # Event requires event_id
    payload = _valid_structured_payload()
    payload["hypothesis"]["anchor"] = {"type": "event"}
    with pytest.raises(ValueError, match="requires event_id"):
        normalize_structured_proposal(payload)

    # Transition requires from_state and to_state
    payload = _valid_structured_payload()
    payload["hypothesis"]["anchor"] = {"type": "transition", "from_state": "A"}
    with pytest.raises(ValueError, match="requires both from_state and to_state"):
        normalize_structured_proposal(payload)

    # Sequence requires at least two events
    payload = _valid_structured_payload()
    payload["hypothesis"]["anchor"] = {"type": "sequence", "events": ["E1"]}
    with pytest.raises(ValueError, match="requires at least two event ids"):
        normalize_structured_proposal(payload)

    # Feature crossing requires feature, operator, and threshold
    payload = _valid_structured_payload()
    payload["hypothesis"]["anchor"] = {
        "type": "feature_crossing",
        "feature": "F1",
        "operator": "crosses_above",
        "threshold": 0,
    }
    proposal, _ = normalize_structured_proposal(payload)
    assert proposal.hypothesis.anchor.type == "feature_crossing"


def test_deprecated_state_anchor_emits_warning() -> None:
    payload = _valid_structured_payload()
    payload["hypothesis"]["anchor"] = {"type": "state", "state_id": "S1"}
    proposal, warnings = normalize_structured_proposal(payload)
    assert any(w.code == DEPRECATED_STATE_ANCHOR for w in warnings)


def test_sampling_policy_validation() -> None:
    payload = _valid_structured_payload()
    payload["hypothesis"]["sampling_policy"]["mode"] = "invalid"
    with pytest.raises(ValueError, match="sampling_policy.mode must be one of"):
        normalize_structured_proposal(payload)

    payload = _valid_structured_payload()
    payload["hypothesis"]["sampling_policy"]["entry_lag_bars"] = 0
    with pytest.raises(ValueError, match="must be >= 1"):
        normalize_structured_proposal(payload)


def test_filter_normalization_uppercases_ids() -> None:
    payload = _valid_structured_payload()
    payload["hypothesis"]["filters"]["states"] = ["low_vol", "normal"]
    payload["hypothesis"]["filters"]["regimes"] = ["bull"]
    proposal, _ = normalize_structured_proposal(payload)
    assert proposal.hypothesis.filters.states == ["LOW_VOL", "NORMAL"]
    assert proposal.hypothesis.filters.regimes == ["BULL"]
