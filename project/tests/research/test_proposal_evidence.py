from __future__ import annotations

import json

from project.research.proposal_evidence import (
    event_lift_is_passing,
    find_event_lift_evidence,
)


MATCH = {
    "mechanism_id": "funding_squeeze",
    "event_id": "FUNDING_EXTREME_ONSET",
    "regime_id": "vol_regime=high+carry_state=funding_neg",
    "symbol": "BTCUSDT",
    "direction": "long",
    "horizon_bars": 24,
}


def _write_report(tmp_path, run_id: str, **overrides):
    row = {
        "run_id": run_id,
        "scorecard_decision": "allow_event_lift",
        "audit_only": False,
        "promotion_eligible": True,
        "classification": "event_specific",
        "decision": "advance_to_mechanism_proposal",
        **MATCH,
    }
    row.update(overrides)
    path = tmp_path / "reports" / "event_lift" / run_id / "event_lift.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"rows": [row]}), encoding="utf-8")
    return path, row


def test_event_lift_is_passing_requires_scorecard_and_non_audit(tmp_path):
    _, passing = _write_report(tmp_path, "passing")
    _, scorecard_blocked = _write_report(
        tmp_path,
        "blocked",
        scorecard_decision="park",
    )
    _, audit = _write_report(
        tmp_path,
        "audit",
        audit_only=True,
        promotion_eligible=False,
        classification="audit_only",
        decision="audit_only",
    )

    assert event_lift_is_passing(passing) is True
    assert event_lift_is_passing(scorecard_blocked) is False
    assert event_lift_is_passing(audit) is False


def test_find_event_lift_evidence_discovers_latest_matching_pass(tmp_path):
    _write_report(
        tmp_path,
        "parked",
        decision="park",
        promotion_eligible=False,
        classification="insufficient_support",
    )
    expected_path, _ = _write_report(tmp_path, "passing")

    evidence = find_event_lift_evidence(
        data_root=tmp_path,
        **MATCH,
    )

    assert evidence is not None
    assert evidence.path == expected_path
    assert evidence.row["run_id"] == "passing"
