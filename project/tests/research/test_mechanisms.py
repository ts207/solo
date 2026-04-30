from __future__ import annotations

from project.research.mechanisms import (
    CandidateHypothesis,
    load_mechanism,
    load_mechanism_registry,
    validate_candidate_against_mechanism,
    validate_mechanism_spec,
)


def _candidate_payload(**overrides):
    payload = {
        "hypothesis": {
            "anchor": {"type": "event", "event_id": "PRICE_DOWN_OI_DOWN"},
            "filters": {"contexts": {"vol_regime": ["high"]}},
            "sampling_policy": {"mode": "episodic", "entry_lag_bars": 1},
            "template": {"id": "mean_reversion"},
            "direction": "long",
            "horizon_bars": 24,
        },
        "required_falsification": [
            "governed_reproduction",
            "search_burden",
            "candidate_traces",
            "year_pnl_split",
            "event_only_control",
            "context_only_control",
            "opposite_direction_control",
            "entry_lag_sensitivity",
            "cost_stress",
            "forward_confirmation",
        ],
        "forbidden_rescue_actions": [
            "drop_bad_years_after_result",
            "change_horizon_after_failure",
            "change_context_after_failure",
            "loosen_gates",
            "add_symbols_after_failure",
            "promote_without_specificity",
            "promote_without_forward_confirmation",
        ],
    }
    payload.update(overrides)
    return payload


def test_registry_loads_forced_flow_mechanism():
    registry = load_mechanism_registry()

    entry = registry.resolve("forced_flow_reversal")

    assert registry.version == "mechanism_registry_v1"
    assert entry.status == "active"
    assert entry.path.name == "forced_flow_reversal.yaml"


def test_registry_loads_active_funding_squeeze_mechanism():
    registry = load_mechanism_registry()

    entry = registry.resolve("funding_squeeze")

    assert entry.status == "active"
    assert entry.path.name == "funding_squeeze.yaml"


def test_forced_flow_mechanism_spec_validates_cleanly():
    mechanism = load_mechanism("forced_flow_reversal")

    issues = validate_mechanism_spec(mechanism)

    assert [issue for issue in issues if issue.status == "fail"] == []
    assert "PRICE_DOWN_OI_DOWN" in mechanism.candidate_events
    assert "forward_confirmation" in mechanism.required_falsification


def test_funding_squeeze_mechanism_spec_validates_cleanly():
    mechanism = load_mechanism("funding_squeeze")

    issues = validate_mechanism_spec(mechanism)

    assert [issue for issue in issues if issue.status == "fail"] == []
    assert mechanism.status == "active"
    assert "FUNDING_EXTREME" in mechanism.candidate_events
    assert "continuation" in mechanism.allowed_templates
    assert "short" in mechanism.allowed_directions
    assert "forward_confirmation" in mechanism.required_falsification


def test_funding_squeeze_candidate_fails_until_event_is_registry_valid():
    mechanism = load_mechanism("funding_squeeze")
    payload = _candidate_payload()
    payload["hypothesis"]["anchor"]["event_id"] = "FUNDING_EXTREME"
    payload["hypothesis"]["filters"]["contexts"] = {"carry_state": ["funding_neg"]}
    payload["hypothesis"]["template"]["id"] = "continuation"
    payload["hypothesis"]["direction"] = "short"
    payload["forbidden_rescue_actions"] = [
        "drop_bad_years_after_result",
        "change_horizon_after_failure",
        "switch_direction_after_failure",
        "loosen_gates",
        "add_symbols_after_failure",
        "promote_without_forward_confirmation",
    ]
    candidate = CandidateHypothesis.from_proposal_payload(payload)

    report = validate_candidate_against_mechanism(candidate, mechanism)

    assert report.status == "fail"
    assert report.classification == "mechanism_violation"
    check = {item.id: item for item in report.checks}["event_in_authoritative_registry"]
    assert check.status == "fail"
    assert check.detail == "FUNDING_EXTREME is not in the authoritative registry"


def test_candidate_passes_when_tuple_and_controls_match_mechanism():
    mechanism = load_mechanism("forced_flow_reversal")
    candidate = CandidateHypothesis.from_proposal_payload(_candidate_payload())

    report = validate_candidate_against_mechanism(candidate, mechanism, proposal_path="proposal.yaml")

    assert report.status == "pass"
    assert report.classification == "mechanism_backed"
    assert {check.id: check.status for check in report.checks}["event_allowed"] == "pass"
    assert {check.id: check.status for check in report.checks}[
        "event_in_authoritative_registry"
    ] == "pass"
    assert {check.id: check.status for check in report.checks}[
        "template_in_template_registry"
    ] == "pass"


def test_candidate_fails_for_forbidden_context():
    mechanism = load_mechanism("forced_flow_reversal")
    payload = _candidate_payload()
    payload["hypothesis"]["filters"]["contexts"] = {"vol_regime": ["low"]}
    candidate = CandidateHypothesis.from_proposal_payload(payload)

    report = validate_candidate_against_mechanism(candidate, mechanism)

    assert report.status == "fail"
    assert report.classification == "mechanism_violation"
    assert {check.id: check.status for check in report.checks}["context_forbidden"] == "fail"


def test_candidate_canonicalizes_display_context_tokens():
    mechanism = load_mechanism("forced_flow_reversal")
    payload = _candidate_payload()
    payload["hypothesis"]["filters"]["contexts"] = {"VOL_REGIME": ["HIGH"]}
    candidate = CandidateHypothesis.from_proposal_payload(payload)

    report = validate_candidate_against_mechanism(candidate, mechanism)

    checks = [item for item in report.checks if item.id == "context_canonicalized"]
    assert checks
    assert checks[0].detail == "VOL_REGIME=HIGH canonicalized to vol_regime=high"
    assert {check.id: check.status for check in report.checks}["context_value_allowed"] == "pass"


def test_candidate_fails_for_invalid_context_value():
    mechanism = load_mechanism("forced_flow_reversal")
    payload = _candidate_payload()
    payload["hypothesis"]["filters"]["contexts"] = {"carry_state": ["funding_negative"]}
    candidate = CandidateHypothesis.from_proposal_payload(payload)

    report = validate_candidate_against_mechanism(candidate, mechanism)

    check = {item.id: item for item in report.checks}["context_value_allowed"]
    assert report.status == "fail"
    assert check.status == "fail"
    assert "carry_state=funding_negative" in check.detail


def test_candidate_fails_when_required_falsification_missing():
    mechanism = load_mechanism("forced_flow_reversal")
    payload = _candidate_payload(required_falsification=["governed_reproduction"])
    candidate = CandidateHypothesis.from_proposal_payload(payload)

    report = validate_candidate_against_mechanism(candidate, mechanism)

    assert report.status == "fail"
    check = {item.id: item for item in report.checks}["required_falsification_declared"]
    assert check.status == "fail"
    assert "forward_confirmation" in check.detail
