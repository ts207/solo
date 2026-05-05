from project.live.decision_reasons import (
    EXECUTION_CONTEXT_DEGRADED,
    build_decision_reason,
    classify_skip_reason,
)


def test_decision_reason_classifies_execution_domain() -> None:
    reason = build_decision_reason(EXECUTION_CONTEXT_DEGRADED, details={"spread_bps": 12})
    assert reason.domain == "execution"
    assert reason.as_dict()["details"]["spread_bps"] == 12
    assert classify_skip_reason(EXECUTION_CONTEXT_DEGRADED) == "execution"
