from project.operator.decision_engine import decide_next_action


def test_decision_engine_returns_modify_for_near_miss():
    result = decide_next_action(
        run_summary={
            "terminal_status": "completed",
            "verdict": "KEEP_RESEARCH",
            "promoted_count": 0,
            "candidate_count": 1,
            "top_candidate": {"metric_value": 1.8},
        },
        diagnostics={"diagnosis": "no_effect"},
    )
    assert result.action == "MODIFY"
    assert result.classification == "near_miss"


def test_decision_engine_returns_stop_for_weak_failure():
    result = decide_next_action(
        run_summary={
            "terminal_status": "completed",
            "verdict": "KEEP_RESEARCH",
            "promoted_count": 0,
            "candidate_count": 0,
            "top_candidate": {"metric_value": 0.2},
        },
        diagnostics={"diagnosis": "no_effect"},
    )
    assert result.action == "STOP"
    assert result.classification == "fail"


def test_decision_engine_warning_only_no_effect_stops_not_repairs():
    result = decide_next_action(
        run_summary={
            "terminal_status": "completed_with_contract_warnings",
            "mechanical_outcome": "warning_only",
            "verdict": "KEEP_RESEARCH",
            "promoted_count": 0,
            "candidate_count": 0,
            "top_candidate": {"metric_value": 0.0},
        },
        diagnostics={"diagnosis": "no_effect"},
    )
    assert result.action == "STOP"
    assert result.classification == "fail"
