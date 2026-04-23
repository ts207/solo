from __future__ import annotations

import pytest
from project.events.policy import DEPLOYABLE_CORE_EVENT_TYPES
from project.tests.events.fixtures.deployable_core_replay_baseline import (
    BASELINE_PATH,
    build_deployable_core_replay_baseline,
    compare_deployable_core_replay_baseline,
    load_deployable_core_replay_baseline,
)


@pytest.mark.slow
def test_deployable_core_replay_baseline_covers_runtime_core() -> None:
    baseline = load_deployable_core_replay_baseline()
    assert {case["event_name"] for case in baseline["cases"]} == DEPLOYABLE_CORE_EVENT_TYPES
    assert all(case["event_count"] > 0 for case in baseline["cases"])
    assert BASELINE_PATH.exists()


@pytest.mark.slow
def test_deployable_core_replay_baseline_matches_current_detector_outputs() -> None:
    baseline = load_deployable_core_replay_baseline()
    current = build_deployable_core_replay_baseline()

    assert compare_deployable_core_replay_baseline(baseline=baseline, current=current) == []


@pytest.mark.slow
def test_deployable_core_replay_baseline_reports_detector_drift() -> None:
    baseline = load_deployable_core_replay_baseline()
    drifted = build_deployable_core_replay_baseline()
    drifted["cases"][0]["event_count"] += 1

    failures = compare_deployable_core_replay_baseline(baseline=baseline, current=drifted)

    assert failures
    assert drifted["cases"][0]["event_name"] in failures[0]
