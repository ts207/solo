from __future__ import annotations

import pytest

from project.events.policy import DEPLOYABLE_CORE_EVENT_TYPES
from project.tests.events.fixtures.deployable_core_known_episode_replay import (
    BASELINE_PATH,
    build_known_episode_replay_baseline,
    compare_known_episode_replay_baseline,
    known_episode_fixtures,
    load_known_episode_replay_baseline,
)


@pytest.mark.slow
def test_known_episode_replay_baseline_covers_expected_episodes_and_core_detectors() -> None:
    baseline = load_known_episode_replay_baseline()

    assert BASELINE_PATH.exists()
    assert {episode["episode_id"] for episode in baseline["episodes"]} == {
        fixture.episode_id for fixture in known_episode_fixtures()
    }
    for episode in baseline["episodes"]:
        assert {item["event_name"] for item in episode["detector_results"]} == DEPLOYABLE_CORE_EVENT_TYPES


@pytest.mark.slow
def test_known_episode_replay_expected_present_and_absent_sets_are_enforced() -> None:
    baseline = load_known_episode_replay_baseline()

    for episode in baseline["episodes"]:
        by_event = {item["event_name"]: item for item in episode["detector_results"]}
        for event_name in episode["expected_present"]:
            assert by_event[event_name]["event_count"] > 0, (episode["episode_id"], event_name)
        for event_name in episode["expected_absent"]:
            assert by_event[event_name]["event_count"] == 0, (episode["episode_id"], event_name)


@pytest.mark.slow
def test_known_episode_replay_baseline_matches_current_detector_outputs() -> None:
    baseline = load_known_episode_replay_baseline()
    current = build_known_episode_replay_baseline()

    assert compare_known_episode_replay_baseline(baseline=baseline, current=current) == []


@pytest.mark.slow
def test_known_episode_replay_baseline_reports_detector_drift() -> None:
    baseline = load_known_episode_replay_baseline()
    drifted = build_known_episode_replay_baseline()
    drifted["episodes"][0]["detector_results"][0]["event_count"] += 1

    failures = compare_known_episode_replay_baseline(baseline=baseline, current=drifted)

    assert failures
    assert drifted["episodes"][0]["episode_id"] in failures[0]
