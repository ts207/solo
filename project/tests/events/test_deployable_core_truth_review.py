from __future__ import annotations

import pandas as pd

from project.events.policy import DEPLOYABLE_CORE_EVENT_TYPES
from project.tests.events.fixtures.deployable_core_known_episode_replay import (
    known_episode_fixtures,
)
from project.tests.events.fixtures.deployable_core_truth_review import (
    EventTruthWindow,
    review_deployable_core_episode_truth,
    review_detector_events,
    truth_windows_by_episode_event,
)


def test_deployable_core_truth_windows_cover_expected_present_sets() -> None:
    review = review_deployable_core_episode_truth()
    covered = {(episode["episode_id"], item["event_name"]) for episode in review["episodes"] for item in episode["detector_reviews"] if item["windows"]}
    truth_keys = set(truth_windows_by_episode_event())
    expected_present = {
        (fixture.episode_id, event_name)
        for fixture in known_episode_fixtures()
        for event_name in fixture.expected_present
    }

    assert truth_keys == covered & truth_keys
    assert expected_present <= truth_keys
    assert {event_name for _, event_name in truth_keys} <= DEPLOYABLE_CORE_EVENT_TYPES


def test_deployable_core_truth_review_passes_current_known_episodes() -> None:
    review = review_deployable_core_episode_truth()

    assert review["status"] == "pass"
    assert review["failure_count"] == 0


def test_truth_review_reports_false_positive_for_expected_absent_detector() -> None:
    events = pd.DataFrame(
        [
            {
                "ts_start": "2024-02-12T02:15:00+00:00",
                "ts_end": "2024-02-12T02:15:00+00:00",
                "confidence": 0.9,
                "severity": 1.0,
            }
        ]
    )

    review = review_detector_events(
        episode_id="basis_funding_dislocation_2024_02_synthetic",
        event_name="VOL_SPIKE",
        events=events,
        windows=[],
        expected_absent=True,
    )

    assert review["status"] == "fail"
    assert review["failures"][0]["kind"] == "false_positive"


def test_truth_review_reports_false_negative_for_missing_required_window() -> None:
    window = EventTruthWindow(
        episode_id="episode",
        event_name="BASIS_DISLOC",
        window_start="2024-01-01T00:00:00+00:00",
        window_end="2024-01-01T00:10:00+00:00",
        min_events=1,
        max_events=1,
        required=True,
    )

    review = review_detector_events(
        episode_id="episode",
        event_name="BASIS_DISLOC",
        events=pd.DataFrame(columns=["ts_start", "ts_end", "confidence", "severity"]),
        windows=[window],
        expected_absent=False,
    )

    assert review["status"] == "fail"
    assert review["failures"][0]["kind"] == "false_negative"


def test_truth_review_reports_quality_and_explosion_violations() -> None:
    events = pd.DataFrame(
        [
            {
                "ts_start": "2024-01-01T00:00:00+00:00",
                "ts_end": "2024-01-01T00:00:00+00:00",
                "confidence": 0.2,
                "severity": 0.1,
            },
            {
                "ts_start": "2024-01-01T00:05:00+00:00",
                "ts_end": "2024-01-01T00:05:00+00:00",
                "confidence": 0.2,
                "severity": 0.1,
            },
        ]
    )
    window = EventTruthWindow(
        episode_id="episode",
        event_name="VOL_SPIKE",
        window_start="2024-01-01T00:00:00+00:00",
        window_end="2024-01-01T00:10:00+00:00",
        max_events=1,
        min_confidence=0.8,
        min_severity=0.75,
    )

    review = review_detector_events(
        episode_id="episode",
        event_name="VOL_SPIKE",
        events=events,
        windows=[window],
        expected_absent=False,
    )

    kinds = {failure["kind"] for failure in review["failures"]}
    assert {"event_explosion", "low_confidence", "low_severity"} <= kinds
