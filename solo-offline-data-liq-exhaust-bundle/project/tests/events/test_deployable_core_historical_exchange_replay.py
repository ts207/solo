from __future__ import annotations

import pytest
from project.events.policy import DEPLOYABLE_CORE_EVENT_TYPES
from project.tests.events.fixtures.deployable_core_historical_exchange_replay import (
    BASELINE_PATH,
    SLICE_PATH,
    build_historical_exchange_replay_baseline,
    compare_historical_exchange_replay_baseline,
    historical_exchange_slices,
    load_historical_exchange_replay_baseline,
    load_historical_exchange_slice,
)


@pytest.mark.slow
def test_historical_exchange_fixture_is_pinned_and_detector_ready() -> None:
    assert SLICE_PATH.exists()
    slice_spec = historical_exchange_slices()[0]
    frame = load_historical_exchange_slice(slice_spec)

    assert len(frame) == 733
    assert frame["timestamp"].iloc[0].isoformat() == "2024-01-01T00:00:00+00:00"
    assert frame["timestamp"].iloc[-1].isoformat() == "2024-01-03T13:00:00+00:00"
    assert {
        "depth_usd",
        "spread_bps",
        "liquidation_notional",
        "oi_delta_1h",
        "rv_96",
    }.issubset(frame.columns)
    assert {"close_perp", "close_spot"}.issubset(frame.columns)


@pytest.mark.slow
def test_historical_exchange_replay_baseline_covers_runtime_core() -> None:
    baseline = load_historical_exchange_replay_baseline()
    assert BASELINE_PATH.exists()
    assert {item["slice_id"] for item in baseline["slices"]} == {
        slice_spec.slice_id for slice_spec in historical_exchange_slices()
    }
    for slice_payload in baseline["slices"]:
        assert (
            {item["event_name"] for item in slice_payload["detector_results"]}
            == DEPLOYABLE_CORE_EVENT_TYPES
        )


@pytest.mark.slow
def test_historical_exchange_replay_expected_present_and_absent_sets_are_enforced() -> None:
    baseline = load_historical_exchange_replay_baseline()

    for slice_payload in baseline["slices"]:
        by_event = {item["event_name"]: item for item in slice_payload["detector_results"]}
        for event_name in slice_payload["expected_present"]:
            assert by_event[event_name]["event_count"] > 0, (slice_payload["slice_id"], event_name)
        for event_name in slice_payload["expected_absent"]:
            assert by_event[event_name]["event_count"] == 0, (slice_payload["slice_id"], event_name)


@pytest.mark.slow
def test_historical_exchange_replay_baseline_matches_current_detector_outputs() -> None:
    baseline = load_historical_exchange_replay_baseline()
    current = build_historical_exchange_replay_baseline()

    assert compare_historical_exchange_replay_baseline(baseline=baseline, current=current) == []


@pytest.mark.slow
def test_historical_exchange_replay_baseline_reports_material_drift() -> None:
    baseline = load_historical_exchange_replay_baseline()
    drifted = build_historical_exchange_replay_baseline()
    drifted["slices"][0]["detector_results"][0]["event_count"] += 1

    failures = compare_historical_exchange_replay_baseline(baseline=baseline, current=drifted)

    assert failures
    assert drifted["slices"][0]["slice_id"] in failures[0]
