from __future__ import annotations

from project.scripts.audit_detector_precision_recall import _select_event_types


def test_select_event_types_skips_live_only_synthetic_by_default():
    selected, skipped = _select_event_types(
        ["ABSORPTION_PROXY", "DEPTH_STRESS_PROXY", "VOL_SPIKE"],
        requested_event_type=None,
        include_live_only_synthetic=False,
    )
    assert selected == ["VOL_SPIKE"]
    assert skipped == ["ABSORPTION_PROXY", "DEPTH_STRESS_PROXY"]


def test_select_event_types_can_include_live_only_synthetic():
    selected, skipped = _select_event_types(
        ["ABSORPTION_PROXY", "DEPTH_STRESS_PROXY", "VOL_SPIKE"],
        requested_event_type=None,
        include_live_only_synthetic=True,
    )
    assert selected == ["ABSORPTION_PROXY", "DEPTH_STRESS_PROXY", "VOL_SPIKE"]
    assert skipped == []


def test_select_event_types_explicit_request_bypasses_default_skip():
    selected, skipped = _select_event_types(
        ["ABSORPTION_PROXY", "DEPTH_STRESS_PROXY", "VOL_SPIKE"],
        requested_event_type="ABSORPTION_PROXY",
        include_live_only_synthetic=False,
    )
    assert selected == ["ABSORPTION_PROXY"]
    assert skipped == []
