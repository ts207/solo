from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from project.events.detectors.registry import get_detector
from project.events.event_output_schema import validate_event_output_frame
from project.events.policy import DEPLOYABLE_CORE_EVENT_TYPES
from project.tests.events.fixtures.deployable_core_known_episode_replay import (
    KnownEpisodeFixture,
    detector_params_for_fixture,
    known_episode_fixtures,
)

REVIEW_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class EventTruthWindow:
    episode_id: str
    event_name: str
    window_start: str
    window_end: str
    min_events: int = 1
    max_events: int = 1
    min_confidence: float = 0.0
    min_severity: float = 0.0
    required: bool = True


TRUTH_WINDOWS: tuple[EventTruthWindow, ...] = (
    EventTruthWindow(
        episode_id="basis_funding_dislocation_2024_02_synthetic",
        event_name="BASIS_DISLOC",
        window_start="2024-02-12T02:10:00+00:00",
        window_end="2024-02-12T02:20:00+00:00",
        max_events=1,
        min_confidence=0.85,
        min_severity=1.0,
    ),
    EventTruthWindow(
        episode_id="basis_funding_dislocation_2024_02_synthetic",
        event_name="FND_DISLOC",
        window_start="2024-02-12T02:20:00+00:00",
        window_end="2024-02-12T02:30:00+00:00",
        max_events=1,
        min_confidence=0.85,
        min_severity=1.0,
    ),
    EventTruthWindow(
        episode_id="basis_funding_dislocation_2024_02_synthetic",
        event_name="SPOT_PERP_BASIS_SHOCK",
        window_start="2024-02-12T02:10:00+00:00",
        window_end="2024-02-12T02:20:00+00:00",
        max_events=1,
        min_confidence=0.95,
        min_severity=1.0,
    ),
    EventTruthWindow(
        episode_id="liquidity_liquidation_vol_cascade_2024_03_synthetic",
        event_name="LIQUIDITY_STRESS_DIRECT",
        window_start="2024-03-16T01:40:00+00:00",
        window_end="2024-03-16T02:35:00+00:00",
        min_events=8,
        max_events=12,
        min_confidence=0.75,
        min_severity=1.0,
    ),
    EventTruthWindow(
        episode_id="liquidity_liquidation_vol_cascade_2024_03_synthetic",
        event_name="LIQUIDITY_SHOCK",
        window_start="2024-03-16T01:40:00+00:00",
        window_end="2024-03-16T02:35:00+00:00",
        min_events=8,
        max_events=12,
        min_confidence=0.75,
        min_severity=1.0,
    ),
    EventTruthWindow(
        episode_id="liquidity_liquidation_vol_cascade_2024_03_synthetic",
        event_name="LIQUIDITY_VACUUM",
        window_start="2024-03-16T01:35:00+00:00",
        window_end="2024-03-16T01:45:00+00:00",
        max_events=1,
        min_confidence=0.75,
        min_severity=1.0,
    ),
    EventTruthWindow(
        episode_id="liquidity_liquidation_vol_cascade_2024_03_synthetic",
        event_name="LIQUIDATION_CASCADE",
        window_start="2024-03-16T01:45:00+00:00",
        window_end="2024-03-16T02:35:00+00:00",
        max_events=1,
        min_confidence=0.85,
        min_severity=1.0,
    ),
    EventTruthWindow(
        episode_id="liquidity_liquidation_vol_cascade_2024_03_synthetic",
        event_name="VOL_SPIKE",
        window_start="2024-03-16T01:10:00+00:00",
        window_end="2024-03-16T02:10:00+00:00",
        min_events=3,
        max_events=3,
        min_confidence=0.80,
        min_severity=0.75,
    ),
    EventTruthWindow(
        episode_id="liquidity_liquidation_vol_cascade_2024_03_synthetic",
        event_name="VOL_SHOCK",
        window_start="2024-03-16T01:10:00+00:00",
        window_end="2024-03-16T02:35:00+00:00",
        min_events=12,
        max_events=18,
        min_confidence=0.80,
        min_severity=0.45,
    ),
)


def truth_windows_by_episode_event() -> dict[tuple[str, str], list[EventTruthWindow]]:
    out: dict[tuple[str, str], list[EventTruthWindow]] = {}
    for window in TRUTH_WINDOWS:
        out.setdefault((window.episode_id, window.event_name), []).append(window)
    return out


def _iso_ts(value: Any) -> str | None:
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.isoformat()


def _window_mask(events: pd.DataFrame, window: EventTruthWindow) -> pd.Series:
    starts = pd.to_datetime(events["ts_start"], utc=True, errors="coerce")
    start = pd.Timestamp(window.window_start)
    end = pd.Timestamp(window.window_end)
    return starts.ge(start) & starts.le(end)


def _event_summary(events: pd.DataFrame) -> dict[str, Any]:
    if events.empty:
        return {
            "event_count": 0,
            "first_ts_start": None,
            "last_ts_start": None,
            "min_confidence": None,
            "min_severity": None,
        }
    starts = pd.to_datetime(events["ts_start"], utc=True, errors="coerce")
    confidence = pd.to_numeric(events["confidence"], errors="coerce")
    severity = pd.to_numeric(events["severity"], errors="coerce")
    return {
        "event_count": len(events),
        "first_ts_start": _iso_ts(starts.min()),
        "last_ts_start": _iso_ts(starts.max()),
        "min_confidence": None if confidence.dropna().empty else round(float(confidence.min()), 12),
        "min_severity": None if severity.dropna().empty else round(float(severity.min()), 12),
    }


def _run_detector(fixture: KnownEpisodeFixture, event_name: str) -> pd.DataFrame:
    detector = get_detector(event_name)
    if detector is None:
        raise AssertionError(f"Missing deployable-core detector: {event_name}")
    events = detector.detect_events(
        fixture.frame.copy(deep=True),
        detector_params_for_fixture(fixture, event_name),
    )
    validate_event_output_frame(events, require_rows=False)
    return events


def review_detector_events(
    *,
    episode_id: str,
    event_name: str,
    events: pd.DataFrame,
    windows: list[EventTruthWindow],
    expected_absent: bool,
) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    matched_indices: set[int] = set()
    window_reviews: list[dict[str, Any]] = []

    for window in windows:
        matched = events[_window_mask(events, window)].copy()
        matched_indices.update(int(idx) for idx in matched.index)
        summary = _event_summary(matched)
        window_review = {
            "window_start": window.window_start,
            "window_end": window.window_end,
            "required": bool(window.required),
            "min_events": int(window.min_events),
            "max_events": int(window.max_events),
            "min_confidence": float(window.min_confidence),
            "min_severity": float(window.min_severity),
            **summary,
        }
        window_reviews.append(window_review)

        if window.required and summary["event_count"] < window.min_events:
            failures.append(
                {
                    "kind": "false_negative",
                    "episode_id": episode_id,
                    "event_name": event_name,
                    "message": f"{event_name} emitted {summary['event_count']} events; expected at least {window.min_events}",
                }
            )
        if summary["event_count"] > window.max_events:
            failures.append(
                {
                    "kind": "event_explosion",
                    "episode_id": episode_id,
                    "event_name": event_name,
                    "message": f"{event_name} emitted {summary['event_count']} events; max allowed is {window.max_events}",
                }
            )
        if summary["event_count"] > 0:
            min_confidence = summary["min_confidence"]
            min_severity = summary["min_severity"]
            if min_confidence is None or float(min_confidence) < window.min_confidence:
                failures.append(
                    {
                        "kind": "low_confidence",
                        "episode_id": episode_id,
                        "event_name": event_name,
                        "message": f"{event_name} minimum confidence {min_confidence} below {window.min_confidence}",
                    }
                )
            if min_severity is None or float(min_severity) < window.min_severity:
                failures.append(
                    {
                        "kind": "low_severity",
                        "episode_id": episode_id,
                        "event_name": event_name,
                        "message": f"{event_name} minimum severity {min_severity} below {window.min_severity}",
                    }
                )

    unmatched = events.drop(index=sorted(matched_indices), errors="ignore")
    if expected_absent and len(events) > 0:
        failures.append(
            {
                "kind": "false_positive",
                "episode_id": episode_id,
                "event_name": event_name,
                "message": f"{event_name} emitted {len(events)} events in expected-absent episode",
            }
        )
    elif len(unmatched) > 0:
        failures.append(
            {
                "kind": "false_positive",
                "episode_id": episode_id,
                "event_name": event_name,
                "message": f"{event_name} emitted {len(unmatched)} events outside truth windows",
            }
        )

    return {
        "episode_id": episode_id,
        "event_name": event_name,
        "expected_absent": bool(expected_absent),
        "observed": _event_summary(events),
        "windows": window_reviews,
        "false_positive_count": int(len(events) if expected_absent else len(unmatched)),
        "failure_count": len(failures),
        "status": "pass" if not failures else "fail",
        "failures": failures,
    }


def review_deployable_core_episode_truth() -> dict[str, Any]:
    windows_by_key = truth_windows_by_episode_event()
    episode_reviews: list[dict[str, Any]] = []
    all_failures: list[dict[str, Any]] = []

    for fixture in known_episode_fixtures():
        detector_reviews: list[dict[str, Any]] = []
        expected_absent = set(fixture.expected_absent)
        for event_name in sorted(DEPLOYABLE_CORE_EVENT_TYPES):
            events = _run_detector(fixture, event_name)
            review = review_detector_events(
                episode_id=fixture.episode_id,
                event_name=event_name,
                events=events,
                windows=windows_by_key.get((fixture.episode_id, event_name), []),
                expected_absent=event_name in expected_absent,
            )
            detector_reviews.append(review)
            all_failures.extend(review["failures"])

        episode_reviews.append(
            {
                "episode_id": fixture.episode_id,
                "label": fixture.label,
                "detector_reviews": detector_reviews,
                "failure_count": int(sum(item["failure_count"] for item in detector_reviews)),
            }
        )

    return {
        "review_schema_version": REVIEW_SCHEMA_VERSION,
        "review_type": "deployable_core_truth_review",
        "episode_count": len(episode_reviews),
        "detector_count": len(DEPLOYABLE_CORE_EVENT_TYPES),
        "failure_count": len(all_failures),
        "status": "pass" if not all_failures else "fail",
        "episodes": episode_reviews,
        "failures": all_failures,
    }


def write_truth_review(path: Path, *, review: dict[str, Any] | None = None) -> None:
    payload = review if review is not None else review_deployable_core_episode_truth()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
