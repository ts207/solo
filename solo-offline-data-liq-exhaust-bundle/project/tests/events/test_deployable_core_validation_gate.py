from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from project.events.detector_contract import DetectorContractError
from project.events.event_output_schema import validate_event_output_frame
from project.events.policy import DEPLOYABLE_CORE_EVENT_TYPES
from project.tests.events.test_detector_output_schema import DEPLOYABLE_CORE_CASES


SIGNATURE_COLUMNS = (
    "event_name",
    "event_version",
    "symbol",
    "timeframe",
    "ts_start",
    "ts_end",
    "phase",
    "family",
    "subtype",
    "evidence_mode",
    "severity",
    "confidence",
    "trigger_value",
    "threshold_snapshot",
    "required_context_present",
    "data_quality_flag",
    "merge_key",
    "cooldown_until",
    "source_features",
    "detector_metadata",
)


def _stable_payload(value):
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return value


def _event_signature(events: pd.DataFrame) -> pd.DataFrame:
    out = events.loc[:, [column for column in SIGNATURE_COLUMNS if column in events.columns]].copy()
    for column in ("ts_start", "ts_end", "cooldown_until"):
        if column in out.columns:
            out[column] = pd.to_datetime(out[column], utc=True, errors="coerce").astype(str)
    for column in ("threshold_snapshot", "source_features", "detector_metadata"):
        if column in out.columns:
            out[column] = out[column].map(_stable_payload)
    return out.reset_index(drop=True)


def _first_required_feature_column(detector, frame: pd.DataFrame) -> str:
    for column in getattr(detector, "required_columns", ()):
        if column != "timestamp" and column in frame.columns:
            return str(column)
    raise AssertionError(f"{detector.event_name} has no non-timestamp required fixture column")


def _append_inert_future_bar(frame: pd.DataFrame) -> pd.DataFrame:
    last = frame.iloc[[-1]].copy()
    neutral = frame.iloc[0]
    for column in frame.columns:
        if column != "timestamp":
            last[column] = neutral[column]
    last["timestamp"] = pd.to_datetime(last["timestamp"], utc=True) + pd.Timedelta(minutes=5)
    return pd.concat([frame, last], ignore_index=True)


def _assert_same_events(left: pd.DataFrame, right: pd.DataFrame) -> None:
    assert_frame_equal(_event_signature(left), _event_signature(right), check_dtype=False)


def test_deployable_core_validation_cases_cover_runtime_core() -> None:
    assert {detector.event_name for detector, _, _ in DEPLOYABLE_CORE_CASES} == DEPLOYABLE_CORE_EVENT_TYPES


@pytest.mark.parametrize(("detector", "frame", "params"), DEPLOYABLE_CORE_CASES)
def test_deployable_core_replay_stability(detector, frame: pd.DataFrame, params: dict) -> None:
    first = detector.detect_events(frame.copy(), dict(params))
    second = detector.detect_events(frame.copy(), dict(params))

    validate_event_output_frame(first, require_rows=True)
    validate_event_output_frame(second, require_rows=True)
    _assert_same_events(first, second)


@pytest.mark.parametrize(("detector", "frame", "params"), DEPLOYABLE_CORE_CASES)
def test_deployable_core_ignores_irrelevant_feature_noise(detector, frame: pd.DataFrame, params: dict) -> None:
    baseline = detector.detect_events(frame.copy(), dict(params))
    noisy = frame.copy()
    noisy["irrelevant_feature_noise"] = np.linspace(0.0, 1.0, len(noisy), dtype=float)

    perturbed = detector.detect_events(noisy, dict(params))

    validate_event_output_frame(perturbed, require_rows=True)
    _assert_same_events(baseline, perturbed)


@pytest.mark.parametrize(("detector", "frame", "params"), DEPLOYABLE_CORE_CASES)
def test_deployable_core_appended_future_bar_does_not_rewrite_prior_events(
    detector,
    frame: pd.DataFrame,
    params: dict,
) -> None:
    baseline = detector.detect_events(frame.copy(), dict(params))
    extended = detector.detect_events(_append_inert_future_bar(frame), dict(params))
    original_end = pd.to_datetime(frame["timestamp"].iloc[-1], utc=True)
    prior_extended = extended[pd.to_datetime(extended["ts_start"], utc=True) <= original_end].copy()

    validate_event_output_frame(extended, require_rows=True)
    _assert_same_events(baseline, prior_extended)


@pytest.mark.parametrize(("detector", "frame", "params"), DEPLOYABLE_CORE_CASES)
def test_deployable_core_sparse_inert_nan_does_not_crash(detector, frame: pd.DataFrame, params: dict) -> None:
    sparse = frame.copy()
    sparse.loc[0, _first_required_feature_column(detector, sparse)] = np.nan

    events = detector.detect_events(sparse, dict(params))

    validate_event_output_frame(events, require_rows=True)
    assert set(events["data_quality_flag"]) <= {"ok", "degraded", "invalid"}


@pytest.mark.parametrize(("detector", "frame", "params"), DEPLOYABLE_CORE_CASES)
def test_deployable_core_missing_required_column_fails_closed(
    detector,
    frame: pd.DataFrame,
    params: dict,
) -> None:
    missing_column = _first_required_feature_column(detector, frame)

    with pytest.raises(DetectorContractError, match=missing_column):
        detector.detect_events(frame.drop(columns=[missing_column]), dict(params))
