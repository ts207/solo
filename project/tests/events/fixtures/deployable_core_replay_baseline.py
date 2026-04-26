from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from project.events.event_output_schema import validate_event_output_frame
from project.tests.events.test_detector_output_schema import DEPLOYABLE_CORE_CASES

BASELINE_PATH = Path(__file__).with_name("deployable_core_replay_baseline.json")

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


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            value = value.tz_localize("UTC")
        return value.isoformat()
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        if pd.isna(value):
            return None
        return round(float(value), 12)
    if pd.isna(value):
        return None
    return value


def _timestamp(value: Any) -> str | None:
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.isoformat()


def _numeric_summary(series: pd.Series) -> dict[str, float | None]:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return {"min": None, "max": None, "mean": None}
    return {
        "min": round(float(numeric.min()), 12),
        "max": round(float(numeric.max()), 12),
        "mean": round(float(numeric.mean()), 12),
    }


def event_signature_records(events: pd.DataFrame) -> list[dict[str, Any]]:
    available_columns = [column for column in SIGNATURE_COLUMNS if column in events.columns]
    signature = events.loc[:, available_columns].copy()
    for column in ("ts_start", "ts_end", "cooldown_until"):
        if column in signature.columns:
            signature[column] = signature[column].map(_timestamp)
    records = signature.reset_index(drop=True).to_dict(orient="records")
    return [{str(key): _jsonable(value) for key, value in row.items()} for row in records]


def signature_digest(records: list[dict[str, Any]]) -> str:
    payload = json.dumps(records, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _value_counts(events: pd.DataFrame, column: str) -> dict[str, int]:
    if column not in events.columns:
        return {}
    counts = events[column].fillna("<null>").astype(str).value_counts().sort_index()
    return {str(key): int(value) for key, value in counts.items()}


def summarize_detector_events(
    *,
    detector: Any,
    events: pd.DataFrame,
    params: dict[str, Any],
    include_events: bool = False,
) -> dict[str, Any]:
    records = event_signature_records(events)
    starts = pd.to_datetime(events["ts_start"], utc=True, errors="coerce")
    ends = pd.to_datetime(events["ts_end"], utc=True, errors="coerce")
    summary: dict[str, Any] = {
        "event_name": str(detector.event_name),
        "detector_class": detector.__class__.__name__,
        "event_version": str(getattr(detector, "event_version", "")),
        "symbol": str(params.get("symbol", "")),
        "timeframe": str(params.get("timeframe", "")),
        "event_count": len(events),
        "first_ts_start": _timestamp(starts.min()),
        "last_ts_start": _timestamp(starts.max()),
        "first_ts_end": _timestamp(ends.min()),
        "last_ts_end": _timestamp(ends.max()),
        "phase_counts": _value_counts(events, "phase"),
        "data_quality_counts": _value_counts(events, "data_quality_flag"),
        "severity": _numeric_summary(events["severity"]),
        "confidence": _numeric_summary(events["confidence"]),
        "trigger_value": _numeric_summary(events["trigger_value"]),
        "signature_digest": signature_digest(records),
    }
    if include_events:
        summary["events"] = records
    return summary


def build_deployable_core_replay_baseline() -> dict[str, Any]:
    cases: list[dict[str, Any]] = []
    for detector, frame, params in DEPLOYABLE_CORE_CASES:
        events = detector.detect_events(frame.copy(deep=True), dict(params))
        validate_event_output_frame(events, require_rows=True)
        cases.append(
            summarize_detector_events(
                detector=detector,
                events=events,
                params=params,
                include_events=True,
            )
        )
    return {
        "baseline_schema_version": 1,
        "baseline_type": "deployable_core_deterministic_replay",
        "fixture_set": "project.tests.events.test_detector_output_schema.DEPLOYABLE_CORE_CASES",
        "cases": cases,
    }


def load_deployable_core_replay_baseline(path: Path = BASELINE_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_deployable_core_replay_baseline(
    path: Path = BASELINE_PATH,
    *,
    baseline: dict[str, Any] | None = None,
) -> None:
    payload = baseline if baseline is not None else build_deployable_core_replay_baseline()
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def compare_deployable_core_replay_baseline(
    *,
    baseline: dict[str, Any],
    current: dict[str, Any],
) -> list[str]:
    failures: list[str] = []
    for key in ("baseline_schema_version", "baseline_type", "fixture_set"):
        if baseline.get(key) != current.get(key):
            failures.append(f"{key}: baseline={baseline.get(key)!r} current={current.get(key)!r}")

    baseline_cases = {str(case.get("event_name")): case for case in baseline.get("cases", [])}
    current_cases = {str(case.get("event_name")): case for case in current.get("cases", [])}
    missing = sorted(set(baseline_cases) - set(current_cases))
    extra = sorted(set(current_cases) - set(baseline_cases))
    if missing:
        failures.append(f"missing current cases: {missing}")
    if extra:
        failures.append(f"unexpected current cases: {extra}")

    for event_name in sorted(set(baseline_cases) & set(current_cases)):
        base_case = baseline_cases[event_name]
        current_case = current_cases[event_name]
        differing_fields = [
            key
            for key in sorted(set(base_case) | set(current_case))
            if base_case.get(key) != current_case.get(key)
        ]
        if differing_fields:
            failures.append(f"{event_name}: replay baseline drift in {differing_fields}")
    return failures
