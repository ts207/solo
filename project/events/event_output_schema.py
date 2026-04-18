from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import pandas as pd


_VALID_QUALITY_FLAGS = {"ok", "degraded", "invalid"}

REQUIRED_EVENT_OUTPUT_COLUMNS: tuple[str, ...] = (
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

EVENT_OUTPUT_COLUMNS: tuple[str, ...] = (
    *REQUIRED_EVENT_OUTPUT_COLUMNS,
    "canonical_family",
    "role",
    "detector_class",
)


@dataclass
class DetectedEvent:
    event_name: str
    event_version: str
    detector_class: str

    symbol: str
    timeframe: str
    ts_start: datetime
    ts_end: datetime

    canonical_family: str
    subtype: str
    phase: str
    evidence_mode: str
    role: str

    confidence: float | None
    severity: float | None
    trigger_value: float | None

    threshold_snapshot: dict[str, Any]
    source_features: dict[str, Any]
    detector_metadata: dict[str, Any]

    required_context_present: bool
    data_quality_flag: str
    merge_key: str | None
    cooldown_until: datetime | None

    def __post_init__(self) -> None:
        if self.confidence is not None:
            self.confidence = float(max(0.0, min(1.0, self.confidence)))
        if self.severity is not None:
            self.severity = float(max(0.0, min(1.0, self.severity)))
        if self.data_quality_flag not in _VALID_QUALITY_FLAGS:
            raise ValueError(f"invalid data_quality_flag {self.data_quality_flag!r}")
        if not isinstance(self.threshold_snapshot, dict):
            raise TypeError("threshold_snapshot must be a dict")
        if not isinstance(self.source_features, dict):
            raise TypeError("source_features must be a dict")
        if not isinstance(self.detector_metadata, dict):
            raise TypeError("detector_metadata must be a dict")

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for key in ("ts_start", "ts_end", "cooldown_until"):
            value = payload.get(key)
            if isinstance(value, datetime):
                payload[key] = value.isoformat()
        payload["family"] = payload["canonical_family"]
        return payload


def empty_event_output_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=EVENT_OUTPUT_COLUMNS)


def normalize_event_output_frame(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return empty_event_output_frame()
    out = frame.copy()
    if "family" not in out.columns and "canonical_family" in out.columns:
        out["family"] = out["canonical_family"]
    if "canonical_family" not in out.columns and "family" in out.columns:
        out["canonical_family"] = out["family"]
    optional_columns = [column for column in EVENT_OUTPUT_COLUMNS if column not in REQUIRED_EVENT_OUTPUT_COLUMNS]
    missing = [column for column in optional_columns if column not in out.columns]
    for column in missing:
        out[column] = None
    present_ordered = [column for column in EVENT_OUTPUT_COLUMNS if column in out.columns]
    ordered = present_ordered + [column for column in out.columns if column not in EVENT_OUTPUT_COLUMNS]
    return out.loc[:, ordered]


def validate_event_output_frame(frame: pd.DataFrame, *, require_rows: bool = False) -> None:
    if require_rows and frame.empty:
        raise ValueError("event output frame must contain at least one row")
    missing = [column for column in REQUIRED_EVENT_OUTPUT_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"event output frame missing required columns: {missing}")
    for column in ("threshold_snapshot", "source_features", "detector_metadata"):
        bad = frame[column].map(lambda value: not isinstance(value, dict))
        if bool(bad.any()):
            raise TypeError(f"{column} must contain dict values")
    invalid_quality = set(frame["data_quality_flag"].dropna().astype(str)) - _VALID_QUALITY_FLAGS
    if invalid_quality:
        raise ValueError(f"invalid data_quality_flag values: {sorted(invalid_quality)}")
