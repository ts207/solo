from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

import pandas as pd

_VALID_QUALITY_FLAGS = {"ok", "degraded", "invalid"}
_VALID_SEVERITY_BUCKETS = {"low", "medium", "high", "extreme", "unknown"}

from project.events.polarity import (
    CANONICAL_EVENT_SIDES as _VALID_EVENT_SIDES,
    normalize_event_side as _normalize_event_side,
    normalize_polarity_semantics,
    side_to_direction as _direction_from_side,
)

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
    "event_side",
    "event_direction",
    "magnitude",
    "severity_bucket",
    "polarity_semantics",
    "polarity_source",
    "magnitude_source",
    "anchor_role",
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

    event_side: str = "unknown"
    event_direction: int = 0
    magnitude: float | None = None
    severity_bucket: str = "unknown"
    polarity_semantics: str = "unknown"
    polarity_source: str = "unknown"
    magnitude_source: str = "unknown"
    anchor_role: str = "alpha_anchor"

    def __post_init__(self) -> None:
        if self.confidence is not None:
            self.confidence = float(max(0.0, min(1.0, self.confidence)))
        if self.severity is not None:
            self.severity = float(max(0.0, min(1.0, self.severity)))
        if self.data_quality_flag not in _VALID_QUALITY_FLAGS:
            raise ValueError(f"invalid data_quality_flag {self.data_quality_flag!r}")
        self.event_side = _normalize_event_side(self.event_side)
        self.event_direction = _direction_from_side(self.event_side, self.event_direction)
        if self.magnitude is not None:
            self.magnitude = float(self.magnitude)
        bucket = str(self.severity_bucket or "unknown").strip().lower()
        if bucket not in _VALID_SEVERITY_BUCKETS:
            bucket = "unknown"
        self.severity_bucket = bucket
        self.polarity_semantics = normalize_polarity_semantics(self.polarity_semantics)
        self.polarity_source = str(self.polarity_source or "unknown").strip() or "unknown"
        self.magnitude_source = str(self.magnitude_source or "unknown").strip() or "unknown"
        self.anchor_role = str(self.anchor_role or "alpha_anchor").strip().lower() or "alpha_anchor"
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
    if "event_side" not in out.columns:
        side_source = next((c for c in ("side", "event_polarity", "direction", "event_direction") if c in out.columns), None)
        out["event_side"] = out[side_source].map(_normalize_event_side) if side_source else "unknown"
    else:
        out["event_side"] = out["event_side"].map(_normalize_event_side)
    if "event_direction" not in out.columns:
        out["event_direction"] = out["event_side"].map(_direction_from_side)
    else:
        out["event_direction"] = [
            _direction_from_side(side, fallback)
            for side, fallback in zip(out["event_side"], out["event_direction"])
        ]
    if "magnitude" not in out.columns:
        out["magnitude"] = out["trigger_value"] if "trigger_value" in out.columns else None
    if "severity_bucket" not in out.columns:
        out["severity_bucket"] = "unknown"
    if "polarity_semantics" not in out.columns:
        out["polarity_semantics"] = "unknown"
    else:
        out["polarity_semantics"] = out["polarity_semantics"].map(normalize_polarity_semantics)
    if "polarity_source" not in out.columns:
        out["polarity_source"] = "unknown"
    if "magnitude_source" not in out.columns:
        out["magnitude_source"] = "unknown"
    if "anchor_role" not in out.columns:
        out["anchor_role"] = out["role"] if "role" in out.columns else "alpha_anchor"
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
    invalid_sides = {_normalize_event_side(v) for v in frame["event_side"].dropna().astype(str)} - _VALID_EVENT_SIDES
    if invalid_sides:
        raise ValueError(f"invalid event_side values: {sorted(invalid_sides)}")
    direction_values = frame["event_direction"].map(lambda value: _direction_from_side("unknown", value))
    invalid_directions = set(direction_values.dropna().astype(int)) - {-1, 0, 1}
    if invalid_directions:
        raise ValueError(f"invalid event_direction values: {sorted(invalid_directions)}")
    invalid_buckets = set(frame["severity_bucket"].dropna().astype(str).str.lower()) - _VALID_SEVERITY_BUCKETS
    if invalid_buckets:
        raise ValueError(f"invalid severity_bucket values: {sorted(invalid_buckets)}")
    invalid_semantics = {normalize_polarity_semantics(v) for v in frame["polarity_semantics"].dropna().astype(str)} - {
        "price_direction", "deviation_direction", "basis_spread_direction", "funding_crowding_side",
        "price_oi_quadrant", "liquidation_side", "liquidity_sweep_side", "regime_transition",
        "temporal_guard", "execution_guard", "neutral_guard", "unknown",
    }
    if invalid_semantics:
        raise ValueError(f"invalid polarity_semantics values: {sorted(invalid_semantics)}")
