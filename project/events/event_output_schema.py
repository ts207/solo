from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


_VALID_QUALITY_FLAGS = {"ok", "degraded", "invalid"}


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
        return payload
