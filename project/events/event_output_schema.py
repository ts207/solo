from dataclasses import dataclass
from datetime import datetime
from typing import Any

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
    data_quality_flag: str          # ok | degraded | invalid
    merge_key: str | None
    cooldown_until: datetime | None
