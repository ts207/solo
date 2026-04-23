from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DetectorCalibrationArtifact:
    event_name: str
    event_version: str
    threshold_version: str
    calibration_mode: str
    symbol_group: str
    timeframe_group: str
    dataset_lineage: dict[str, Any] = field(default_factory=dict)
    training_period: dict[str, Any] = field(default_factory=dict)
    validation_period: dict[str, Any] = field(default_factory=dict)
    parameters: dict[str, Any] = field(default_factory=dict)
    robustness: dict[str, Any] = field(default_factory=dict)
    failure_notes: tuple[str, ...] = ()
    notes: str = ""
    path: Path | None = None
