from __future__ import annotations

import json
from pathlib import Path

from project.events.calibration.models import DetectorCalibrationArtifact
from project.events.calibration.validators import validate_calibration_artifact


def load_calibration_artifact(path: Path) -> DetectorCalibrationArtifact:
    payload = json.loads(path.read_text(encoding="utf-8"))
    artifact = DetectorCalibrationArtifact(
        event_name=str(payload.get("event_name", "")).strip().upper(),
        event_version=str(payload.get("event_version", "")).strip(),
        threshold_version=str(payload.get("threshold_version", "")).strip(),
        calibration_mode=str(payload.get("calibration_mode", "")).strip(),
        symbol_group=str(payload.get("symbol_group", "default")).strip(),
        timeframe_group=str(payload.get("timeframe_group", "default")).strip(),
        dataset_lineage=dict(payload.get("dataset_lineage", {}) or {}),
        training_period=dict(payload.get("training_period", {}) or {}),
        validation_period=dict(payload.get("validation_period", {}) or {}),
        parameters=dict(payload.get("parameters", {}) or {}),
        robustness=dict(payload.get("robustness", {}) or {}),
        failure_notes=tuple(str(item) for item in payload.get("failure_notes", []) or []),
        notes=str(payload.get("notes", "")),
        path=path,
    )
    validate_calibration_artifact(artifact)
    return artifact


def save_calibration_artifact(path: Path, artifact: DetectorCalibrationArtifact) -> None:
    validate_calibration_artifact(artifact)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "event_name": artifact.event_name,
        "event_version": artifact.event_version,
        "threshold_version": artifact.threshold_version,
        "calibration_mode": artifact.calibration_mode,
        "symbol_group": artifact.symbol_group,
        "timeframe_group": artifact.timeframe_group,
        "dataset_lineage": dict(artifact.dataset_lineage),
        "training_period": dict(artifact.training_period),
        "validation_period": dict(artifact.validation_period),
        "parameters": dict(artifact.parameters),
        "robustness": dict(artifact.robustness),
        "failure_notes": list(artifact.failure_notes),
        "notes": artifact.notes,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
