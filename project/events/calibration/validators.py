from __future__ import annotations

from project.events.calibration.models import DetectorCalibrationArtifact


class CalibrationArtifactError(ValueError):
    pass


def validate_calibration_artifact(artifact: DetectorCalibrationArtifact) -> None:
    if not artifact.event_name.strip():
        raise CalibrationArtifactError("event_name must be non-empty")
    if not artifact.event_version.strip():
        raise CalibrationArtifactError("event_version must be non-empty")
    if not artifact.threshold_version.strip():
        raise CalibrationArtifactError("threshold_version must be non-empty")
    if not artifact.calibration_mode.strip():
        raise CalibrationArtifactError("calibration_mode must be non-empty")
