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
    if not artifact.symbol_group.strip():
        raise CalibrationArtifactError("symbol_group must be non-empty")
    if not artifact.timeframe_group.strip():
        raise CalibrationArtifactError("timeframe_group must be non-empty")
    if not artifact.parameters:
        raise CalibrationArtifactError(f"{artifact.event_name}: parameters must be non-empty")
    if not artifact.robustness:
        raise CalibrationArtifactError(f"{artifact.event_name}: robustness must be non-empty")
    if not artifact.dataset_lineage:
        raise CalibrationArtifactError(f"{artifact.event_name}: dataset_lineage must be non-empty")
    if not artifact.training_period:
        raise CalibrationArtifactError(f"{artifact.event_name}: training_period must be non-empty")
    if not artifact.validation_period:
        raise CalibrationArtifactError(f"{artifact.event_name}: validation_period must be non-empty")
