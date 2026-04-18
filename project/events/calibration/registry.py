from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Iterable

from project.core.config import get_data_root
from project.events.calibration.io import load_calibration_artifact
from project.events.calibration.models import DetectorCalibrationArtifact


def calibration_root() -> Path:
    return get_data_root() / "artifacts" / "calibration" / "detectors"


def _event_dir(event_name: str) -> Path:
    return calibration_root() / str(event_name).strip().upper()


@lru_cache(maxsize=256)
def find_calibration_artifacts(event_name: str) -> tuple[DetectorCalibrationArtifact, ...]:
    root = _event_dir(event_name)
    if not root.exists():
        return ()
    artifacts: list[DetectorCalibrationArtifact] = []
    for path in sorted(root.rglob("calibration.json")):
        try:
            artifacts.append(load_calibration_artifact(path))
        except Exception:
            continue
    return tuple(artifacts)


def latest_calibration_artifact(event_name: str, preferred_version: str | None = None) -> DetectorCalibrationArtifact | None:
    artifacts = list(find_calibration_artifacts(event_name))
    if preferred_version:
        preferred = [artifact for artifact in artifacts if artifact.event_version == preferred_version]
        if preferred:
            artifacts = preferred
    if not artifacts:
        return None
    artifacts.sort(key=lambda item: (item.event_version, item.threshold_version, str(item.path or "")))
    return artifacts[-1]


def build_calibration_matrix_rows(event_names: Iterable[str] | None = None) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if event_names is None:
        roots = [path.name for path in calibration_root().iterdir()] if calibration_root().exists() else []
        event_names = sorted(roots)
    for event_name in event_names:
        for artifact in find_calibration_artifacts(event_name):
            rows.append({
                "event_name": artifact.event_name,
                "event_version": artifact.event_version,
                "threshold_version": artifact.threshold_version,
                "calibration_mode": artifact.calibration_mode,
                "symbol_group": artifact.symbol_group,
                "timeframe_group": artifact.timeframe_group,
                "path": str(artifact.path) if artifact.path else "",
            })
    return rows
