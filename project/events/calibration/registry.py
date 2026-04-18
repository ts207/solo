from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Iterable

from project import PROJECT_ROOT
from project.core.config import get_data_root
from project.events.calibration.io import load_calibration_artifact
from project.events.calibration.models import DetectorCalibrationArtifact


def calibration_root() -> Path:
    return get_data_root() / "artifacts" / "calibration" / "detectors"


def packaged_calibration_root() -> Path:
    return PROJECT_ROOT / "events" / "calibration" / "artifacts" / "detectors"


def calibration_roots() -> tuple[Path, ...]:
    return (calibration_root(), packaged_calibration_root())


@lru_cache(maxsize=256)
def find_calibration_artifacts(event_name: str) -> tuple[DetectorCalibrationArtifact, ...]:
    artifacts: list[DetectorCalibrationArtifact] = []
    token = str(event_name).strip().upper()
    for base_root in calibration_roots():
        root = base_root / token
        if not root.exists():
            continue
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


def calibration_registry_key(artifact: DetectorCalibrationArtifact) -> tuple[str, str, str, str]:
    return (
        artifact.event_name,
        artifact.event_version,
        artifact.symbol_group,
        artifact.timeframe_group,
    )


def find_duplicate_calibration_keys(event_names: Iterable[str] | None = None) -> dict[tuple[str, str, str, str], list[str]]:
    artifacts: list[DetectorCalibrationArtifact] = []
    if event_names is None:
        names: set[str] = set()
        for root in calibration_roots():
            if root.exists():
                names.update(path.name for path in root.iterdir() if path.is_dir())
        event_names = sorted(names)
    for event_name in event_names:
        artifacts.extend(find_calibration_artifacts(event_name))

    seen: dict[tuple[str, str, str, str], list[str]] = {}
    for artifact in artifacts:
        key = calibration_registry_key(artifact)
        seen.setdefault(key, []).append(str(artifact.path or ""))
    return {key: paths for key, paths in seen.items() if len(paths) > 1}


def build_calibration_matrix_rows(event_names: Iterable[str] | None = None) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if event_names is None:
        roots: set[str] = set()
        for root in calibration_roots():
            if root.exists():
                roots.update(path.name for path in root.iterdir() if path.is_dir())
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
                "dataset_lineage": artifact.dataset_lineage,
                "training_period": artifact.training_period,
                "validation_period": artifact.validation_period,
                "robustness": artifact.robustness,
                "path": str(artifact.path) if artifact.path else "",
            })
    return rows
