from __future__ import annotations

from pathlib import Path
from typing import Iterable

from project import PROJECT_ROOT


def resolve_data_root(data_root: str | Path | None = None) -> Path:
    if data_root:
        return Path(data_root)
    return PROJECT_ROOT.parent / "data"


def existing_artifact_paths_for_run(
    *,
    run_id: str,
    data_root: str | Path | None = None,
    stages: Iterable[str] | None = None,
) -> list[Path]:
    """Return existing artifact roots for a run and optional lifecycle stages.

    This is intentionally conservative: a stage guard should fail before a run can
    silently overwrite shared research outputs keyed only by run_id.
    """

    root = resolve_data_root(data_root)
    requested = {str(stage).strip().lower() for stage in (stages or []) if str(stage).strip()}
    if not requested:
        requested = {"discovery", "validation", "promotion", "thesis", "config"}

    candidates: dict[str, list[Path]] = {
        "discovery": [
            root / "reports" / "phase2" / run_id,
            root / "reports" / "edge_candidates" / run_id,
        ],
        "validation": [root / "reports" / "validation" / run_id],
        "promotion": [root / "reports" / "promotions" / run_id],
        "thesis": [root / "live" / "theses" / run_id],
        "config": [PROJECT_ROOT / "configs" / f"live_paper_{run_id}.yaml"],
    }
    out: list[Path] = []
    for stage in requested:
        out.extend(path for path in candidates.get(stage, []) if path.exists())
    return out


def assert_run_id_available(
    *,
    run_id: str | None,
    data_root: str | Path | None = None,
    stages: Iterable[str] | None = None,
    overwrite: bool = False,
) -> None:
    """Raise if a run_id already has artifacts and overwrite was not explicit."""

    token = str(run_id or "").strip()
    if not token:
        return
    if overwrite:
        return
    existing = existing_artifact_paths_for_run(run_id=token, data_root=data_root, stages=stages)
    if existing:
        paths = "\n".join(f"  - {path}" for path in existing)
        raise FileExistsError(
            "Refusing to overwrite existing run artifacts for "
            f"run_id={token!r}. Pass --overwrite only if this is intentional.\n{paths}"
        )
