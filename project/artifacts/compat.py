from __future__ import annotations

from pathlib import Path

from project.artifacts.catalog import data_root, phase2_candidates_path, phase2_diagnostics_path
from project.research.services.pathing import phase2_run_dir


def phase2_candidates_compat_path(
    run_id: str,
    event_type: str | None = None,
    root: Path | None = None,
) -> Path:
    canonical = phase2_candidates_path(run_id, root)
    if canonical.exists():
        return canonical

    resolved_root = data_root(root)
    legacy_event = str(event_type or "").strip()
    if legacy_event:
        legacy_root = phase2_run_dir(data_root=resolved_root, run_id=run_id)
        for legacy_base in (
            legacy_root / legacy_event,
            legacy_root / "legacy" / legacy_event,
        ):
            legacy_parquet = legacy_base / "phase2_candidates.parquet"
            if legacy_parquet.exists():
                return legacy_parquet
            legacy_csv = legacy_base / "phase2_candidates.csv"
            if legacy_csv.exists():
                return legacy_csv
            for nested in sorted(legacy_base.glob("*/phase2_candidates.parquet")):
                return nested
            for nested in sorted(legacy_base.glob("*/phase2_candidates.csv")):
                return nested

    return canonical


def phase2_diagnostics_compat_path(
    run_id: str,
    root: Path | None = None,
) -> Path:
    canonical = phase2_diagnostics_path(run_id, root)
    if canonical.exists():
        return canonical

    resolved_root = data_root(root)
    legacy = (
        phase2_run_dir(data_root=resolved_root, run_id=run_id)
        / "search_engine"
        / "phase2_diagnostics.json"
    )
    if legacy.exists():
        return legacy
    return canonical
