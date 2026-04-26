from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

from project.artifacts.catalog import data_root as resolve_data_root


def _unique_existing_paths(paths: Iterable[Path]) -> list[Path]:
    seen: set[str] = set()
    out: list[Path] = []
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        if path.exists():
            out.append(path)
    return out


def discover_run_artifacts(*, run_id: str, data_root: Path | None = None) -> dict[str, object]:
    """
    Canonical discovery artifacts live under:

      <data_root>/reports/phase2/<run_id>/

    This helper intentionally ignores legacy locations (e.g. reports/edge_candidates)
    to keep the "discover" lane unambiguous.
    """
    root = resolve_data_root(data_root)
    phase2_dir = root / "reports" / "phase2" / str(run_id)

    # Canonical artifacts emitted by phase2 search engine + discovery service.
    expected = [
        phase2_dir / "artifact_manifest.json",
        phase2_dir / "phase2_candidates.parquet",
        phase2_dir / "phase2_candidates.csv",
        phase2_dir / "phase2_diagnostics.json",
        phase2_dir / "canonical_research_path.json",
        phase2_dir / "discovery_decision_trace.parquet",
        phase2_dir / "discovery_decision_trace.csv",
        phase2_dir / "research_decision_trace.parquet",
        phase2_dir / "research_decision_trace.csv",
        phase2_dir / "phase2_candidate_overlap_metrics.parquet",
        phase2_dir / "phase2_diversified_shortlist.parquet",
    ]

    existing = _unique_existing_paths(expected)
    rel_paths: list[str] = []
    for path in existing:
        try:
            rel_paths.append(str(path.relative_to(root)))
        except ValueError:
            rel_paths.append(str(path))

    return {
        "run_id": str(run_id),
        "data_root": str(root),
        "phase2_dir": str(phase2_dir),
        "artifact_paths": rel_paths,
    }

