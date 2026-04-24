from __future__ import annotations

from pathlib import Path

import pytest

from project.artifacts.catalog import (
    blueprint_summary_path,
    checklist_path,
    kpi_scorecard_path,
    load_json_dict,
    phase2_candidates_path,
    phase2_diagnostics_path,
    promotion_summary_path,
    run_manifest_path,
)
from project.artifacts.compat import phase2_candidates_compat_path, phase2_diagnostics_compat_path
from project.core.exceptions import DataIntegrityError


def test_catalog_paths_and_json_loading(tmp_path: Path) -> None:
    root = tmp_path / "data"
    run_id = "r1"
    assert run_manifest_path(run_id, root) == root / "runs" / run_id / "run_manifest.json"
    assert (
        checklist_path(run_id, root)
        == root / "runs" / run_id / "research_checklist" / "checklist.json"
    )
    assert kpi_scorecard_path(run_id, root) == root / "runs" / run_id / "kpi_scorecard.json"
    assert (
        promotion_summary_path(run_id, root)
        == root / "reports" / "promotions" / run_id / "promotion_summary.json"
    )
    assert (
        blueprint_summary_path(run_id, root)
        == root / "reports" / "strategy_blueprints" / run_id / "blueprint_summary.json"
    )

    p = run_manifest_path(run_id, root)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text('{"status": "success"}', encoding="utf-8")
    assert load_json_dict(p)["status"] == "success"


def test_load_json_dict_raises_for_malformed_existing_json(tmp_path: Path) -> None:
    path = run_manifest_path("broken", tmp_path / "data")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not-json", encoding="utf-8")

    with pytest.raises(DataIntegrityError, match="Failed to read JSON artifact"):
        load_json_dict(path)


def test_phase2_candidates_prefers_existing_parquet(tmp_path: Path) -> None:
    root = tmp_path / "data"
    base = root / "reports" / "phase2" / "r2"
    base.mkdir(parents=True, exist_ok=True)
    csv_path = base / "phase2_candidates.csv"
    csv_path.write_text("candidate_id\n1\n", encoding="utf-8")
    assert phase2_candidates_path("r2", root) == csv_path
    pq_path = base / "phase2_candidates.parquet"
    pq_path.write_bytes(b"PAR1")
    assert phase2_candidates_path("r2", root) == pq_path


def test_canonical_phase2_paths_ignore_legacy_nested_layouts(tmp_path: Path) -> None:
    root = tmp_path / "data"
    legacy_dir = root / "reports" / "phase2" / "r3" / "search_engine"
    legacy_dir.mkdir(parents=True, exist_ok=True)
    (legacy_dir / "phase2_diagnostics.json").write_text("{}", encoding="utf-8")

    assert (
        phase2_diagnostics_path("r3", root)
        == root / "reports" / "phase2" / "r3" / "phase2_diagnostics.json"
    )


def test_compat_phase2_helpers_resolve_legacy_paths_explicitly(tmp_path: Path) -> None:
    root = tmp_path / "data"
    legacy_phase2 = root / "reports" / "phase2" / "r4"
    event_dir = legacy_phase2 / "legacy" / "VOL"
    event_dir.mkdir(parents=True, exist_ok=True)
    candidate_path = event_dir / "phase2_candidates.csv"
    candidate_path.write_text("candidate_id\n1\n", encoding="utf-8")
    diagnostics_dir = legacy_phase2 / "search_engine"
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    diagnostics_path = diagnostics_dir / "phase2_diagnostics.json"
    diagnostics_path.write_text("{}", encoding="utf-8")

    assert (
        phase2_candidates_compat_path("r4", event_type="VOL", root=root)
        == root / "reports" / "phase2" / "r4" / "phase2_candidates.parquet"
    )
    assert (
        phase2_diagnostics_compat_path("r4", root=root)
        == root / "reports" / "phase2" / "r4" / "phase2_diagnostics.json"
    )
    assert (
        phase2_candidates_compat_path(
            "r4", event_type="VOL", root=root, allow_legacy=True
        )
        == candidate_path
    )
    assert phase2_diagnostics_compat_path("r4", root=root, allow_legacy=True) == diagnostics_path
