from __future__ import annotations

from pathlib import Path

import pytest

from project.core.run_guard import assert_run_id_available, existing_artifact_paths_for_run


def test_run_guard_detects_existing_phase2_artifacts(tmp_path: Path) -> None:
    run_id = "duplicate_run"
    phase2 = tmp_path / "reports" / "phase2" / run_id
    phase2.mkdir(parents=True)

    existing = existing_artifact_paths_for_run(
        run_id=run_id,
        data_root=tmp_path,
        stages=["discovery"],
    )

    assert phase2 in existing
    with pytest.raises(FileExistsError):
        assert_run_id_available(
            run_id=run_id,
            data_root=tmp_path,
            stages=["discovery"],
        )


def test_run_guard_allows_explicit_overwrite(tmp_path: Path) -> None:
    run_id = "overwrite_run"
    (tmp_path / "reports" / "phase2" / run_id).mkdir(parents=True)

    assert_run_id_available(
        run_id=run_id,
        data_root=tmp_path,
        stages=["discovery"],
        overwrite=True,
    )
