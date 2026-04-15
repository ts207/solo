from __future__ import annotations

from pathlib import Path

from project.tests.conftest import REPO_ROOT


ALLOWED = {
    Path("project/io/utils.py"),
    Path("project/scripts/generate_synthetic_milestone_data.py"),
}


def test_project_code_uses_shared_storage_abstraction_for_artifact_writes() -> None:
    offenders: list[str] = []
    for path in (REPO_ROOT / "project").rglob("*.py"):
        if path.is_relative_to(REPO_ROOT / "project" / "tests"):
            continue
        rel = path.relative_to(REPO_ROOT)
        if rel in ALLOWED:
            continue
        text = path.read_text(encoding="utf-8")
        if ".to_parquet(" in text:
            offenders.append(str(rel))
    assert offenders == [], f"direct to_parquet bypasses shared storage abstraction: {offenders}"
