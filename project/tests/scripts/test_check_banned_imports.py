from __future__ import annotations

from pathlib import Path

from project.scripts.check_banned_imports import check_files


def test_banned_import_checker_flags_deep_import(tmp_path: Path) -> None:
    repo = Path(__file__).resolve().parents[3]
    target = repo / "project" / "tmp_banned_import_fixture.py"
    target.write_text("from project.live.runner import LiveRunner\n", encoding="utf-8")
    try:
        violations = check_files([target])
    finally:
        target.unlink(missing_ok=True)
    assert violations
    assert "project.live.runner" in violations[0]
