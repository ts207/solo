from pathlib import Path

from project.core.dependency_lock import build_dependency_lock_report


def test_dependency_lock_report_detects_unpinned(tmp_path: Path) -> None:
    pyproject_text = """
[project]
dependencies = [
  "numpy==1.0",
  "websockets",
]
"""
    (tmp_path / "pyproject.toml").write_text(pyproject_text, encoding="utf-8")
    (tmp_path / "constraints.lock").write_text("", encoding="utf-8")
    report = build_dependency_lock_report(project_root=tmp_path)
    assert report["status"] == "fail"
    assert "websockets" in report["unpinned_dependencies"]
