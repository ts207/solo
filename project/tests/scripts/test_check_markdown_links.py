from __future__ import annotations

from pathlib import Path

from project.scripts.check_markdown_links import collect_markdown_link_issues


def test_markdown_link_checker_accepts_relative_and_repo_root_links(tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    (tmp_path / "project").mkdir()
    (tmp_path / "project" / "cli.py").write_text("", encoding="utf-8")
    (docs / "next.md").write_text("# Next\n", encoding="utf-8")
    (docs / "index.md").write_text(
        "[next](next.md)\n[cli](project/cli.py)\n[external](https://example.com)\n",
        encoding="utf-8",
    )

    assert collect_markdown_link_issues(root=tmp_path) == []


def test_markdown_link_checker_reports_missing_internal_links(tmp_path: Path) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "index.md").write_text("[missing](missing.md)\n", encoding="utf-8")

    issues = collect_markdown_link_issues(root=tmp_path)

    assert len(issues) == 1
    assert issues[0].markdown_path == "docs/index.md"
    assert issues[0].target == "missing.md"


def test_markdown_link_checker_skips_generated_by_default(tmp_path: Path) -> None:
    generated = tmp_path / "docs" / "generated"
    generated.mkdir(parents=True)
    (generated / "index.md").write_text("[missing](missing.md)\n", encoding="utf-8")

    assert collect_markdown_link_issues(root=tmp_path) == []
