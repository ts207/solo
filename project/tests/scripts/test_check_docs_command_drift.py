from __future__ import annotations

from pathlib import Path

from project.scripts import check_docs_command_drift


def test_collect_md_files_skips_generated_docs(tmp_path: Path, monkeypatch) -> None:
    docs = tmp_path / "docs"
    generated = docs / "generated"
    docs.mkdir()
    generated.mkdir()
    active_doc = docs / "operator.md"
    generated_doc = generated / "legacy_surface_inventory.md"
    active_doc.write_text("# Operator\n", encoding="utf-8")
    generated_doc.write_text("# Generated\n", encoding="utf-8")

    monkeypatch.setattr(check_docs_command_drift, "SCAN_PATHS", [docs])

    files = check_docs_command_drift.collect_md_files()

    assert files == [active_doc]
