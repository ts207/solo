"""
E2-T1: Stage cache hash must include directly-imported project.* module hashes.

If a shared utility module changes, the cache hash must change — even if the
stage script itself is unchanged.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from project.pipelines.execution_engine import compute_stage_input_hash


def _write_files(tmp_path: Path, script_source: str, module_source: str) -> tuple[Path, Path]:
    """Write a stage script and a shared module into a temp project layout."""
    project_dir = tmp_path / "project" / "shared"
    project_dir.mkdir(parents=True)
    mod = project_dir / "util.py"
    mod.write_text(module_source)
    script = tmp_path / "stage_script.py"
    script.write_text(script_source)
    return script, mod


def test_hash_changes_when_directly_imported_module_changes(tmp_path):
    """Changing a project.* module imported by the stage script must invalidate the cache."""
    script_source = "from project.shared.util import foo\n\nfoo()\n"
    script, mod = _write_files(tmp_path, script_source, "def foo(): return 1\n")

    h1 = compute_stage_input_hash(script, ["--arg", "val"], "run1")

    mod.write_text("def foo(): return 2\n")  # module changed

    h2 = compute_stage_input_hash(script, ["--arg", "val"], "run1")

    assert h1 != h2, "Hash must change when a directly imported module changes"


def test_hash_stable_when_unimported_module_changes(tmp_path):
    """Changing a module NOT imported by the stage script must NOT change the cache hash."""
    script_source = "# no imports\nprint('hello')\n"
    script, mod = _write_files(tmp_path, script_source, "def bar(): return 1\n")

    h1 = compute_stage_input_hash(script, ["--arg", "val"], "run1")

    mod.write_text("def bar(): return 999\n")  # module changed but not imported

    h2 = compute_stage_input_hash(script, ["--arg", "val"], "run1")

    assert h1 == h2, "Hash must not change when an unimported module changes"


def test_hash_changes_when_import_module_changes(tmp_path):
    """Test `import project.shared.util` style (not just from ... import)."""
    script_source = "import project.shared.util\n"
    script, mod = _write_files(tmp_path, script_source, "X = 1\n")

    h1 = compute_stage_input_hash(script, [], "run1")

    mod.write_text("X = 2\n")

    h2 = compute_stage_input_hash(script, [], "run1")

    assert h1 != h2


def test_hash_stable_when_module_missing(tmp_path):
    """If an imported module file cannot be found, hash must still be deterministic."""
    script_source = "from project.nonexistent.module import something\n"
    script = tmp_path / "stage_script.py"
    script.write_text(script_source)

    h1 = compute_stage_input_hash(script, [], "run1")
    h2 = compute_stage_input_hash(script, [], "run1")

    assert h1 == h2, "Hash must be deterministic even when imported module is missing"


def test_hash_stable_when_script_missing(tmp_path):
    """Missing script should not crash; hash should be deterministic (existing behavior)."""
    script = tmp_path / "nonexistent_script.py"

    h1 = compute_stage_input_hash(script, [], "run1")
    h2 = compute_stage_input_hash(script, [], "run1")

    assert h1 == h2
