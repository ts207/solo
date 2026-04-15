from pathlib import Path
import subprocess

def test_makefile_exports_canonical_targets():
    """Verify Makefile provides the unified 4-stage interface explicitly."""
    repo_root = Path(__file__).parent.parent.parent.parent
    makefile_path = repo_root / "Makefile"
    assert makefile_path.exists(), "Makefile not found"
    content = makefile_path.read_text()
    
    assert "discover:" in content
    assert "validate:" in content
    assert "promote:" in content
    assert "deploy-paper:" in content

import sys


def test_removed_pipeline_alias_hidden_from_cli_help():
    """Verify the old pipeline alias is no longer part of the public CLI."""
    repo_root = Path(__file__).parent.parent.parent.parent
    result = subprocess.run(
        [sys.executable, "-m", "project.cli", "-h"],
        cwd=repo_root,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0
    assert "pipeline" not in result.stdout
    assert "run-all" not in result.stdout
