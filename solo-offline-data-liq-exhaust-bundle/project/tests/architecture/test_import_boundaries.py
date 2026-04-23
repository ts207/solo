from __future__ import annotations

import ast
from pathlib import Path

from project import PROJECT_ROOT


def _forbidden_imports(root: Path, forbidden_prefix: str) -> list[tuple[str, str]]:
    findings: list[tuple[str, str]] = []
    for path in sorted(root.rglob("*.py")):
        # Skip __init__.py if needed, or other specific files
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = str(node.module or "")
                if module.startswith(forbidden_prefix):
                    findings.append((str(path.relative_to(PROJECT_ROOT)), module))
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(forbidden_prefix):
                        findings.append((str(path.relative_to(PROJECT_ROOT)), alias.name))
    return findings


def test_research_layer_does_not_import_pipeline_modules():
    findings = _forbidden_imports(PROJECT_ROOT / "research", "project.pipelines")
    assert findings == []


def test_contracts_layer_does_not_import_pipeline_modules():
    findings = _forbidden_imports(PROJECT_ROOT / "contracts", "project.pipelines")
    assert findings == []


def test_runtime_layer_does_not_import_io_modules():
    findings = _forbidden_imports(PROJECT_ROOT / "runtime", "project.io")
    assert findings == []
