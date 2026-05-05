from __future__ import annotations

import re
from pathlib import Path
from typing import Any

_UNPINNED_RE = re.compile(r'^\s*["\']?([A-Za-z0-9_.\-]+(?:\[[^\]]+\])?)(?:["\']|,)?\s*$')


def _extract_dependency_lines(pyproject_text: str) -> list[str]:
    in_deps = False
    out: list[str] = []
    for raw in pyproject_text.splitlines():
        line = raw.strip()
        if line == "dependencies = [":
            in_deps = True
            continue
        if in_deps and line == "]":
            break
        if in_deps and line and not line.startswith("#"):
            out.append(line.rstrip(","))
    return out


def _is_pinned(dep: str) -> bool:
    token = dep.strip().strip('"').strip("'")
    return "==" in token or " @ " in token


def build_dependency_lock_report(*, project_root: str | Path = ".") -> dict[str, Any]:
    root = Path(project_root)
    pyproject = root / "pyproject.toml"
    lock_candidates = [root / "uv.lock", root / "requirements.lock", root / "constraints.lock"]
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    if not pyproject.exists():
        errors.append({"error": "missing_pyproject", "path": str(pyproject)})
        return {"kind": "dependency_lock_report", "status": "fail", "errors": errors, "warnings": warnings}
    deps = _extract_dependency_lines(pyproject.read_text(encoding="utf-8"))
    unpinned = []
    for dep in deps:
        token = dep.strip().strip('"').strip("'")
        if token and not _is_pinned(token):
            unpinned.append(token)
    if unpinned:
        errors.append({"error": "unpinned_dependencies", "dependencies": unpinned})
    lockfiles = [str(path.relative_to(root)) for path in lock_candidates if path.exists()]
    if not lockfiles:
        errors.append({"error": "missing_lockfile", "expected_one_of": [str(p.relative_to(root)) for p in lock_candidates]})
    return {
        "kind": "dependency_lock_report",
        "project_root": str(root),
        "lockfiles": lockfiles,
        "unpinned_dependencies": unpinned,
        "errors": errors,
        "warnings": warnings,
        "status": "pass" if not errors else "fail",
    }
