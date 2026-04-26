#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = ROOT / "project"

PREFERRED_DEEP_IMPORTS: dict[str, tuple[str, str]] = {
    "project.artifacts.catalog": ("project.artifacts", "project/artifacts"),
    "project.compilers.executable_strategy_spec": ("project.compilers", "project/compilers"),
    "project.portfolio.allocation_spec": ("project.portfolio", "project/portfolio"),
    "project.portfolio.sizing": ("project.portfolio", "project/portfolio"),
    "project.spec_validation.loaders": ("project.spec_validation", "project/spec_validation"),
    "project.spec_validation.ontology": ("project.spec_validation", "project/spec_validation"),
    "project.spec_validation.search": ("project.spec_validation", "project/spec_validation"),
    "project.eval.splits": ("project.eval", "project/eval"),
    "project.live.runner": ("project.live", "project/live"),
    "project.live.health_checks": ("project.live", "project/live"),
    "project.live.state": ("project.live", "project/live"),
}

EXEMPT_RELATIVE = {
    "project/scripts/run_live_engine.py",
    "project/tests/eval/test_splits.py",
}


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _candidate_files(args: argparse.Namespace) -> list[Path]:
    if args.all:
        roots = [PROJECT_ROOT]
        files: list[Path] = []
        for root in roots:
            files.extend(root.rglob("*.py"))
        return sorted(files)
    return [Path(p).resolve() for p in args.files if str(p).endswith(".py")]


def check_files(files: list[Path]) -> list[str]:
    violations: list[str] = []
    for path in files:
        if not path.exists() or path.suffix != ".py":
            continue
        rel = path.relative_to(ROOT).as_posix()
        if rel in EXEMPT_RELATIVE or "/tests/" in f"/{rel}" or rel.startswith("project/scripts/"):
            continue
        imports = _imported_modules(path)
        for deep_module, (preferred_root, owner_root_rel) in PREFERRED_DEEP_IMPORTS.items():
            owner_root = ROOT / owner_root_rel
            if _is_under(path, owner_root):
                continue
            if deep_module in imports or any(module.startswith(f"{deep_module}.") for module in imports):
                violations.append(f"{rel} imports {deep_module}; prefer {preferred_root}")
    return violations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check architectural deep-import allowlist.")
    parser.add_argument("--all", action="store_true", help="scan all project Python files")
    parser.add_argument("files", nargs="*", help="changed files passed by pre-commit")
    args = parser.parse_args(argv)

    violations = check_files(_candidate_files(args))
    if violations:
        print("Banned import check failed:")
        for violation in sorted(set(violations)):
            print(f"- {violation}")
        return 1
    print("Banned import check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
