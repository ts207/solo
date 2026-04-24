#!/usr/bin/env python3
import ast
import json
import re
import sys
from collections import defaultdict
from pathlib import Path


def _files_importing(root: Path, module_pattern: str) -> list[str]:
    pattern = re.compile(rf"(?:from|import)\s+{re.escape(module_pattern)}(?:\.|\s|$)")
    matches: list[str] = []
    for base in (root / "project", root / "project" / "tests"):
        if not base.exists():
            continue
        for file_path in base.rglob("*.py"):
            text = file_path.read_text(encoding="utf-8")
            if pattern.search(text):
                matches.append(str(file_path.relative_to(root)).replace("\\", "/"))
    return sorted(set(matches))

def main():
    root = Path(__file__).resolve().parent.parent.parent
    project_dir = root / "project"

    if not project_dir.exists():
        print("project/ not found")
        sys.exit(1)

    modules = {}
    test_funcs = 0
    src_funcs = 0

    # Build dependency graph
    edges = []

    for py_file in project_dir.rglob("*.py"):
        rel_path = py_file.relative_to(root)
        module_name = str(rel_path.with_suffix("")).replace("/", ".")
        try:
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
        except SyntaxError:
            continue

        imports = []
        is_test = "tests" in py_file.parts

        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if is_test and node.name.startswith("test_"):
                    test_funcs += 1
                elif not is_test:
                    src_funcs += 1
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("project."):
                        imports.append(alias.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module and node.module.startswith("project."):
                    imports.append(node.module)

        modules[module_name] = imports
        for imp in imports:
            edges.append((module_name, imp))

    coupling_count = len(edges)

    cross_boundary_imports = 0
    for src, dst in edges:
        src_parts = src.split(".")
        dst_parts = dst.split(".")
        if len(src_parts) >= 2 and len(dst_parts) >= 2:
            if src_parts[1] != dst_parts[1]:
                cross_boundary_imports += 1

    graph = defaultdict(list)
    for src, dst in edges:
        graph[src].append(dst)

    visited = set()
    path_stack = set()
    circular_dependency_count = 0

    def dfs(node):
        nonlocal circular_dependency_count
        visited.add(node)
        path_stack.add(node)
        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                dfs(neighbor)
            elif neighbor in path_stack:
                circular_dependency_count += 1
        path_stack.remove(node)

    for node in graph:
        if node not in visited:
            dfs(node)

    test_coverage_ratio = test_funcs / max(1, src_funcs + test_funcs)
    compat_importers = len(_files_importing(root, "project.research.compat"))
    strategy_dsl_importers = len(_files_importing(root, "project.strategy_dsl"))
    strategy_templates_importers = len(_files_importing(root, "project.strategy_templates"))
    run_all_path = root / "project" / "pipelines" / "run_all.py"
    run_all_coordinator_lines = (
        len(run_all_path.read_text(encoding="utf-8").splitlines()) if run_all_path.exists() else 0
    )

    metrics = {
        "metrics": {
            "project.research.compat_importers": compat_importers,
            "project.strategy_dsl_importers": strategy_dsl_importers,
            "project.strategy_templates_importers": strategy_templates_importers,
            "run_all_coordinator_lines": run_all_coordinator_lines,
            "module_coupling_count": coupling_count,
            "cross_boundary_import_count": cross_boundary_imports,
            "circular_dependency_count": circular_dependency_count,
            "test_coverage_ratio": test_coverage_ratio,
        }
    }

    out_file = root / "docs" / "generated" / "architecture_metrics.json"
    out_file.parent.mkdir(parents=True, exist_ok=True)

    # Check baseline to avoid coupling increase
    if out_file.exists():
        try:
            old = json.loads(out_file.read_text())
            old_coupling = old.get("metrics", {}).get("module_coupling_count", float('inf'))
            if coupling_count > old_coupling:
                print(f"FAILED: Coupling increased from {old_coupling} to {coupling_count}", file=sys.stderr)
                if "--check" in sys.argv:
                    sys.exit(1)
            # preserve existing metrics not generated here
            old["metrics"].update(metrics["metrics"])
            metrics = old
        except Exception:
            pass

    out_file.write_text(json.dumps(metrics, indent=2))
    print(f"Wrote metrics to {out_file}")

if __name__ == "__main__":
    main()
