import ast
import os
from typing import List, Tuple

SUSPICIOUS_METHODS = {"quantile", "mean", "std", "median", "ffill"}
ALLOWED_WRAPPERS = {"rolling", "expanding", "shift", "trailing_"}


class TemporalLintVisitor(ast.NodeVisitor):
    def __init__(self, filename: str):
        self.filename = filename
        self.errors: List[Tuple[int, str]] = []

    def visit_Call(self, node: ast.Call):
        if isinstance(node.func, ast.Attribute):
            method_name = node.func.attr
            if method_name in SUSPICIOUS_METHODS:
                # Check if it's preceded by a safe wrapper
                # e.g., df.rolling(...).mean()
                is_safe = False
                current = node.func.value
                while isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
                    if current.func.attr in ALLOWED_WRAPPERS:
                        is_safe = True
                        break
                    current = current.func.value

                if not is_safe:
                    self.errors.append(
                        (
                            node.lineno,
                            f"Potentially unsafe full-sample operation: '{method_name}'. "
                            f"Consider using a rolling/trailing primitive.",
                        )
                    )
        self.generic_visit(node)


def lint_file(filepath: str) -> List[Tuple[int, str]]:
    with open(filepath, "r") as f:
        tree = ast.parse(f.read())
    visitor = TemporalLintVisitor(filepath)
    visitor.visit(tree)
    return visitor.errors


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python temporal_lint.py <file_or_dir>")
        sys.exit(1)

    target = sys.argv[1]
    all_errors = []

    if os.path.isfile(target):
        files = [target]
    else:
        files = []
        for root, _, filenames in os.walk(target):
            for f in filenames:
                if f.endswith(".py"):
                    files.append(os.path.join(root, f))

    for f in files:
        errors = lint_file(f)
        for line, msg in errors:
            print(f"{f}:{line}: {msg}")
            all_errors.append((f, line, msg))

    if all_errors:
        sys.exit(1)
    else:
        print("No PIT linting errors found.")
