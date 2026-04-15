import ast
import os
from pathlib import Path


def test_no_unshifted_rolling_quantiles():
    project_root = Path(__file__).parent.parent.parent
    detectors_dir = project_root / "project" / "events" / "detectors"

    errors = []

    class QuantileVisitor(ast.NodeVisitor):
        def __init__(self, filename):
            self.filename = filename

        def visit_Call(self, node):
            if isinstance(node.func, ast.Attribute) and node.func.attr == "quantile":
                # Reconstruct the source code of the entire statement/expression containing this call
                # to do a simple string heuristic check, or we can traverse the AST deeply.
                # AST approach: look at the parent nodes or children to see if `shift` is called.

                # A simple approximation using unparse (Python 3.9+)
                try:
                    expr_str = ast.unparse(node)
                    # if shift(1) happens before .rolling().quantile()
                    # expression looks like x.shift(1).rolling().quantile()
                    has_prior_shift = ".shift(" in expr_str

                    if not has_prior_shift:
                        errors.append(
                            f"{self.filename}:{node.lineno} - Unshifted quantile call found: {expr_str}"
                        )
                except Exception:
                    pass

            self.generic_visit(node)

    for root, _, files in os.walk(detectors_dir):
        for file in files:
            if file.endswith(".py"):
                filepath = Path(root) / file
                with open(filepath, "r", encoding="utf-8") as f:
                    tree = ast.parse(f.read(), filename=str(filepath))

                # We need to check if the quantile call is nested inside a shift() call
                # e.g. x.rolling().quantile().shift(1)
                # To do this safely, we will look at all Call nodes where func.attr == 'shift'
                # and see if they *contain* a quantile call.
                # Then we aggregate all quantile calls and remove the ones covered by a shift.

                class ShiftVisitor(ast.NodeVisitor):
                    def __init__(self):
                        self.shifted_quantile_nodes = set()

                    def visit_Call(self, node):
                        if isinstance(node.func, ast.Attribute) and node.func.attr == "shift":
                            # Check if the caller of shift is a quantile call
                            # e.g. `obj.quantile(0.9).shift(1)`
                            # node.func.value should be the `quantile` call
                            caller = node.func.value
                            if (
                                isinstance(caller, ast.Call)
                                and isinstance(caller.func, ast.Attribute)
                                and caller.func.attr == "quantile"
                            ):
                                self.shifted_quantile_nodes.add(caller)
                        self.generic_visit(node)

                shift_visitor = ShiftVisitor()
                shift_visitor.visit(tree)

                class LeakedQuantileVisitor(ast.NodeVisitor):
                    def visit_Call(self, node):
                        if isinstance(node.func, ast.Attribute) and node.func.attr == "quantile":
                            if node not in shift_visitor.shifted_quantile_nodes:
                                # Also check if there's a shift *before* the rolling
                                expr_str = ast.unparse(node)
                                if ".shift(" not in expr_str:
                                    errors.append(
                                        f"{filepath.name}:{node.lineno} - Unshifted quantile call: {expr_str}"
                                    )
                        self.generic_visit(node)

                LeakedQuantileVisitor().visit(tree)

    assert not errors, "\\n".join(errors)
