def test_spec_registry_init_does_not_define_loader_functions():
    """TICKET-019: __init__.py must be thin re-exports; loaders must live in loaders.py."""
    import ast
    import inspect
    from pathlib import Path

    import project.spec_registry as registry_mod

    init_path = Path(inspect.getfile(registry_mod))
    tree = ast.parse(init_path.read_text())
    defined_in_init = [
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name.startswith("load_")
    ]
    assert not defined_in_init, (
        f"__init__.py defines loader functions directly: {defined_in_init}. "
        "Move them to loaders.py."
    )


def test_spec_registry_public_api_unchanged():
    """TICKET-019: all existing public names remain importable from project.spec_registry."""
    from project.spec_registry import (
        REPO_ROOT,
        clear_caches,
        load_gates_spec,
    )

    assert callable(load_gates_spec)
    assert callable(clear_caches)
    assert REPO_ROOT.exists()
