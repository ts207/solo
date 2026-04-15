"""Guard: unused microstructure features must not be computed in build_features."""

from __future__ import annotations
import ast
from pathlib import Path

REMOVED_COLUMNS = {"ms_roll_24", "ms_amihud_24", "ms_kyle_24", "ms_vpin_24"}
# Anchor the path to this file's location so the test works from any working directory.
BUILD_FEATURES_PATH = (
    Path(__file__).resolve().parents[3] / "pipelines" / "features" / "build_features.py"
)


def test_removed_ms_features_not_assigned():
    source = BUILD_FEATURES_PATH.read_text()
    tree = ast.parse(source)
    assigned_keys = set()
    for node in ast.walk(tree):
        # Catch `out["ms_roll_24"] = ...` patterns
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Subscript):
                    if isinstance(target.slice, ast.Constant):
                        assigned_keys.add(target.slice.value)
    still_present = REMOVED_COLUMNS & assigned_keys
    assert not still_present, f"Removed ms_* features still assigned: {still_present}"
