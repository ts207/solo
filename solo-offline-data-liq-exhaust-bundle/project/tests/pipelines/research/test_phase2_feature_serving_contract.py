"""
Contract test: phase2_search_engine uses search_feature_utils to serve features,
not the deleted search_feature_frame module. This test guards the new feature-serving
boundary against regression.
"""

import importlib
import pytest


def test_search_feature_frame_not_importable():
    """search_feature_frame was deleted — it must not be importable."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("project.research.search_feature_frame")


def test_search_engine_imports_search_feature_utils():
    """phase2_search_engine must import from search_feature_utils, not from the deleted module."""
    import project.research.phase2_search_engine as eng

    source = open(eng.__file__).read()
    assert "search_feature_utils" in source, (
        "phase2_search_engine must use search_feature_utils for feature serving"
    )
    assert "search_feature_frame" not in source, (
        "phase2_search_engine must not reference deleted search_feature_frame"
    )


def test_search_feature_utils_importable():
    """The new feature-serving module must be importable."""
    from project.research.search.search_feature_utils import (
        normalize_search_feature_columns,
        prepare_search_features_for_symbol,
    )

    assert callable(normalize_search_feature_columns)
    assert callable(prepare_search_features_for_symbol)
