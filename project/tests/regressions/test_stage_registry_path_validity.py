from __future__ import annotations

from project import PROJECT_ROOT
from project.pipelines import stage_registry


def test_stage_registry_exact_script_paths_resolve() -> None:
    issues = stage_registry.validate_stage_registry_definitions(PROJECT_ROOT)
    assert issues == []
