from __future__ import annotations

import pytest


def test_imports():
    """
    Smoke test for core package imports to catch migration regressions.
    """
    try:

        print("Imported project.events.registry")
    except Exception as e:
        pytest.fail(f"Failed to import project.events.registry: {e}")

    try:

        print("Imported project.engine.runner")
    except Exception as e:
        pytest.fail(f"Failed to import project.engine.runner: {e}")

    try:

        print("Imported project.strategy.runtime")
    except Exception as e:
        pytest.fail(f"Failed to import project.strategy.runtime: {e}")

    try:

        print("Imported project.strategy.dsl")
    except Exception as e:
        pytest.fail(f"Failed to import project.strategy.dsl: {e}")

    try:

        print("Imported project.strategy.templates")
    except Exception as e:
        pytest.fail(f"Failed to import project.strategy.templates: {e}")

    try:

        print("Imported project.pipelines.run_all")
    except Exception as e:
        pytest.fail(f"Failed to import project.pipelines.run_all: {e}")

    try:

        print("Imported project.events.event_prerequisites")
    except Exception as e:
        pytest.fail(f"Failed to import project.events.event_prerequisites: {e}")
