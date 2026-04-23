from __future__ import annotations

from pathlib import Path

import pytest

from project.tests.synthetic_truth.scenarios.registry import SCENARIO_REGISTRY

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def scenario_registry():
    return SCENARIO_REGISTRY


@pytest.fixture
def default_seed():
    return 42


@pytest.fixture
def default_symbol():
    return "BTCUSDT"
