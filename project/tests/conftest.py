from __future__ import annotations

from project import PROJECT_ROOT

import pytest

from project.tests.synthetic_truth.scenarios.registry import SCENARIO_REGISTRY
from project.tests.synthetic_truth.scenarios.factory import ScenarioFactory


REPO_ROOT = PROJECT_ROOT.parent
SPEC_ROOT = REPO_ROOT / "spec"


@pytest.fixture
def scenario_registry():
    return SCENARIO_REGISTRY


@pytest.fixture
def default_seed():
    return 42


@pytest.fixture
def default_symbol():
    return "BTCUSDT"
