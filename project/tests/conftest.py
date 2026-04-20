from __future__ import annotations

import os
from pathlib import Path
from project import PROJECT_ROOT

import pytest

from project.tests.synthetic_truth.scenarios.registry import SCENARIO_REGISTRY
from project.tests.synthetic_truth.scenarios.factory import ScenarioFactory


REPO_ROOT = PROJECT_ROOT.parent
SPEC_ROOT = REPO_ROOT / "spec"
PRODUCTION_DATA_LAKE = REPO_ROOT / "data"


@pytest.fixture(scope="session", autouse=True)
def isolate_data_lake(tmp_path_factory):
    """
    Enforce environmental isolation for all tests.
    Creates a temporary data root and points all relevant environment variables to it.
    """
    tmp_root = tmp_path_factory.mktemp("edge_test_data")
    
    # Create basic structure
    (tmp_root / "lake").mkdir(parents=True, exist_ok=True)
    
    # Set environment variables to point to the temporary root
    os.environ["BACKTEST_DATA_ROOT"] = str(tmp_root)
    
    # Clear EDGE_DATA_ROOT if it exists to allow BACKTEST_DATA_ROOT to take effect
    # and to allow tests to override isolation via monkeypatch.setenv("BACKTEST_DATA_ROOT", ...)
    if "EDGE_DATA_ROOT" in os.environ:
        del os.environ["EDGE_DATA_ROOT"]
    
    # Clear get_data_root cache if it exists (for various implementations)
    from project.core.config import get_data_root as g1
    from project.pipelines.pipeline_defaults import get_data_root as g2
    
    for func in [g1, g2]:
        if hasattr(func, "cache_clear"):
            func.cache_clear()
    
    yield tmp_root


@pytest.fixture(autouse=True)
def verify_isolation(isolate_data_lake):
    """
    Verification fixture that runs for every test to ensure no leak to production data.
    """
    from project.core.config import get_data_root
    
    def check():
        current_root = get_data_root()
        if str(PRODUCTION_DATA_LAKE) in str(current_root.resolve()):
            # If it's the exact production root or a subdirectory of it, fail.
            # Allow it only if it's within the temp directory.
            if str(isolate_data_lake) not in str(current_root.resolve()):
                 pytest.fail(f"Test isolation breach! get_data_root() returned production path: {current_root}")

    check()
    yield
    check()


@pytest.fixture
def scenario_registry():
    return SCENARIO_REGISTRY


@pytest.fixture
def default_seed():
    return 42


@pytest.fixture
def default_symbol():
    return "BTCUSDT"
