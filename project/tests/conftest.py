from __future__ import annotations

from pathlib import Path

import pytest



PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PROJECT_ROOT.parent


# Tests frequently write logical `.parquet` artifacts.  Minimal CI/runtime
# environments may not install pyarrow or fastparquet, so activate the
# repository's pickle-backed parquet compatibility layer during test startup.
from project.io.parquet_compat import patch_pandas_parquet_fallback

patch_pandas_parquet_fallback()


@pytest.fixture
def scenario_registry():
    from project.tests.synthetic_truth.scenarios.registry import SCENARIO_REGISTRY

    return SCENARIO_REGISTRY


@pytest.fixture
def default_seed():
    return 42


@pytest.fixture
def default_symbol():
    return "BTCUSDT"
