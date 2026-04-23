from __future__ import annotations

import pytest
from pathlib import Path


def test_all_family_contracts_exist():
    families = ["stat_disloc", "trend", "liquidity", "positioning", "execution"]
    for f in families:
        path = Path(f"spec/benchmarks/confirmatory_rerun_contract_{f}.yaml")
        assert path.exists(), f"Contract for {f} is missing"
