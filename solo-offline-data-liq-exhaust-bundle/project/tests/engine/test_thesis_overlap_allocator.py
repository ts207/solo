from __future__ import annotations

import pandas as pd
import pytest

from project.engine.risk_allocator import AllocationContract, AllocationPolicy, RiskLimits, allocate_position_details


def _ts(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")


def test_allocator_applies_overlap_group_budget_caps_and_evidence_scaling() -> None:
    idx = _ts(2)
    raw = {
        "s1": pd.Series([0.0, 1.0], index=idx),
        "s2": pd.Series([0.0, 1.0], index=idx),
    }
    req = {
        "s1": pd.Series([1.0, 1.0], index=idx),
        "s2": pd.Series([1.0, 1.0], index=idx),
    }
    contract = AllocationContract(
        limits=RiskLimits(
            max_portfolio_gross=10.0,
            max_strategy_gross=10.0,
            max_symbol_gross=10.0,
            max_new_exposure_per_bar=10.0,
        ),
        policy=AllocationPolicy(
            strategy_thesis_map={"s1": "thesis::1", "s2": "thesis::2"},
            thesis_overlap_group_map={"thesis::1": "grp_a", "thesis::2": "grp_a"},
            overlap_group_risk_budgets={"grp_a": 1.0},
            thesis_evidence_multipliers={"thesis::1": 0.5, "thesis::2": 1.0},
        ),
    )

    details = allocate_position_details(raw, req, contract.limits, contract=contract)

    # requested becomes 0.5 + 1.0 = 1.5 gross; overlap cap of 1.0 scales both by 2/3
    assert details.allocated_positions_by_strategy["s1"].iloc[1] == pytest.approx(1.0 / 3.0)
    assert details.allocated_positions_by_strategy["s2"].iloc[1] == pytest.approx(2.0 / 3.0)
    assert details.summary["overlap_group_budget_hits"] == {"grp_a": 1}
    assert "thesis_overlap_budget" in details.diagnostics.loc[1, "clip_reason"]
