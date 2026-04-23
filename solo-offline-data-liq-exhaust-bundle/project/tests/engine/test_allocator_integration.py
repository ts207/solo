from __future__ import annotations

import pandas as pd
import pytest

from project.engine.risk_allocator import RiskLimits, allocate_position_details


def _ts(n: int) -> pd.DatetimeIndex:
    return pd.date_range("2024-01-01", periods=n, freq="5min", tz="UTC")


def test_allocate_position_details_exposes_portfolio_cap_diagnostics() -> None:
    idx = _ts(3)
    raw = {
        "s1": pd.Series([0.0, 1.0, 1.0], index=idx),
        "s2": pd.Series([0.0, 1.0, 1.0], index=idx),
    }
    req = {
        "s1": pd.Series([1.0, 1.0, 1.0], index=idx),
        "s2": pd.Series([1.0, 1.0, 1.0], index=idx),
    }
    details = allocate_position_details(
        raw,
        req,
        RiskLimits(
            max_portfolio_gross=1.0,
            max_symbol_gross=10.0,
            max_strategy_gross=10.0,
            max_new_exposure_per_bar=10.0,
        ),
    )

    diag = details.diagnostics.set_index("timestamp")
    assert details.allocated_positions_by_strategy["s1"].iloc[1] == pytest.approx(0.5)
    assert details.scale_by_strategy["s2"].iloc[1] == pytest.approx(0.5)
    assert diag.loc[idx[1], "allocated_gross"] == pytest.approx(1.0)
    assert "max_portfolio_gross" in diag.loc[idx[1], "clip_reason"]
