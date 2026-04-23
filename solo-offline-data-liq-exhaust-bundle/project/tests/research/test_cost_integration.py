from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

import project.research.cost_integration as cost_integration


def test_integrate_execution_costs_applies_round_trip_cost_semantics(monkeypatch) -> None:
    monkeypatch.setattr(
        cost_integration,
        "resolve_execution_costs",
        lambda **kwargs: SimpleNamespace(
            config_digest="digest-1",
            fee_bps_per_side=4.0,
            slippage_bps_per_fill=2.0,
            round_trip_cost_bps=12.0,
            cost_bps=6.0,
        ),
    )

    frame = pd.DataFrame(
        [
            {
                "candidate_id": "c1",
                "expectancy_per_trade": 0.01,
            }
        ]
    )

    out = cost_integration.integrate_execution_costs(frame, "BTCUSDT")
    row = out.iloc[0]
    assert row["symbol"] == "BTCUSDT"
    assert row["cost_config_digest"] == "digest-1"
    assert row["resolved_cost_bps"] == pytest.approx(6.0)
    assert row["round_trip_cost_bps"] == pytest.approx(12.0)
    assert row["after_cost_expectancy_per_trade"] == pytest.approx(0.0088)
    assert row["after_cost_expectancy"] == pytest.approx(88.0)
    assert row["stressed_after_cost_expectancy_per_trade"] == pytest.approx(0.0076)
    assert row["stressed_after_cost_expectancy"] == pytest.approx(76.0)
    assert bool(row["gate_after_cost_positive"]) is True
    assert bool(row["gate_after_cost_stressed_positive"]) is True
