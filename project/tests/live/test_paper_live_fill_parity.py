from __future__ import annotations

import pandas as pd

from project.engine.execution_simulator import compare_expected_realized_fill_costs


def test_paper_live_fill_cost_gap_stays_inside_declared_tolerance_for_replay_fixture() -> None:
    expected = pd.DataFrame(
        {
            "client_order_id": ["o1", "o2", "o3"],
            "expected_cost_bps": [4.0, 5.0, 6.0],
        }
    )
    realized = pd.DataFrame(
        {
            "client_order_id": ["o1", "o2", "o3"],
            "realized_total_cost_bps": [4.5, 5.5, 6.5],
        }
    )

    comparison = compare_expected_realized_fill_costs(
        expected,
        realized,
        tolerance_bps=1.0,
    )

    assert comparison["within_tolerance"] is True
    assert comparison["mean_abs_gap_bps"] == 0.5
    assert comparison["samples"] == 3.0
