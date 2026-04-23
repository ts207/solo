from __future__ import annotations

import pandas as pd

from project.engine.execution_simulator import (
    ExecutionSimulationConfig,
    calibrate_execution_model_v2,
    simulate_execution_event,
    simulate_execution_frame,
)
from project.engine.fill_model_v2 import FillModelRequest


def test_execution_simulator_models_latency_and_partial_passive_fill() -> None:
    result = simulate_execution_event(
        FillModelRequest(
            symbol="BTCUSDT",
            side="buy",
            quantity=2.0,
            order_type="limit",
            urgency="passive",
            limit_price=99.9,
        ),
        {
            "bid": 99.9,
            "ask": 100.1,
            "mid_price": 100.0,
            "spread_bps": 2.0,
            "depth_usd": 1_000.0,
            "vol_regime_bps": 20.0,
        },
        ExecutionSimulationConfig(fee_bps_per_side=3.0, latency_ms=500),
    )

    assert result["model_family"] == "execution_simulator_v2"
    assert result["latency_ms"] == 500
    assert 0.0 < result["filled_quantity"] < result["requested_quantity"]
    assert result["residual_quantity"] > 0.0
    assert result["expected_total_cost_bps"] > result["expected_fee_bps"]


def test_execution_simulator_frame_uses_same_cost_family() -> None:
    idx = pd.date_range("2026-04-19", periods=2, freq="5min", tz="UTC")
    frame = pd.DataFrame(
        {
            "spread_bps": [1.0, 3.0],
            "depth_usd": [1_000_000.0, 50_000.0],
            "close": [100.0, 100.0],
            "high": [100.1, 100.4],
            "low": [99.9, 99.6],
        },
        index=idx,
    )
    simulated = simulate_execution_frame(
        frame,
        pd.Series([1_000.0, 25_000.0], index=idx),
        {"base_fee_bps": 2.0, "urgency": "aggressive"},
    )

    assert simulated["model_family"].nunique() == 1
    assert simulated["expected_cost_bps"].iloc[1] > simulated["expected_cost_bps"].iloc[0]


def test_execution_simulator_calibrates_from_observed_fills() -> None:
    calibrated = calibrate_execution_model_v2(
        {"base_fee_bps": 4.0},
        pd.DataFrame(
            {
                "realized_fee_bps": [2.0, 3.0, 4.0],
                "realized_slippage_bps": [0.4, 0.6, 0.8],
            }
        ),
    )

    assert calibrated["cost_model"] == "execution_simulator_v2"
    assert calibrated["base_fee_bps"] == 3.0
    assert calibrated["passive_adverse_selection_bps"] == 0.6
