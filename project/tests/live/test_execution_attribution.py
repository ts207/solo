from __future__ import annotations

import pytest

from project.live.execution_attribution import (
    build_execution_attribution_record,
    summarize_execution_attribution,
)


def test_build_execution_attribution_record_for_buy_order() -> None:
    record = build_execution_attribution_record(
        client_order_id="o1",
        symbol="BTCUSDT",
        strategy="alpha_1",
        volatility_regime="elevated",
        microstructure_regime="healthy",
        side="BUY",
        quantity=1.0,
        signal_timestamp="2024-01-01T00:00:00+00:00",
        expected_entry_price=100.0,
        realized_fill_price=100.2,
        expected_return_bps=40.0,
        expected_adverse_bps=10.0,
        expected_cost_bps=5.0,
        realized_fee_bps=2.0,
    )

    assert record.strategy == "alpha_1"
    assert record.volatility_regime == "elevated"
    assert record.microstructure_regime == "healthy"
    assert record.realized_slippage_bps == pytest.approx(20.0)
    assert record.realized_total_cost_bps == pytest.approx(22.0)
    assert record.expected_net_edge_bps == pytest.approx(25.0)
    assert record.realized_net_edge_bps == pytest.approx(8.0)
    assert record.edge_decay_bps == pytest.approx(-17.0)


def test_summarize_execution_attribution_reports_edge_decay() -> None:
    records = [
        build_execution_attribution_record(
            client_order_id="o1",
            symbol="BTCUSDT",
            strategy="alpha_1",
            volatility_regime="elevated",
            microstructure_regime="healthy",
            side="BUY",
            quantity=1.0,
            signal_timestamp="2024-01-01T00:00:00+00:00",
            expected_entry_price=100.0,
            realized_fill_price=100.1,
            expected_return_bps=40.0,
            expected_adverse_bps=10.0,
            expected_cost_bps=5.0,
            realized_fee_bps=2.0,
        ),
        build_execution_attribution_record(
            client_order_id="o2",
            symbol="ETHUSDT",
            strategy="alpha_2",
            volatility_regime="stressed",
            microstructure_regime="fragile",
            side="SELL",
            quantity=1.0,
            signal_timestamp="2024-01-01T00:05:00+00:00",
            expected_entry_price=200.0,
            realized_fill_price=199.6,
            expected_return_bps=35.0,
            expected_adverse_bps=8.0,
            expected_cost_bps=4.0,
            realized_fee_bps=1.0,
        ),
    ]

    summary = summarize_execution_attribution(records)

    assert summary["fills"] == 2.0
    assert summary["avg_expected_net_edge_bps"] > summary["avg_realized_net_edge_bps"]
    assert summary["avg_edge_decay_bps"] < 0.0
    assert summary["avg_realized_fee_bps"] == 1.5
