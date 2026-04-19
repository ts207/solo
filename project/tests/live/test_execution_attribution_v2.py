from __future__ import annotations

from project.live.execution_attribution import (
    build_execution_attribution_record,
    summarize_execution_attribution,
    summarize_execution_attribution_by,
    summarize_live_quality_inputs,
)


def test_execution_attribution_record_carries_thesis_overlap_fields() -> None:
    record = build_execution_attribution_record(
        client_order_id="oid1",
        symbol="BTCUSDT",
        strategy="thesis::1",
        thesis_id="thesis::1",
        overlap_group_id="grp_a",
        governance_tier="A",
        operational_role="trigger",
        active_episode_ids=["EP_VOL_BREAKOUT"],
        volatility_regime="VOLATILITY",
        microstructure_regime="THIN",
        side="BUY",
        quantity=1.0,
        signal_timestamp="2026-03-31T00:00:00Z",
        expected_entry_price=100.0,
        realized_fill_price=101.0,
        expected_return_bps=15.0,
        expected_adverse_bps=5.0,
        expected_cost_bps=2.0,
        realized_fee_bps=1.0,
    )

    summary = summarize_execution_attribution([record])
    by_group = summarize_execution_attribution_by([record], "overlap_group_id")

    assert record.thesis_id == "thesis::1"
    assert record.active_episode_ids == ["EP_VOL_BREAKOUT"]
    assert summary["overlap_group_count"] == 1.0
    assert summary["episode_count"] == 1.0
    assert "grp_a" in by_group


def test_execution_attribution_summarizes_live_quality_inputs() -> None:
    records = [
        build_execution_attribution_record(
            client_order_id=f"oid{i}",
            symbol="BTCUSDT",
            strategy="thesis::1",
            thesis_id="thesis::1",
            volatility_regime="VOLATILITY",
            microstructure_regime="THIN",
            side="BUY",
            quantity=1.0,
            signal_timestamp="2026-03-31T00:00:00Z",
            expected_entry_price=100.0,
            realized_fill_price=100.1,
            expected_return_bps=15.0,
            expected_adverse_bps=5.0,
            expected_cost_bps=2.0,
            realized_fee_bps=1.0,
        )
        for i in range(3)
    ]

    metrics = summarize_live_quality_inputs(
        records,
        expected_slippage_bps=0.0,
        submitted_orders=4,
        stale_data_events=1,
        thesis_decay_rate=0.25,
    )

    assert metrics["sample_count"] == 3.0
    assert metrics["fill_rate"] == 0.75
    assert metrics["stale_data_frequency"] == 0.25
    assert metrics["thesis_decay_rate"] == 0.25
