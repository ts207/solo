from __future__ import annotations

from project.live.live_quality_gate import LiveQualityThresholds, evaluate_live_quality_gate


def test_live_quality_gate_downscales_on_slippage_and_fill_drift() -> None:
    result = evaluate_live_quality_gate(
        "T1",
        {
            "sample_count": 20,
            "slippage_drift_bps": 10.0,
            "fill_rate": 0.50,
            "edge_divergence_bps": 5.0,
        },
        LiveQualityThresholds(
            max_slippage_drift_bps=5.0,
            disable_slippage_drift_bps=20.0,
            min_fill_rate=0.80,
            disable_fill_rate=0.30,
        ),
    )

    assert result.action == "downscale"
    assert 0.0 < result.risk_scale < 1.0
    assert "slippage_drift" in result.reason_codes
    assert "fill_rate_drift" in result.reason_codes


def test_live_quality_gate_disables_on_edge_divergence_and_stale_frequency() -> None:
    result = evaluate_live_quality_gate(
        "T1",
        {
            "sample_count": 20,
            "edge_divergence_bps": 30.0,
            "stale_data_frequency": 0.25,
        },
    )

    assert result.action == "disable"
    assert result.risk_scale == 0.0
    assert "edge_divergence_disable" in result.reason_codes
    assert "stale_data_frequency_disable" in result.reason_codes


def test_live_quality_gate_waits_for_minimum_evidence() -> None:
    result = evaluate_live_quality_gate(
        "T1",
        {
            "sample_count": 2,
            "edge_divergence_bps": 100.0,
        },
        LiveQualityThresholds(min_samples=5),
    )

    assert result.action == "allow"
    assert result.risk_scale == 1.0
