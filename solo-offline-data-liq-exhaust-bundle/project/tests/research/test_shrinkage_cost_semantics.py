from __future__ import annotations

import pandas as pd
import pytest

from project.research.helpers.diagnostics import _refresh_phase2_metrics_after_shrinkage


def test_refresh_phase2_metrics_prefers_explicit_round_trip_cost_field() -> None:
    frame = pd.DataFrame(
        [
            {
                "effect_shrunk_state": 100.0,
                "cost_bps_resolved": 6.0,
                "round_trip_cost_bps_resolved": 14.0,
                "p_value_shrunk": 0.01,
                "sample_size": 12,
                "n_events": 12,
                "validation_samples": 4,
                "test_samples": 4,
            }
        ]
    )

    out = _refresh_phase2_metrics_after_shrinkage(
        frame,
        min_after_cost=0.0,
        conservative_cost_multiplier=2.0,
        min_sample_size_gate=0,
        require_sign_stability=False,
        quality_floor_fallback=0.0,
        min_events_fallback=0,
        min_information_weight_state=0.0,
    )

    row = out.iloc[0]
    assert row["after_cost_expectancy"] == pytest.approx(86.0)
    assert row["after_cost_expectancy_per_trade"] == pytest.approx(0.0086)
    assert row["stressed_after_cost_expectancy"] == pytest.approx(72.0)
    assert row["stressed_after_cost_expectancy_per_trade"] == pytest.approx(0.0072)
    assert bool(row["gate_economic"]) is True
    assert bool(row["gate_economic_conservative"]) is True


def test_refresh_phase2_metrics_doubles_per_side_cost_when_round_trip_missing() -> None:
    frame = pd.DataFrame(
        [
            {
                "effect_shrunk_state": 100.0,
                "cost_bps_resolved": 6.0,
                "p_value_shrunk": 0.01,
                "sample_size": 12,
                "n_events": 12,
                "validation_samples": 4,
                "test_samples": 4,
            }
        ]
    )

    out = _refresh_phase2_metrics_after_shrinkage(
        frame,
        min_after_cost=0.0,
        conservative_cost_multiplier=2.0,
        min_sample_size_gate=0,
        require_sign_stability=False,
        quality_floor_fallback=0.0,
        min_events_fallback=0,
        min_information_weight_state=0.0,
    )

    row = out.iloc[0]
    assert row["after_cost_expectancy"] == pytest.approx(88.0)
    assert row["after_cost_expectancy_per_trade"] == pytest.approx(0.0088)
    assert row["stressed_after_cost_expectancy"] == pytest.approx(76.0)
    assert row["stressed_after_cost_expectancy_per_trade"] == pytest.approx(0.0076)
