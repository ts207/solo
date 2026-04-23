import pytest
import pandas as pd
from project.research.compile_strategy_blueprints import (
    _passes_fallback_gate,
    _passes_quality_floor,
    _choose_event_rows,
    _validate_promoted_candidates_frame,
)


def test_passes_fallback_gate():
    gates = {"min_t_stat": 2.5, "min_after_cost_expectancy_bps": 1.0, "min_sample_size": 100}
    # Pass
    row_pass = {
        "t_stat": 3.0,
        "after_cost_expectancy_per_trade": 0.0002,  # 2 bps
        "n_events": 150,
    }
    assert _passes_fallback_gate(row_pass, gates) == True

    # Fail T-stat
    row_fail_t = row_pass.copy()
    row_fail_t["t_stat"] = 2.0
    assert _passes_fallback_gate(row_fail_t, gates) == False


def test_choose_event_rows_fallback():
    phase2_df = pd.DataFrame(
        [
            {
                "candidate_id": "test_1",
                "t_stat": 3.0,
                "after_cost_expectancy_per_trade": 0.0002,
                "stressed_after_cost_expectancy_per_trade": 0.0001,  # Added this
                "n_events": 150,
                "is_discovery": False,  # NOT a discovery
                "robustness_score": 0.8,
                "expectancy_per_trade": 0.0003,
                "cost_ratio": 0.1,
                "gate_bridge_tradable": True,
            }
        ]
    )
    # In discovery mode, should return nothing
    selected, diag, _ = _choose_event_rows(
        "run_test", "event_test", [], phase2_df, 1, True, True, 50, mode="discovery"
    )
    assert len(selected) == 0

    # In fallback mode, should return test_1
    selected, diag, _ = _choose_event_rows(
        "run_test", "event_test", [], phase2_df, 1, True, True, 50, mode="fallback"
    )
    assert len(selected) == 1
    assert selected[0]["candidate_id"] == "test_1"


def test_validate_promoted_candidates_frame_rejects_non_promoted():
    df = pd.DataFrame(
        [
            {"candidate_id": "c1", "status": "PROMOTED", "event": "VOL_SHOCK"},
            {"candidate_id": "c2", "status": "REJECTED", "event": "VOL_SHOCK"},
        ]
    )
    with pytest.raises(ValueError, match="non-promoted"):
        _validate_promoted_candidates_frame(df, source_label="unit_test")


def test_passes_quality_floor_enforces_retail_constraints():
    row = {
        "robustness_score": 0.9,
        "n_events": 200,
        "after_cost_expectancy_per_trade": 0.0005,
        "stressed_after_cost_expectancy_per_trade": 0.0003,
        "cost_ratio": 0.2,
        "gate_bridge_tradable": True,
        "bridge_validation_after_cost_bps": 5.0,
        "bridge_effective_cost_bps_per_trade": 8.0,
        "turnover_proxy_mean": 3.0,
        "tob_coverage": 0.85,
    }
    assert _passes_quality_floor(
        row,
        strict_cost_fields=True,
        min_events=100,
        min_robustness=0.6,
        min_tob_coverage=0.8,
        min_net_expectancy_bps=4.0,
        max_fee_plus_slippage_bps=10.0,
        max_daily_turnover_multiple=4.0,
    )
    row_bad = dict(row)
    row_bad["bridge_effective_cost_bps_per_trade"] = 12.0
    assert not _passes_quality_floor(
        row_bad,
        strict_cost_fields=True,
        min_events=100,
        min_robustness=0.6,
        min_tob_coverage=0.8,
        min_net_expectancy_bps=4.0,
        max_fee_plus_slippage_bps=10.0,
        max_daily_turnover_multiple=4.0,
    )
