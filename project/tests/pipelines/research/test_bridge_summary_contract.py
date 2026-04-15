from __future__ import annotations

import pandas as pd

from project.research.bridge_evaluate_phase2 import (
    _bridge_summary_count_fields,
    _build_bridge_summary_payload,
    _filter_candidates_for_symbol,
)
from project.research.bridge_evaluation import bridge_metrics_for_row


def test_bridge_summary_count_fields_include_canonical_and_extended_detectors():
    out = _bridge_summary_count_fields(
        n_candidates_in=12,
        n_candidates_tradable=3,
        n_candidates_tradable_without_microstructure=5,
        top_5_bridge_fail_reasons={"gate_bridge_after_cost_positive_validation": 9},
    )

    assert out["n_candidates_in"] == 12
    assert out["n_candidates_tradable"] == 3
    assert out["n_candidates_tradable_without_microstructure"] == 5
    assert out["microstructure_delta_tradable"] == 2
    assert out["top_5_bridge_fail_reasons"] == {"gate_bridge_after_cost_positive_validation": 9}

    # Backward-compatible aliases are kept in sync.
    assert out["candidate_count"] == out["n_candidates_in"]
    assert out["tradable_count"] == out["n_candidates_tradable"]


def test_bridge_summary_count_fields_empty_case_is_zero_safe():
    out = _bridge_summary_count_fields(
        n_candidates_in=0,
        n_candidates_tradable=0,
        n_candidates_tradable_without_microstructure=0,
        top_5_bridge_fail_reasons=None,
    )

    assert out["n_candidates_in"] == 0
    assert out["n_candidates_tradable"] == 0
    assert out["n_candidates_tradable_without_microstructure"] == 0
    assert out["microstructure_delta_tradable"] == 0
    assert out["top_5_bridge_fail_reasons"] == {}
    assert out["candidate_count"] == 0
    assert out["tradable_count"] == 0


def test_filter_candidates_for_symbol_keeps_only_matching_symbol_or_all():
    frame = pd.DataFrame(
        [
            {"candidate_id": "btc_1", "symbol": "BTCUSDT"},
            {"candidate_id": "eth_1", "symbol": "ETHUSDT"},
            {"candidate_id": "all_1", "symbol": "ALL"},
        ]
    )

    out = _filter_candidates_for_symbol(frame, "BTCUSDT")

    assert list(out["candidate_id"]) == ["btc_1", "all_1"]


def test_build_bridge_summary_payload_includes_primary_fail_and_cost_diagnostics():
    frame = pd.DataFrame(
        [
            {
                "gate_bridge_tradable": False,
                "gate_bridge_tradable_without_microstructure": False,
                "bridge_fail_reasons": "gate_bridge_after_cost_positive_validation,gate_bridge_edge_cost_ratio",
                "bridge_fail_gate_primary": "gate_bridge_after_cost_positive_validation",
                "bridge_validation_after_cost_bps": -0.5,
                "bridge_effective_cost_bps_per_trade": 0.5,
            },
            {
                "gate_bridge_tradable": False,
                "gate_bridge_tradable_without_microstructure": False,
                "bridge_fail_reasons": "gate_bridge_after_cost_positive_validation",
                "bridge_fail_gate_primary": "gate_bridge_after_cost_positive_validation",
                "bridge_validation_after_cost_bps": -0.5,
                "bridge_effective_cost_bps_per_trade": 0.5,
            },
        ]
    )

    out = _build_bridge_summary_payload(frame)

    assert out["candidate_count"] == 2
    assert out["tradable_count"] == 0
    assert out["after_cost_non_positive_count"] == 2
    assert out["median_bridge_validation_after_cost_bps"] == -0.5
    assert out["median_bridge_effective_cost_bps_per_trade"] == 0.5
    assert out["uniform_negative_expectancy_count"] == 2
    assert out["primary_fail_gate_counts"] == {"gate_bridge_after_cost_positive_validation": 2}


def test_bridge_metrics_for_row_skips_missing_optional_value_warning_spam(caplog):
    row = pd.Series(
        {
            "after_cost_expectancy_per_trade": -0.00005,
            "expectancy_per_trade": -0.00005,
            "avg_dynamic_cost_bps": 0.5,
            "turnover_proxy_mean": 0.5,
        }
    )

    with caplog.at_level("WARNING"):
        out = bridge_metrics_for_row(row, stressed_cost_multiplier=2.0)

    assert out["bridge_validation_after_cost_bps"] == -1.0
    assert "safe_float: failed to convert None" not in caplog.text
    assert "safe_int: failed to convert None" not in caplog.text


def test_bridge_metrics_for_row_falls_back_to_expectancy_when_per_trade_field_missing():
    row = pd.Series(
        {
            "expectancy": 0.00125,
            "avg_dynamic_cost_bps": 0.5,
            "turnover_proxy_mean": 0.5,
            "sample_size": 40,
        }
    )

    out = bridge_metrics_for_row(row, stressed_cost_multiplier=2.0)

    assert out["bridge_validation_after_cost_bps"] > 0.0
    assert out["bridge_validation_trades"] == 40
