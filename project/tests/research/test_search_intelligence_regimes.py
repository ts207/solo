from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from project.research.search_intelligence import _build_frontier, _build_summary


def test_search_frontier_is_regime_first_and_executable_only():
    frontier = _build_frontier(
        SimpleNamespace(events={"events": {}}),
        tested_regions=pd.DataFrame(
            [{"event_type": "LIQUIDITY_STRESS_DIRECT", "canonical_regime": "LIQUIDITY_STRESS"}]
        ),
        failures=pd.DataFrame(),
        untested_top_k=3,
        repair_top_k=1,
        exhausted_failure_threshold=3,
        quality_weights={},
    )

    assert "untested_canonical_regimes" in frontier
    assert "LIQUIDITY_STRESS" not in frontier["untested_canonical_regimes"]
    fanout = frontier["canonical_regime_event_fanout"]
    for regime, event_ids in fanout.items():
        assert regime
        assert "SEQ_LIQ_VACUUM_THEN_DEPTH_RECOVERY" not in event_ids
        assert "SESSION_OPEN_EVENT" not in event_ids
        assert "COPULA_PAIRS_TRADING" not in event_ids


def test_build_summary_hides_top_regions_without_statistical_support():
    tested_regions = pd.DataFrame(
        [
            {
                "run_id": "r1",
                "candidate_id": "",
                "event_type": "VOL_SHOCK",
                "template_id": "continuation",
                "direction": "short",
                "horizon": "24b",
                "eval_status": "evaluated",
                "after_cost_expectancy": 2.1,
                "q_value": None,
                "gate_promo_statistical": False,
            }
        ]
    )

    summary = _build_summary("btc_campaign", tested_regions, top_k=5)

    assert summary["top_performing_regions"] == []


def test_build_summary_hides_top_regions_with_non_significant_q_value():
    tested_regions = pd.DataFrame(
        [
            {
                "run_id": "r1",
                "candidate_id": "",
                "event_type": "VOL_SHOCK",
                "template_id": "continuation",
                "direction": "short",
                "horizon": "24b",
                "eval_status": "evaluated",
                "after_cost_expectancy": 2.1,
                "q_value": 0.64,
                "gate_promo_statistical": False,
            }
        ]
    )

    summary = _build_summary("btc_campaign", tested_regions, top_k=5)

    assert summary["top_performing_regions"] == []


def test_build_summary_keeps_supported_positive_regions():
    tested_regions = pd.DataFrame(
        [
            {
                "run_id": "r1",
                "candidate_id": "c1",
                "event_type": "VOL_SHOCK",
                "template_id": "continuation",
                "direction": "short",
                "horizon": "24b",
                "eval_status": "evaluated",
                "after_cost_expectancy": 2.1,
                "q_value": 0.04,
                "gate_promo_statistical": False,
            },
            {
                "run_id": "r1",
                "candidate_id": "c2",
                "event_type": "RANGE_BREAKOUT",
                "template_id": "continuation",
                "direction": "long",
                "horizon": "24b",
                "eval_status": "evaluated",
                "after_cost_expectancy": -0.5,
                "q_value": 0.02,
                "gate_promo_statistical": True,
            },
        ]
    )

    summary = _build_summary("btc_campaign", tested_regions, top_k=5)

    assert len(summary["top_performing_regions"]) == 1
    assert summary["top_performing_regions"][0]["candidate_id"] == "c1"
