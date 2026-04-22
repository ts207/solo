from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

import project.research.agent_io.campaign_planner as planner_mod
from project.core.exceptions import DataIntegrityError
from project.research.agent_io.campaign_planner import CampaignPlanner, CampaignPlannerConfig
from project.research.knowledge.memory import write_memory_table
from project.research.knowledge.schemas import canonical_json


@pytest.fixture
def patched_planner_registry(monkeypatch):
    monkeypatch.setattr(
        planner_mod,
        "build_canonical_semantic_registry_views",
        lambda: {
            "events": {
                "events": {
                    "EVENT_A": {"enabled": True, "family": "FAMILY_A"},
                    "EVENT_B": {"enabled": True, "family": "FAMILY_B"},
                }
            },
            "templates": {
                "families": {
                    "FAMILY_A": {"allowed_templates": ["mean_reversion"]},
                    "FAMILY_B": {"allowed_templates": ["mean_reversion"]},
                }
            },
        },
    )
    monkeypatch.setattr(
        planner_mod,
        "event_matches_filters",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        planner_mod,
        "get_event_governance_metadata",
        lambda event_type: {
            "tier": "A",
            "operational_role": "trigger",
            "deployment_disposition": "deployable",
            "evidence_mode": "direct",
            "trade_trigger_eligible": True,
            "rank_penalty": 0.0,
        },
    )
    monkeypatch.setattr(
        planner_mod,
        "load_event_priority_weights",
        lambda _path: {"EVENT_A": 10.0, "EVENT_B": 1.0},
    )


def _planner(tmp_path: Path, **overrides) -> CampaignPlanner:
    config = {
        "program_id": "program_1",
        "registry_root": tmp_path / "registries",
        "data_root": tmp_path / "data",
        "symbols": ("BTCUSDT",),
        "horizon_bars": (12,),
        "entry_lags": (1,),
        "directions": ("long",),
        "max_proposals": 2,
    }
    config.update(overrides)
    return CampaignPlanner(CampaignPlannerConfig(**config))


def _write_tested_regions(tmp_path: Path, rows: list[dict]) -> None:
    base = {
        "program_id": "program_1",
        "run_id": "run_prior",
        "symbol_scope": "BTCUSDT",
        "trigger_type": "EVENT",
        "template_id": "mean_reversion",
        "direction": "long",
        "horizon": "12",
        "entry_lag": 1,
        "context_json": canonical_json({"carry_state": ["neutral"]}),
        "eval_status": "evaluated",
        "failure_confidence": 0.9,
        "failure_sample_size": 100,
    }
    write_memory_table(
        "program_1",
        "tested_regions",
        pd.DataFrame([{**base, **row} for row in rows]),
        data_root=tmp_path / "data",
    )


def _write_event_statistics(tmp_path: Path, rows: list[dict]) -> None:
    base = {
        "runs_tested": 0,
        "times_evaluated": 0,
        "times_promoted": 0,
        "avg_q_value": 0.5,
        "avg_after_cost_expectancy": 0.0,
        "dominant_fail_gate": "",
    }
    write_memory_table(
        "program_1",
        "event_statistics",
        pd.DataFrame([{**base, **row} for row in rows]),
        data_root=tmp_path / "data",
    )


def test_campaign_planner_prefers_high_priority_event_when_scope_is_unseen(
    patched_planner_registry,
    tmp_path: Path,
) -> None:
    plan = _planner(tmp_path).plan()

    assert plan.ranked_proposals[0].event_type == "EVENT_A"
    assert plan.ranked_proposals[0].rationale["priority_score"] == 1.0
    assert plan.summary["duplicate_region_exclusions"] == 0
    selection = plan.summary["selection_rationale"]
    assert selection["selected_event_type"] == "EVENT_A"
    assert selection["runner_up_event_type"] == "EVENT_B"
    assert selection["score_margin"] > 0
    assert selection["dominant_positive_factors"][0]["factor"] == "priority_score"


def test_campaign_planner_blocks_fully_tested_duplicate_scope_and_explains_why(
    patched_planner_registry,
    tmp_path: Path,
) -> None:
    contexts = {"vol_regime": ["low", "high"]}
    write_memory_table(
        "program_1",
        "tested_regions",
        pd.DataFrame(
            [
                {
                    "program_id": "program_1",
                    "run_id": "run_prior",
                    "symbol_scope": "BTCUSDT",
                    "event_type": "EVENT_A",
                    "trigger_type": "EVENT",
                    "template_id": "mean_reversion",
                    "direction": "long",
                    "horizon": "12",
                    "entry_lag": 1,
                    "context_json": canonical_json(contexts),
                    "eval_status": "evaluated",
                    "failure_cause_class": "market",
                    "failure_confidence": 0.9,
                    "failure_sample_size": 100,
                }
            ]
        ),
        data_root=tmp_path / "data",
    )

    plan = _planner(tmp_path).plan()

    assert plan.ranked_proposals[0].event_type == "EVENT_B"
    assert [proposal.event_type for proposal in plan.ranked_proposals] == ["EVENT_B"]
    assert plan.summary["duplicate_region_exclusions"] == 1
    reason = plan.summary["duplicate_exclusion_reasons"][0]
    assert reason["event_type"] == "EVENT_A"
    assert reason["reason"] == "all_proposed_scope_combinations_already_tested"
    assert reason["tested_scope_count"] == 1
    assert plan.excluded_region_keys == reason["excluded_region_keys"]


def test_campaign_planner_scores_regime_gap_only_for_matching_event_history(
    patched_planner_registry,
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        planner_mod,
        "load_event_priority_weights",
        lambda _path: {"EVENT_A": 1.0, "EVENT_B": 1.0},
    )
    _write_tested_regions(
        tmp_path,
        [
            {
                "event_type": "EVENT_A",
                "failure_cause_class": "market",
                "context_json": canonical_json({"vol_regime": ["high"]}),
                "horizon": "24",
            },
            {
                "event_type": "EVENT_B",
                "failure_cause_class": "market",
                "context_json": canonical_json({"vol_regime": ["high", "low"]}),
                "horizon": "24",
            },
        ],
    )

    plan = _planner(tmp_path, regime_gap_threshold=0).plan()

    assert plan.ranked_proposals[0].event_type == "EVENT_A"
    event_a = plan.ranked_proposals[0]
    event_b = plan.ranked_proposals[1]
    assert event_a.proposal["contexts"] == {"vol_regime": ["low"]}
    assert event_a.rationale["regime_score"] == 1.0
    assert event_a.rationale["score_components"]["regime_score"] == 0.8
    assert event_a.rationale["regime_gap"]["undercovered_contexts"]["vol_regime"]["counts"] == {
        "high": 1,
        "low": 0,
    }
    assert event_b.event_type == "EVENT_B"
    assert event_b.rationale["regime_score"] == 0.0
    assert event_b.rationale["score_components"]["regime_score"] == 0.0
    assert event_b.rationale["regime_gap"]["undercovered_contexts"] == {}


def test_campaign_planner_fails_closed_on_malformed_tested_region_context_json(
    patched_planner_registry,
    tmp_path: Path,
) -> None:
    _write_tested_regions(
        tmp_path,
        [
            {
                "event_type": "EVENT_A",
                "failure_cause_class": "market",
                "context_json": "{not-json",
            }
        ],
    )

    with pytest.raises(DataIntegrityError, match="tested_regions.context_json"):
        _planner(tmp_path).plan()


def test_campaign_planner_ranks_retest_worthy_failure_above_clean_no_edge(
    patched_planner_registry,
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        planner_mod,
        "load_event_priority_weights",
        lambda _path: {"EVENT_A": 1.0, "EVENT_B": 1.0},
    )
    _write_tested_regions(
        tmp_path,
        [
            {"event_type": "EVENT_A", "failure_cause_class": "insufficient_sample"},
            {"event_type": "EVENT_B", "failure_cause_class": "market"},
        ],
    )

    plan = _planner(tmp_path).plan()

    assert plan.ranked_proposals[0].event_type == "EVENT_A"
    retest_rationale = plan.ranked_proposals[0].rationale["failure_penalty"]
    no_edge_rationale = plan.ranked_proposals[1].rationale["failure_penalty"]
    assert retest_rationale["insufficient_sample"] > 0
    assert retest_rationale["retest_bonus"] > 0
    assert retest_rationale["total_penalty"] < no_edge_rationale["total_penalty"]
    assert no_edge_rationale["market"] > 0


def test_campaign_planner_downgrades_mechanical_failure_below_clean_lower_priority_event(
    patched_planner_registry,
    tmp_path: Path,
) -> None:
    _write_tested_regions(
        tmp_path,
        [{"event_type": "EVENT_A", "failure_cause_class": "mechanical"}],
    )

    plan = _planner(tmp_path).plan()

    assert plan.ranked_proposals[0].event_type == "EVENT_B"
    mechanical = next(
        proposal for proposal in plan.ranked_proposals if proposal.event_type == "EVENT_A"
    )
    assert mechanical.rationale["failure_penalty"]["mechanical"] == 3.0
    assert mechanical.rationale["history_penalty"] == 3.0
    selection = plan.summary["selection_rationale"]
    assert selection["selected_event_type"] == "EVENT_B"
    assert selection["runner_up_event_type"] == "EVENT_A"
    assert selection["runner_up_dominant_penalties"][0]["factor"] == "history_penalty"


def test_campaign_planner_blocks_surface_dead_events_and_marks_generated_profile(
    patched_planner_registry,
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        planner_mod,
        "analyze_feature_surface_viability",
        lambda **kwargs: {
            "status": "warn",
            "detectors": {
                "EVENT_A": {
                    "status": "block",
                    "blocking_columns": ["oi_notional"],
                    "blocked_symbols": ["BTCUSDT"],
                },
                "EVENT_B": {"status": "pass", "blocking_columns": [], "blocked_symbols": []},
            },
        },
    )

    plan = _planner(tmp_path, horizon_bars=(12, 24), directions=("long", "short")).plan()

    assert [proposal.event_type for proposal in plan.ranked_proposals] == ["EVENT_B"]
    assert plan.summary["surface_blocked_events"][0]["event_type"] == "EVENT_A"
    proposal = plan.ranked_proposals[0].proposal
    assert proposal["discovery_profile"] == "exploratory"
    assert proposal["phase2_gate_profile"] == "discovery"
    assert plan.ranked_proposals[0].rationale["surface_viability"]["status"] == "pass"


def test_campaign_planner_prefers_positive_post_cost_history_over_raw_priority(
    patched_planner_registry,
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        planner_mod,
        "load_event_priority_weights",
        lambda _path: {"EVENT_A": 4.0, "EVENT_B": 1.0},
    )
    _write_event_statistics(
        tmp_path,
        [
            {
                "event_type": "EVENT_A",
                "runs_tested": 6,
                "times_evaluated": 6,
                "times_promoted": 0,
                "avg_q_value": 0.82,
                "avg_after_cost_expectancy": -7.5,
                "dominant_fail_gate": "gate_after_cost_positive",
            },
            {
                "event_type": "EVENT_B",
                "runs_tested": 6,
                "times_evaluated": 6,
                "times_promoted": 4,
                "avg_q_value": 0.08,
                "avg_after_cost_expectancy": 6.5,
                "dominant_fail_gate": "",
            },
        ],
    )

    plan = _planner(tmp_path).plan()

    assert plan.ranked_proposals[0].event_type == "EVENT_B"
    top = plan.ranked_proposals[0]
    runner_up = plan.ranked_proposals[1]
    assert top.rationale["economics_signal"]["status"] == "positive"
    assert top.rationale["score_components"]["economics_score"] > 0
    assert runner_up.rationale["economics_signal"]["status"] == "negative"
    assert runner_up.rationale["score_components"]["economics_score"] < 0
    assert plan.summary["selection_rationale"]["dominant_positive_factors"][0]["factor"] in {
        "economics_score",
        "family_gap_score",
        "event_gap_score",
        "maturity_bonus",
    }


def test_campaign_planner_keeps_economics_neutral_without_event_statistics(
    patched_planner_registry,
    tmp_path: Path,
) -> None:
    plan = _planner(tmp_path).plan()

    top = plan.ranked_proposals[0]
    assert top.rationale["economics_signal"]["status"] == "unknown"
    assert top.rationale["score_components"]["economics_score"] == 0.0



def test_event_economics_signals_prefer_recent_stressed_resilience() -> None:
    signals = planner_mod._event_economics_signals(
        pd.DataFrame(
            [
                {
                    "event_type": "EVENT_A",
                    "times_evaluated": 6,
                    "times_promoted": 1,
                    "avg_q_value": 0.18,
                    "avg_after_cost_expectancy": 1.0,
                    "median_after_cost_expectancy": 1.2,
                    "recent_after_cost_expectancy": 3.5,
                    "avg_stressed_after_cost_expectancy": 0.8,
                    "median_stressed_after_cost_expectancy": 1.0,
                    "recent_stressed_after_cost_expectancy": 2.8,
                    "positive_after_cost_rate": 0.83,
                    "positive_stressed_after_cost_rate": 0.83,
                    "tradable_rate": 0.9,
                    "statistical_pass_rate": 0.75,
                    "dominant_fail_gate": "",
                },
                {
                    "event_type": "EVENT_B",
                    "times_evaluated": 6,
                    "times_promoted": 1,
                    "avg_q_value": 0.18,
                    "avg_after_cost_expectancy": 1.0,
                    "median_after_cost_expectancy": 1.2,
                    "recent_after_cost_expectancy": -1.5,
                    "avg_stressed_after_cost_expectancy": -0.8,
                    "median_stressed_after_cost_expectancy": -1.0,
                    "recent_stressed_after_cost_expectancy": -2.8,
                    "positive_after_cost_rate": 0.45,
                    "positive_stressed_after_cost_rate": 0.15,
                    "tradable_rate": 0.3,
                    "statistical_pass_rate": 0.2,
                    "dominant_fail_gate": "gate_promo_retail_net_expectancy",
                },
            ]
        )
    )

    assert signals["EVENT_A"]["score"] > signals["EVENT_B"]["score"]
    assert signals["EVENT_A"]["stressed_component"] > 0.0
    assert signals["EVENT_B"]["cost_drag"] > signals["EVENT_A"]["cost_drag"]
