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
