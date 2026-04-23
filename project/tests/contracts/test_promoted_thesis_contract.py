from __future__ import annotations

from project.live.contracts import PromotedThesis, ThesisEvidence, ThesisLineage, ThesisRequirements


def test_promoted_thesis_contract_model_dump() -> None:
    thesis = PromotedThesis(
        thesis_id="thesis::run_1::cand_1",
        status="active",
        symbol_scope={
            "mode": "single_symbol",
            "symbols": ["BTCUSDT"],
            "candidate_symbol": "BTCUSDT",
        },
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_family="VOL_SHOCK",
        canonical_regime="volatility_transition",
        event_side="long",
        required_context={"symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
        supportive_context={"canonical_regime": "VOLATILITY"},
        expected_response={"direction": "long", "net_expectancy_bps": 9.0},
        invalidation={"metric": "adverse_proxy", "operator": ">", "value": 0.02},
        risk_notes=["direction:long"],
        evidence=ThesisEvidence(
            sample_size=120,
            validation_samples=60,
            test_samples=60,
            estimate_bps=12.0,
            net_expectancy_bps=9.0,
            q_value=0.01,
            stability_score=0.9,
            cost_survival_ratio=1.0,
            tob_coverage=0.95,
            rank_score=1.0,
            promotion_track="deploy",
            policy_version="v1",
            bundle_version="b1",
        ),
        lineage=ThesisLineage(
            run_id="run_1",
            candidate_id="cand_1",
            hypothesis_id="hyp_1",
            plan_row_id="plan_1",
            blueprint_id="bp_1",
            proposal_id="proposal_1",
            source_discovery_mode="edge_cells",
            source_cell_id="cell_1",
            source_scoreboard_run_id="score_1",
            source_event_atom="vol_shock_core",
            source_context_cell="high_vol",
            source_contrast_lift_bps=8.0,
        ),
    )

    payload = thesis.model_dump()

    assert payload["promotion_class"] == "paper_promoted"
    assert payload["deployment_state"] == "paper_only"
    assert payload["evidence_gaps"] == []
    assert payload["status"] == "active"
    assert payload["timeframe"] == "5m"
    assert payload["primary_event_id"] == "VOL_SHOCK"
    assert payload["event_family"] == "VOL_SHOCK"
    assert payload["canonical_regime"] == "VOLATILITY_TRANSITION"
    assert payload["trigger_clause"] == {"events": []}
    assert payload["confirmation_clause"] == {"events": []}
    assert payload["invalidation_clause"]["metric"] == "adverse_proxy"
    assert payload["context_clause"]["event_type"] == "VOL_SHOCK"
    assert payload["overlap_group_id"] == ""
    assert payload["evidence"]["net_expectancy_bps"] == 9.0
    assert payload["lineage"]["blueprint_id"] == "bp_1"
    assert payload["lineage"]["source_discovery_mode"] == "edge_cells"
    assert payload["lineage"]["source_cell_id"] == "cell_1"
    assert payload["lineage"]["source_contrast_lift_bps"] == 8.0


def test_promoted_thesis_primary_event_id_does_not_backfill_event_family() -> None:
    thesis = PromotedThesis(
        thesis_id="thesis::run_1::cand_2",
        status="active",
        symbol_scope={"mode": "single_symbol", "symbols": ["BTCUSDT"], "candidate_symbol": "BTCUSDT"},
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_side="long",
        evidence=ThesisEvidence(sample_size=1),
        lineage=ThesisLineage(run_id="run_1", candidate_id="cand_2"),
    )

    assert thesis.primary_event_id == "VOL_SHOCK"
    assert thesis.event_family == ""


def test_promoted_thesis_uses_event_family_as_compatibility_fallback_only() -> None:
    thesis = PromotedThesis(
        thesis_id="thesis::run_1::cand_3",
        status="active",
        symbol_scope={"mode": "single_symbol", "symbols": ["BTCUSDT"], "candidate_symbol": "BTCUSDT"},
        timeframe="5m",
        event_family="VOL_SHOCK",
        event_side="long",
        requirements=ThesisRequirements(trigger_events=["VOL_SHOCK"]),
        evidence=ThesisEvidence(sample_size=1),
        lineage=ThesisLineage(run_id="run_1", candidate_id="cand_3"),
    )

    assert thesis.primary_event_id == "VOL_SHOCK"
    assert thesis.event_family == "VOL_SHOCK"
    assert thesis.trigger_clause == {"events": ["VOL_SHOCK"]}
