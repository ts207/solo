from __future__ import annotations

from project.live.contracts import (
    PromotedThesis,
    ThesisEvidence,
    ThesisGovernance,
    ThesisLineage,
    ThesisRequirements,
)
from project.live.contracts.live_trade_context import LiveTradeContext
from project.live.retriever import retrieve_ranked_theses
from project.live.thesis_store import ThesisStore


def _thesis_with_overlap(thesis_id: str, group_id: str, sample_size: int, rank_score: float) -> PromotedThesis:
    return PromotedThesis(
        thesis_id=thesis_id,
        status="active",
        lineage=ThesisLineage(run_id="test", candidate_id=thesis_id),
        symbol_scope={
            "mode": "single_symbol",
            "symbols": ["BTCUSDT"],
            "candidate_symbol": "BTCUSDT",
        },
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_family="VOL_SHOCK",
        canonical_regime="VOLATILITY_TRANSITION",
        event_side="both",
        required_context={},
        supportive_context={"has_realized_oos_path": True},
        expected_response={},
        invalidation={},
        freshness_policy={"allowed_staleness_classes": ["fresh"]},
        risk_notes=[],
        evidence=ThesisEvidence(
            sample_size=sample_size,
            validation_samples=sample_size // 2,
            test_samples=sample_size // 2,
            estimate_bps=90.0,
            net_expectancy_bps=84.0,
            q_value=0.02,
            stability_score=0.8,
            rank_score=rank_score,
        ),
        governance=ThesisGovernance(overlap_group_id=group_id, trade_trigger_eligible=True),
        requirements=ThesisRequirements(trigger_events=["VOL_SHOCK"]),
    )


def test_retriever_overlap_suppression_picks_one_winner() -> None:
    winner = _thesis_with_overlap("thesis::winner", "grp_overlap", 100, 3.0)
    loser = _thesis_with_overlap("thesis::loser", "grp_overlap", 50, 1.0)

    store = ThesisStore([loser, winner])
    context = LiveTradeContext(
        timestamp="2026-04-02T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_family="VOL_SHOCK",
        canonical_regime="VOLATILITY_TRANSITION",
        event_side="long",
        live_features={},
        regime_snapshot={"canonical_regime": "VOLATILITY_TRANSITION"},
        execution_env={},
        portfolio_state={},
        active_event_ids=["VOL_SHOCK"],
    )

    matches = retrieve_ranked_theses(thesis_store=store, context=context, include_pending=False, limit=10)

    # Winner should be eligible, loser should be suppressed
    eligible_ids = [m.thesis.thesis_id for m in matches if m.eligibility_passed]
    suppressed_ids = [m.thesis.thesis_id for m in matches if not m.eligibility_passed]

    assert "thesis::winner" in eligible_ids
    assert "thesis::loser" in suppressed_ids
    assert any("overlap_suppressed:grp_overlap:thesis::winner" in m.reasons_against for m in matches if m.thesis.thesis_id == "thesis::loser")


def test_retriever_overlap_suppression_honors_active_groups() -> None:
    # If the group is already active in context, ALL new candidates from that group should be suppressed
    candidate = _thesis_with_overlap("thesis::candidate", "grp_active", 100, 3.0)

    store = ThesisStore([candidate])
    context = LiveTradeContext(
        timestamp="2026-04-02T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_family="VOL_SHOCK",
        canonical_regime="VOLATILITY_TRANSITION",
        event_side="long",
        live_features={},
        regime_snapshot={"canonical_regime": "VOLATILITY_TRANSITION"},
        execution_env={},
        portfolio_state={},
        active_event_ids=["VOL_SHOCK"],
        active_groups={"grp_active"},
    )

    matches = retrieve_ranked_theses(thesis_store=store, context=context, include_pending=False, limit=10)

    assert matches[0].thesis.thesis_id == "thesis::candidate"
    assert matches[0].eligibility_passed is False
    assert "overlap_suppressed:grp_active" in matches[0].reasons_against[0]
