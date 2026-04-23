from __future__ import annotations

from project.live.contracts import LiveTradeContext, PromotedThesis, ThesisEvidence, ThesisLineage
from project.live.contradiction_model import assess_contradictions
from project.live.retriever import ThesisMatch


def test_contradiction_model_reduces_utility_inputs_not_just_raw_score() -> None:
    thesis = PromotedThesis(
        thesis_id="thesis_contra",
        timeframe="5m",
        primary_event_id="EVENT_A",
        event_side="long",
        evidence=ThesisEvidence(sample_size=100),
        lineage=ThesisLineage(run_id="run_1", candidate_id="cand_1"),
    )
    match = ThesisMatch(
        thesis=thesis,
        eligibility_passed=True,
        support_score=0.9,
        contradiction_penalty=0.2,
        reasons_for=[],
        reasons_against=["contradiction_event:EVENT_A"],
    )
    context = LiveTradeContext(
        timestamp="2026-04-10T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        primary_event_id="EVENT_A",
        event_side="long",
        contradiction_event_ids=["EVENT_A"],
    )

    assessment = assess_contradictions(match=match, context=context)

    assert assessment.penalty_bps > 0.0
    assert assessment.probability_penalty > 0.0
    assert "context_contradiction_events" in assessment.reasons
