from __future__ import annotations

from project.live.contracts import LiveTradeContext, PromotedThesis, ThesisEvidence, ThesisLineage
from project.live.retriever import ThesisMatch
from project.live.scoring import build_decision_score


def test_build_decision_score_treats_zero_q_value_as_strong_evidence():
    thesis = PromotedThesis(
        thesis_id="thesis_1",
        timeframe="5m",
        primary_event_id="EVENT_A",
        event_side="long",
        evidence=ThesisEvidence(
            sample_size=150,
            q_value=0.0,
            net_expectancy_bps=12.0,
        ),
        lineage=ThesisLineage(run_id="run_1", candidate_id="cand_1"),
    )
    match = ThesisMatch(
        thesis=thesis,
        eligibility_passed=True,
        support_score=0.9,
        contradiction_penalty=0.0,
        reasons_for=["episode_match:test"],
        reasons_against=[],
    )
    context = LiveTradeContext(
        timestamp="2026-04-10T12:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        primary_event_id="EVENT_A",
        event_side="long",
        live_features={"spread_bps": 1.0, "depth_usd": 100_000.0, "tob_coverage": 0.95},
    )

    score = build_decision_score(match, context)

    assert "q_value_ok" in score.reasons_for
    assert "q_value_weak" not in score.reasons_against
