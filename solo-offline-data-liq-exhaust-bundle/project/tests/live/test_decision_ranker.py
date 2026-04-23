from __future__ import annotations

from project.live.contracts import LiveTradeContext, PromotedThesis, ThesisEvidence, ThesisLineage
from project.live.decision_ranker import rank_decisions_by_expected_value
from project.live.retriever import ThesisMatch


def _match(thesis_id: str, *, support: float, net: float) -> ThesisMatch:
    thesis = PromotedThesis(
        thesis_id=thesis_id,
        timeframe="5m",
        primary_event_id="EVENT_A",
        canonical_regime="VOL",
        event_side="long",
        expected_response={"stop_value": 0.001},
        evidence=ThesisEvidence(
            sample_size=100,
            estimate_bps=net + 2.0,
            net_expectancy_bps=net,
            stability_score=0.8,
        ),
        lineage=ThesisLineage(run_id="run_1", candidate_id=thesis_id),
    )
    return ThesisMatch(
        thesis=thesis,
        eligibility_passed=True,
        support_score=support,
        contradiction_penalty=0.0,
        reasons_for=["trigger_clause_match:EVENT_A"],
        reasons_against=[],
    )


def test_decision_ranker_prioritizes_expected_net_edge_over_raw_support() -> None:
    context = LiveTradeContext(
        timestamp="2026-04-10T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        primary_event_id="EVENT_A",
        canonical_regime="VOL",
        event_side="long",
        live_features={"spread_bps": 1.0, "depth_usd": 100_000.0},
    )

    ranked = rank_decisions_by_expected_value(
        matches=[
            _match("high_support_low_edge", support=0.95, net=2.0),
            _match("lower_support_high_edge", support=0.55, net=20.0),
        ],
        context=context,
    )

    assert ranked[0].match.thesis.thesis_id == "lower_support_high_edge"
    assert ranked[0].utility_score > ranked[1].utility_score
