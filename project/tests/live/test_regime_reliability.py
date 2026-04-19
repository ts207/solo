from __future__ import annotations

from project.live.contracts import LiveTradeContext, PromotedThesis, ThesisEvidence, ThesisLineage
from project.live.regime_reliability import evaluate_regime_reliability


def test_regime_reliability_drops_when_thesis_regime_support_weakens() -> None:
    thesis = PromotedThesis(
        thesis_id="thesis_regime",
        timeframe="5m",
        primary_event_id="EVENT_A",
        canonical_regime="VOL",
        event_side="long",
        evidence=ThesisEvidence(sample_size=100, stability_score=0.8),
        lineage=ThesisLineage(run_id="run_1", candidate_id="cand_1"),
    )
    matching = LiveTradeContext(
        timestamp="2026-04-10T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        primary_event_id="EVENT_A",
        canonical_regime="VOL",
        event_side="long",
    )
    mismatch = matching.model_copy(update={"canonical_regime": "TREND"})

    assert (
        evaluate_regime_reliability(thesis=thesis, context=mismatch).reliability
        < evaluate_regime_reliability(thesis=thesis, context=matching).reliability
    )
