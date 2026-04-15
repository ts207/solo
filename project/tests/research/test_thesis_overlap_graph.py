from __future__ import annotations

from project.live.contracts import (
    PromotedThesis,
    ThesisEvidence,
    ThesisGovernance,
    ThesisLineage,
    ThesisRequirements,
    ThesisSource,
)
from project.portfolio.thesis_overlap import (
    build_thesis_overlap_graph,
    overlap_group_id_for_thesis,
)


def _thesis(thesis_id: str, *, event_family: str = "VOL_SHOCK", episode: str = "EP_VOL_BREAKOUT") -> PromotedThesis:
    thesis = PromotedThesis(
        thesis_id=thesis_id,
        status="active",
        symbol_scope={"mode": "single_symbol", "symbols": ["BTCUSDT"], "candidate_symbol": "BTCUSDT"},
        timeframe="5m",
        primary_event_id=event_family,
        event_family=event_family,
        event_side="long",
        required_context={"symbol": "BTCUSDT"},
        supportive_context={"canonical_regime": "VOLATILITY"},
        expected_response={"direction": "long"},
        invalidation={"metric": "adverse_proxy", "operator": ">", "value": 0.02},
        risk_notes=[],
        evidence=ThesisEvidence(sample_size=120, rank_score=0.8, stability_score=0.9),
        lineage=ThesisLineage(run_id="run1", candidate_id=thesis_id),
        governance=ThesisGovernance(tier="A", operational_role="trigger", trade_trigger_eligible=True),
        requirements=ThesisRequirements(required_episodes=[episode]),
        source=ThesisSource(event_contract_ids=[event_family], episode_contract_ids=[episode]),
    )
    return thesis


def test_overlap_group_id_is_stable() -> None:
    thesis = _thesis("thesis::1")
    group_id = overlap_group_id_for_thesis(thesis)
    assert group_id == "VOL_SHOCK::EP_VOL_BREAKOUT::VOLATILITY::trigger"


def test_build_thesis_overlap_graph_emits_edges_and_groups() -> None:
    t1 = _thesis("thesis::1")
    t2 = _thesis("thesis::2")
    t3 = _thesis("thesis::3", event_family="LIQUIDITY_VACUUM", episode="EP_LIQUIDITY_SHOCK")

    payload = build_thesis_overlap_graph([t1, t2, t3])

    assert payload["schema_version"] == "thesis_overlap_graph_v1"
    assert payload["thesis_count"] == 3
    assert payload["overlap_group_count"] >= 2
    assert any(edge["source"] == "thesis::1" and edge["target"] == "thesis::2" for edge in payload["edges"])
    assert all(node["overlap_group_id"] for node in payload["nodes"])
    assert payload["nodes"][0]["primary_event_id"] in {"LIQUIDITY_VACUUM", "VOL_SHOCK"}
    assert "compat_event_family" in payload["nodes"][0]
