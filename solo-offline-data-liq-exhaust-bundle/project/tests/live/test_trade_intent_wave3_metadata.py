from __future__ import annotations

from project.live.contracts import (
    PromotedThesis,
    ThesisEvidence,
    ThesisGovernance,
    ThesisLineage,
    ThesisRequirements,
    ThesisSource,
)
from project.live.contracts.live_trade_context import LiveTradeContext
from project.live.decision import decide_trade_intent
from project.live.thesis_store import ThesisStore


def test_decision_attaches_overlap_and_governance_metadata() -> None:
    thesis = PromotedThesis(
        thesis_id="thesis::run_1::cand_1",
        status="active",
        symbol_scope={"mode": "single_symbol", "symbols": ["BTCUSDT"], "candidate_symbol": "BTCUSDT"},
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_family="VOL_SHOCK",
        event_side="long",
        required_context={"symbol": "BTCUSDT"},
        supportive_context={"canonical_regime": "VOLATILITY", "bridge_certified": True, "has_realized_oos_path": True},
        expected_response={"direction": "long", "stop_value": 0.01},
        invalidation={"metric": "adverse_proxy", "operator": ">", "value": 0.02},
        risk_notes=[],
        evidence=ThesisEvidence(sample_size=120, rank_score=0.8, stability_score=0.9, estimate_bps=10.0, net_expectancy_bps=7.0),
        lineage=ThesisLineage(run_id="run_1", candidate_id="cand_1"),
        governance=ThesisGovernance(tier="A", operational_role="trigger", overlap_group_id="grp_a", trade_trigger_eligible=True),
        requirements=ThesisRequirements(trigger_events=["VOL_SHOCK"], required_episodes=["EP_VOL_BREAKOUT"]),
        source=ThesisSource(event_contract_ids=["VOL_SHOCK"], episode_contract_ids=["EP_VOL_BREAKOUT"]),
    )
    store = ThesisStore([thesis])
    context = LiveTradeContext(
        timestamp="2026-03-31T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        event_family="VOL_SHOCK",
        primary_event_id="VOL_SHOCK",
        canonical_regime="VOLATILITY",
        event_side="long",
        live_features={"adverse_proxy": 0.0},
        regime_snapshot={"canonical_regime": "VOLATILITY"},
        execution_env={},
        portfolio_state={"available_balance": 1000.0},
        active_event_families=["VOL_SHOCK"],
        active_event_ids=["VOL_SHOCK"],
        active_episode_ids=["EP_VOL_BREAKOUT"],
    )

    outcome = decide_trade_intent(context=context, thesis_store=store, include_pending=False)

    assert outcome.trade_intent.metadata["overlap_group_id"] == "grp_a"
    assert outcome.trade_intent.metadata["governance_tier"] == "A"
    assert outcome.trade_intent.metadata["operational_role"] == "trigger"
    assert outcome.trade_intent.metadata["active_episode_ids"] == ["EP_VOL_BREAKOUT"]
    assert outcome.trade_intent.metadata["primary_event_id"] == "VOL_SHOCK"
    assert outcome.trade_intent.metadata["canonical_regime"] == "VOLATILITY"
    assert outcome.trade_intent.metadata["thesis_canonical_regime"] == "VOLATILITY"
