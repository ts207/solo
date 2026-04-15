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


def _store() -> ThesisStore:
    thesis = PromotedThesis(
        thesis_id="thesis::run_2::cand_2",
        status="active",
        symbol_scope={"mode": "single_symbol", "symbols": ["BTCUSDT"], "candidate_symbol": "BTCUSDT"},
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_family="VOL_SHOCK",
        event_side="long",
        required_context={"symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
        supportive_context={"canonical_regime": "VOLATILITY", "has_realized_oos_path": True},
        expected_response={"direction": "long"},
        invalidation={"metric": "spread_bps", "operator": ">", "value": 10.0},
        risk_notes=[],
        evidence=ThesisEvidence(
            sample_size=150,
            validation_samples=75,
            test_samples=75,
            estimate_bps=14.0,
            net_expectancy_bps=10.0,
            q_value=0.01,
            stability_score=0.9,
            cost_survival_ratio=1.0,
            tob_coverage=0.95,
            rank_score=1.0,
            promotion_track="deploy",
            policy_version="v1",
            bundle_version="b1",
        ),
        lineage=ThesisLineage(run_id="run_2", candidate_id="cand_2", blueprint_id="bp_2"),
        governance=ThesisGovernance(
            tier="A",
            operational_role="trigger",
            deployment_disposition="primary_trigger_candidate",
            evidence_mode="direct",
            trade_trigger_eligible=True,
            requires_stronger_evidence=False,
        ),
        requirements=ThesisRequirements(
            trigger_events=["VOL_SHOCK"],
            confirmation_events=["VOL_SPIKE"],
            required_episodes=["EP_LIQUIDITY_SHOCK"],
            disallowed_regimes=["CALM"],
        ),
        source=ThesisSource(event_contract_ids=["VOL_SHOCK"], episode_contract_ids=["EP_LIQUIDITY_SHOCK"]),
    )
    return ThesisStore([thesis], run_id="run_2")


def test_episode_requirement_enables_trade_when_context_matches() -> None:
    context = LiveTradeContext(
        timestamp="2026-03-30T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_family="VOL_SHOCK",
        event_side="long",
        live_features={"spread_bps": 3.0, "depth_usd": 100000.0, "tob_coverage": 0.95},
        regime_snapshot={"canonical_regime": "VOLATILITY"},
        execution_env={"runtime_mode": "monitor_only"},
        portfolio_state={"available_balance": 1000.0},
        active_event_families=["VOL_SHOCK", "VOL_SPIKE"],
        active_episode_ids=["EP_LIQUIDITY_SHOCK"],
    )

    outcome = decide_trade_intent(context=context, thesis_store=_store(), include_pending=False)

    assert outcome.trade_intent.action in {"trade_small", "trade_normal"}
    assert any(reason.startswith("required_episode_match:") for reason in outcome.trade_intent.reasons_for)


def test_episode_requirement_blocks_trade_when_missing() -> None:
    context = LiveTradeContext(
        timestamp="2026-03-30T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_family="VOL_SHOCK",
        event_side="long",
        live_features={"spread_bps": 3.0, "depth_usd": 100000.0, "tob_coverage": 0.95},
        regime_snapshot={"canonical_regime": "VOLATILITY"},
        execution_env={"runtime_mode": "monitor_only"},
        portfolio_state={"available_balance": 1000.0},
        active_event_families=["VOL_SHOCK"],
        active_episode_ids=[],
    )

    outcome = decide_trade_intent(context=context, thesis_store=_store(), include_pending=False)

    assert outcome.trade_intent.action == "reject"
    assert any(reason.startswith("required_episode_missing:") for reason in outcome.trade_intent.reasons_against)
