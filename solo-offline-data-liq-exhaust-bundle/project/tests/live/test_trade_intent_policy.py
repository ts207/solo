from __future__ import annotations

from project.live.contracts import PromotedThesis, ThesisEvidence, ThesisLineage
from project.live.contracts.live_trade_context import LiveTradeContext
from project.live.decision import decide_trade_intent
from project.live.thesis_store import ThesisStore


def _store() -> ThesisStore:
    thesis = PromotedThesis(
        thesis_id="thesis::run_1::cand_1",
        status="active",
        symbol_scope={"mode": "single_symbol", "symbols": ["BTCUSDT"], "candidate_symbol": "BTCUSDT"},
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_family="VOL_SHOCK",
        event_side="long",
        required_context={"symbol": "BTCUSDT", "event_type": "VOL_SHOCK"},
        supportive_context={
            "canonical_regime": "VOLATILITY",
            "bridge_certified": True,
            "has_realized_oos_path": True,
        },
        expected_response={"direction": "long"},
        invalidation={"metric": "adverse_proxy", "operator": ">", "value": 0.02},
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
        lineage=ThesisLineage(run_id="run_1", candidate_id="cand_1", blueprint_id="bp_1"),
    )
    return ThesisStore([thesis], run_id="run_1")


def test_decision_policy_emits_trade_small_for_strong_context() -> None:
    context = LiveTradeContext(
        timestamp="2026-03-30T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_family="VOL_SHOCK",
        event_side="long",
        live_features={"spread_bps": 2.0, "depth_usd": 100000.0, "tob_coverage": 0.95},
        regime_snapshot={"canonical_regime": "VOLATILITY"},
        execution_env={"runtime_mode": "monitor_only"},
        portfolio_state={"available_balance": 1000.0},
    )

    outcome = decide_trade_intent(context=context, thesis_store=_store(), include_pending=False)

    assert outcome.trade_intent.action in {"trade_small", "trade_normal"}
    assert outcome.trade_intent.side == "buy"
    assert outcome.trade_intent.thesis_id == "thesis::run_1::cand_1"


def test_decision_policy_rejects_when_no_match_exists() -> None:
    context = LiveTradeContext(
        timestamp="2026-03-30T00:00:00Z",
        symbol="ETHUSDT",
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_family="VOL_SHOCK",
        event_side="long",
        live_features={"spread_bps": 2.0, "depth_usd": 100000.0, "tob_coverage": 0.95},
        regime_snapshot={"canonical_regime": "VOLATILITY"},
        execution_env={"runtime_mode": "monitor_only"},
        portfolio_state={"available_balance": 1000.0},
    )

    outcome = decide_trade_intent(context=context, thesis_store=_store(), include_pending=False)

    assert outcome.trade_intent.action == "reject"
    assert outcome.trade_intent.reasons_against == ["no_matching_thesis"]
