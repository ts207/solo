from __future__ import annotations

from project.live.contracts import PromotedThesis, ThesisEvidence, ThesisLineage
from project.live.contracts.live_trade_context import LiveTradeContext
from project.live.replay import replay_contexts
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
        supportive_context={"canonical_regime": "VOLATILITY", "has_realized_oos_path": True},
        expected_response={"direction": "long"},
        invalidation={"metric": "adverse_proxy", "operator": ">", "value": 0.02},
        risk_notes=[],
        evidence=ThesisEvidence(
            sample_size=120,
            validation_samples=60,
            test_samples=60,
            estimate_bps=10.0,
            net_expectancy_bps=7.0,
            q_value=0.01,
            stability_score=0.8,
            cost_survival_ratio=1.0,
            tob_coverage=0.95,
            rank_score=1.0,
        ),
        lineage=ThesisLineage(run_id="run_1", candidate_id="cand_1", blueprint_id="bp_1"),
    )
    return ThesisStore([thesis], run_id="run_1")


def test_replay_acceptance_tracks_action_funnel() -> None:
    contexts = [
        LiveTradeContext(
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
        ),
        LiveTradeContext(
            timestamp="2026-03-30T00:05:00Z",
            symbol="BTCUSDT",
            timeframe="5m",
            primary_event_id="VOL_SHOCK",
            event_family="VOL_SHOCK",
            event_side="long",
            live_features={"spread_bps": 8.0, "depth_usd": 5000.0, "tob_coverage": 0.5},
            regime_snapshot={"canonical_regime": "VOLATILITY"},
            execution_env={"runtime_mode": "monitor_only"},
            portfolio_state={"available_balance": 1000.0},
        ),
    ]

    result = replay_contexts(thesis_store=_store(), contexts=contexts, include_pending=False)

    assert result.contexts_evaluated == 2
    assert sum(result.action_counts.values()) == 2
    assert any(intent.action in {"probe", "trade_small", "trade_normal"} for intent in result.intents)
