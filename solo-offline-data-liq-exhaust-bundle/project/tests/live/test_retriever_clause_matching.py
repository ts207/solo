from __future__ import annotations

from project.domain.models import ThesisDefinition
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


def _canonical_confirm_thesis() -> PromotedThesis:
    return PromotedThesis(
        thesis_id="thesis::shadow::cand_confirm",
        status="active",
        symbol_scope={
            "mode": "single_symbol",
            "symbols": ["BTCUSDT"],
            "candidate_symbol": "BTCUSDT",
        },
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_family="IGNORED_FALLBACK",
        canonical_regime="VOLATILITY_TRANSITION",
        event_side="both",
        required_context={},
        supportive_context={"has_realized_oos_path": True},
        expected_response={},
        invalidation={},
        freshness_policy={"allowed_staleness_classes": ["fresh", "watch"]},
        risk_notes=[],
        evidence=ThesisEvidence(
            sample_size=40,
            validation_samples=20,
            test_samples=20,
            estimate_bps=90.0,
            net_expectancy_bps=84.0,
            q_value=0.02,
            stability_score=0.8,
            rank_score=3.0,
        ),
        lineage=ThesisLineage(
            run_id="shadow",
            candidate_id="THESIS_VOL_SHOCK_LIQUIDITY_CONFIRM",
        ),
        governance=ThesisGovernance(trade_trigger_eligible=True),
        requirements=ThesisRequirements(
            trigger_events=["VOL_SHOCK"], confirmation_events=["LIQUIDITY_VACUUM"]
        ),
    )


def test_retriever_uses_canonical_confirmation_clause_when_present() -> None:
    store = ThesisStore([_canonical_confirm_thesis()])
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
        active_event_families=["VOL_SHOCK"],
        active_event_ids=["VOL_SHOCK"],
        active_episode_ids=[],
    )

    match = retrieve_ranked_theses(
        thesis_store=store, context=context, include_pending=False, limit=1
    )[0]

    assert match.eligibility_passed is True
    assert "trigger_clause_match:VOL_SHOCK" in match.reasons_for
    assert "canonical_regime_match:VOLATILITY_TRANSITION" in match.reasons_for
    assert "confirmation_missing:LIQUIDITY_VACUUM" in match.reasons_against


def test_retriever_matches_trigger_without_family_metadata() -> None:
    store = ThesisStore([_canonical_confirm_thesis()])
    context = LiveTradeContext(
        timestamp="2026-04-02T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_family="",
        canonical_regime="VOLATILITY_TRANSITION",
        event_side="long",
        live_features={},
        regime_snapshot={"canonical_regime": "VOLATILITY_TRANSITION"},
        execution_env={},
        portfolio_state={},
        active_event_families=[],
        active_event_ids=["VOL_SHOCK", "LIQUIDITY_VACUUM"],
        active_episode_ids=[],
    )

    match = retrieve_ranked_theses(
        thesis_store=store, context=context, include_pending=False, limit=1
    )[0]

    assert match.eligibility_passed is True
    assert "trigger_clause_match:VOL_SHOCK" in match.reasons_for
    assert "confirmation_match:LIQUIDITY_VACUUM" in match.reasons_for


def test_retriever_rejects_family_only_trigger_match_when_ids_disagree() -> None:
    store = ThesisStore([_canonical_confirm_thesis()])
    context = LiveTradeContext(
        timestamp="2026-04-02T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        primary_event_id="OTHER_EVENT",
        event_family="IGNORED_FALLBACK",
        canonical_regime="VOLATILITY_TRANSITION",
        event_side="long",
        live_features={},
        regime_snapshot={"canonical_regime": "VOLATILITY_TRANSITION"},
        execution_env={},
        portfolio_state={},
        active_event_families=["IGNORED_FALLBACK"],
        active_event_ids=["OTHER_EVENT"],
        active_episode_ids=[],
    )

    match = retrieve_ranked_theses(
        thesis_store=store, context=context, include_pending=False, limit=1
    )[0]

    assert match.eligibility_passed is False
    assert "trigger_clause_missing:VOL_SHOCK,LIQUIDITY_VACUUM" not in match.reasons_against
    assert "trigger_clause_missing:VOL_SHOCK" in match.reasons_against


def test_retriever_rejects_required_context_mismatch() -> None:
    thesis = _canonical_confirm_thesis().model_copy(
        update={"required_context": {"symbol": "ETHUSDT"}}
    )
    store = ThesisStore([thesis])
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
        active_event_ids=["VOL_SHOCK", "LIQUIDITY_VACUUM"],
        active_episode_ids=[],
    )

    match = retrieve_ranked_theses(
        thesis_store=store, context=context, include_pending=False, limit=1
    )[0]

    assert match.eligibility_passed is False
    assert "required_context_mismatch:symbol" in match.reasons_against


def test_retriever_enforces_required_state_ids() -> None:
    thesis = _canonical_confirm_thesis().model_copy(
        update={
            "required_state_ids": ["STATE_A"],
            "supportive_state_ids": ["STATE_B"],
        }
    )
    store = ThesisStore([thesis])
    base_context = LiveTradeContext(
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
        active_event_ids=["VOL_SHOCK", "LIQUIDITY_VACUUM"],
        active_episode_ids=[],
    )

    missing = retrieve_ranked_theses(
        thesis_store=store,
        context=base_context,
        include_pending=False,
        limit=1,
    )[0]
    matched = retrieve_ranked_theses(
        thesis_store=store,
        context=base_context.model_copy(update={"active_state_ids": ["STATE_A", "STATE_B"]}),
        include_pending=False,
        limit=1,
    )[0]

    assert missing.eligibility_passed is False
    assert "required_state_missing:STATE_A" in missing.reasons_against
    assert matched.eligibility_passed is True
    assert "required_state_match:STATE_A" in matched.reasons_for
    assert "supportive_state_match:STATE_B" in matched.reasons_for


def test_retriever_rejects_stale_thesis_when_policy_disallows_it() -> None:
    thesis = _canonical_confirm_thesis().model_copy(
        update={
            "staleness_class": "stale",
            "freshness_policy": {"allowed_staleness_classes": ["fresh"]},
        }
    )
    store = ThesisStore([thesis])
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
        active_event_ids=["VOL_SHOCK", "LIQUIDITY_VACUUM"],
        active_episode_ids=[],
    )

    match = retrieve_ranked_theses(
        thesis_store=store, context=context, include_pending=False, limit=1
    )[0]

    assert match.eligibility_passed is False
    assert "freshness_disallowed:stale" in match.reasons_against


def test_retriever_hard_fails_on_invalidation_trigger() -> None:
    thesis = _canonical_confirm_thesis().model_copy(
        update={"invalidation": {"metric": "adverse_proxy", "operator": ">", "value": 0.02}}
    )
    store = ThesisStore([thesis])
    context = LiveTradeContext(
        timestamp="2026-04-02T00:00:00Z",
        symbol="BTCUSDT",
        timeframe="5m",
        primary_event_id="VOL_SHOCK",
        event_family="VOL_SHOCK",
        canonical_regime="VOLATILITY_TRANSITION",
        event_side="long",
        live_features={"adverse_proxy": 0.03},
        regime_snapshot={"canonical_regime": "VOLATILITY_TRANSITION"},
        execution_env={},
        portfolio_state={},
        active_event_ids=["VOL_SHOCK"],
        active_episode_ids=[],
    )

    match = retrieve_ranked_theses(
        thesis_store=store, context=context, include_pending=False, limit=1
    )[0]

    assert match.eligibility_passed is False
    assert "invalidation_triggered" in match.reasons_against


def test_retriever_hard_fails_on_disallowed_regime() -> None:
    thesis = _canonical_confirm_thesis().model_copy(
        update={
            "requirements": ThesisRequirements(
                trigger_events=["VOL_SHOCK"],
                confirmation_events=["LIQUIDITY_VACUUM"],
                disallowed_regimes=["VOLATILITY_TRANSITION"],
            )
        }
    )
    store = ThesisStore([thesis])
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
        active_event_ids=["VOL_SHOCK", "LIQUIDITY_VACUUM"],
        active_episode_ids=[],
    )

    match = retrieve_ranked_theses(
        thesis_store=store, context=context, include_pending=False, limit=1
    )[0]

    assert match.eligibility_passed is False
    assert "regime_disallowed:VOLATILITY_TRANSITION" in match.reasons_against


def test_retriever_blocks_non_live_deployment_state_in_trading_mode() -> None:
    thesis = _canonical_confirm_thesis().model_copy(update={"deployment_state": "paper_only"})
    store = ThesisStore([thesis])
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
        execution_env={"runtime_mode": "trading"},
        portfolio_state={},
        active_event_ids=["VOL_SHOCK", "LIQUIDITY_VACUUM"],
        active_episode_ids=[],
    )

    match = retrieve_ranked_theses(
        thesis_store=store, context=context, include_pending=False, limit=1
    )[0]

    assert match.eligibility_passed is False
    assert "deployment_state_blocked:paper_only" in match.reasons_against


def test_retriever_prefers_exported_deployment_state_over_authored_definition(
    monkeypatch,
) -> None:
    thesis = _canonical_confirm_thesis().model_copy(update={"deployment_state": "live_enabled"})
    store = ThesisStore([thesis])
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
        execution_env={"runtime_mode": "trading"},
        portfolio_state={},
        active_event_ids=["VOL_SHOCK", "LIQUIDITY_VACUUM"],
        active_episode_ids=[],
    )
    monkeypatch.setattr(
        "project.live.retriever.resolve_promoted_thesis_definition",
        lambda thesis: ThesisDefinition(
            thesis_id=thesis.thesis_id,
            thesis_kind="runtime_contract",
            event_family="VOL_SHOCK",
            timeframe="5m",
            primary_event_id="VOL_SHOCK",
            deployment_state="paper_only",
        ),
    )

    match = retrieve_ranked_theses(
        thesis_store=store, context=context, include_pending=False, limit=1
    )[0]

    assert match.eligibility_passed is True
    assert "deployment_state:live_enabled" in match.reasons_for
    assert "deployment_state_blocked:paper_only" not in match.reasons_against


def test_retriever_supportive_context_boosts_score_without_gating() -> None:
    boosted = _canonical_confirm_thesis().model_copy(
        update={
            "thesis_id": "thesis::shadow::boosted",
            "supportive_context": {
                "bridge_certified": True,
                "has_realized_oos_path": True,
            },
        }
    )
    plain = _canonical_confirm_thesis().model_copy(
        update={
            "thesis_id": "thesis::shadow::plain",
            "supportive_context": {"has_realized_oos_path": False},
            "evidence": ThesisEvidence(
                sample_size=40,
                validation_samples=20,
                test_samples=20,
                estimate_bps=90.0,
                net_expectancy_bps=84.0,
                q_value=0.02,
                stability_score=0.8,
                rank_score=3.0,
            ),
        }
    )
    store = ThesisStore([plain, boosted])
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
        active_event_ids=["VOL_SHOCK", "LIQUIDITY_VACUUM"],
        active_episode_ids=[],
    )

    matches = retrieve_ranked_theses(
        thesis_store=store, context=context, include_pending=False, limit=2
    )

    assert matches[0].thesis.thesis_id == "thesis::shadow::boosted"
    assert "supportive_context:bridge_certified" in matches[0].reasons_for
    assert matches[1].eligibility_passed is True


def test_retriever_suppresses_lower_ranked_overlap_group_match() -> None:
    winner = _canonical_confirm_thesis().model_copy(
        update={
            "thesis_id": "thesis::shadow::winner",
            "governance": ThesisGovernance(
                overlap_group_id="grp_overlap", trade_trigger_eligible=True
            ),
        }
    )
    loser = _canonical_confirm_thesis().model_copy(
        update={
            "thesis_id": "thesis::shadow::loser",
            "evidence": ThesisEvidence(
                sample_size=20,
                validation_samples=10,
                test_samples=10,
                estimate_bps=50.0,
                net_expectancy_bps=45.0,
                q_value=0.05,
                stability_score=0.5,
                rank_score=1.0,
            ),
            "governance": ThesisGovernance(
                overlap_group_id="grp_overlap", trade_trigger_eligible=True
            ),
        }
    )
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
        active_event_ids=["VOL_SHOCK", "LIQUIDITY_VACUUM"],
        active_episode_ids=[],
    )

    matches = retrieve_ranked_theses(
        thesis_store=store, context=context, include_pending=False, limit=2
    )

    assert matches[0].thesis.thesis_id == "thesis::shadow::winner"
    assert matches[1].eligibility_passed is False
    assert "overlap_suppressed:grp_overlap:thesis::shadow::winner" in matches[1].reasons_against
