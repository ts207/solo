from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from project.events.data_capabilities import load_data_capability_profile
from project.events.polarity import side_to_order_side
from project.live.contracts.live_trade_context import LiveTradeContext
from project.live.contracts.trade_intent import TradeIntent
from project.live.decision_ranker import rank_decisions_by_expected_value
from project.live.policy import score_to_action, thresholds_from_config
from project.live.retriever import ThesisMatch, retrieve_ranked_theses
from project.live.scoring import DecisionScore, build_decision_score
from project.live.thesis_store import ThesisStore


def _reject_context(*, context: LiveTradeContext, reason: str) -> "DecisionOutcome":
    reject = TradeIntent(action="reject", symbol=context.symbol, side="flat", thesis_id="", support_score=0.0, contradiction_penalty=0.0, confidence_band="none", size_fraction=0.0, reasons_for=[], reasons_against=[reason], metadata={"primary_event_id": str(context.primary_event_id or context.event_family), "canonical_regime": str(context.canonical_regime or context.regime_snapshot.get("canonical_regime", "")), "active_event_ids": list(context.active_event_ids), "trade_eligible": bool(context.trade_eligible), "data_quality_flag": str(context.data_quality_flag)})
    return DecisionOutcome(context=context, ranked_matches=[], top_score=None, trade_intent=reject)


def _runtime_mode(context: LiveTradeContext, policy_config: dict[str, Any] | None) -> str:
    return str(
        context.execution_env.get("runtime_mode")
        or context.execution_context.get("runtime_mode")
        or context.live_features.get("runtime_mode")
        or (policy_config or {}).get("runtime_mode")
        or ""
    ).strip().lower()


def _side_to_order_side(value: object) -> str:
    return side_to_order_side(value)


def _thesis_canonical_regime(thesis) -> str:
    return str(
        thesis.canonical_regime
        or (thesis.supportive_context or {}).get("canonical_regime", "")
    ).strip().upper()


def _resolve_trade_side(match: ThesisMatch, context: LiveTradeContext) -> str:
    thesis_side = _side_to_order_side(match.thesis.event_side)
    if thesis_side != "flat":
        return thesis_side
    return _side_to_order_side(context.event_side)


def _annotate_trade_intent(
    *,
    intent: TradeIntent,
    match: ThesisMatch,
    context: LiveTradeContext,
    score: DecisionScore,
) -> TradeIntent:
    return intent.model_copy(
        update={
            "metadata": {
                **dict(intent.metadata),
                "expected_return_bps": float(match.thesis.evidence.estimate_bps or 0.0),
                "expected_gross_edge_bps": float(match.thesis.evidence.estimate_bps or 0.0),
                "expected_net_edge_bps": float(match.thesis.evidence.net_expectancy_bps or 0.0),
                "expected_adverse_bps": float(
                    abs(match.thesis.expected_response.get("stop_value", 0.0) or 0.0) * 10_000.0
                ),
                "expected_downside_bps": float(score.expected_downside_bps),
                "expected_net_pnl_bps": float(score.expected_net_pnl_bps),
                "fill_probability": float(score.fill_probability),
                "edge_confidence": float(score.regime_reliability),
                "utility_score": float(score.utility_score),
                "probability_positive_post_cost": float(score.probability_positive_post_cost),
                "overlap_group_id": str(match.thesis.governance.overlap_group_id or ""),
                "governance_tier": str(match.thesis.governance.tier or ""),
                "operational_role": str(match.thesis.governance.operational_role or ""),
                "active_episode_ids": list(context.active_episode_ids),
                "active_event_ids": list(context.active_event_ids),
                "primary_event_id": str(context.primary_event_id or context.event_family),
                "canonical_regime": str(
                    _thesis_canonical_regime(match.thesis)
                    or context.canonical_regime
                    or context.regime_snapshot.get("canonical_regime", "")
                ),
                "compat_active_event_families": list(context.active_event_families),
                "compat_event_family": str(context.event_family),
                "compat_thesis_event_family": str(
                    match.thesis.event_family or match.thesis.primary_event_id or ""
                ),
                "thesis_canonical_regime": _thesis_canonical_regime(match.thesis),
                "meta_rank_score": float(match.thesis.evidence.rank_score or 0.0),
                "ranking_model": "expected_value_v1",
            }
        }
    )


def build_trade_intent_for_match(
    *,
    context: LiveTradeContext,
    match: ThesisMatch,
    policy_config: dict[str, Any] | None = None,
) -> tuple[DecisionScore, TradeIntent]:
    score = build_decision_score(match, context)
    intent = score_to_action(
        score=score,
        symbol=context.symbol,
        side=_resolve_trade_side(match, context),
        thesis_id=match.thesis.thesis_id,
        invalidation=match.thesis.invalidation,
        thresholds=thresholds_from_config(policy_config),
    )
    return score, _annotate_trade_intent(intent=intent, match=match, context=context, score=score)


def build_candidate_trade_outcomes(
    *,
    context: LiveTradeContext,
    ranked_matches: list[ThesisMatch],
    policy_config: dict[str, Any] | None = None,
) -> list[DecisionOutcome]:
    outcomes: list[DecisionOutcome] = []
    for match in ranked_matches:
        if not match.eligibility_passed:
            continue
        score, intent = build_trade_intent_for_match(
            context=context,
            match=match,
            policy_config=policy_config,
        )
        outcomes.append(
            DecisionOutcome(
                context=context,
                ranked_matches=ranked_matches,
                top_score=score,
                trade_intent=intent,
            )
        )
    return outcomes


@dataclass(frozen=True)
class DecisionOutcome:
    context: LiveTradeContext
    ranked_matches: list[ThesisMatch]
    top_score: DecisionScore | None
    trade_intent: TradeIntent


def decide_trade_intent(
    *,
    context: LiveTradeContext,
    thesis_store: ThesisStore,
    policy_config: dict[str, Any] | None = None,
    include_pending: bool = True,
) -> DecisionOutcome:
    profile_name = str(
        context.live_features.get("data_capability_profile")
        or context.signal_context.get("data_capability_profile")
        or (policy_config or {}).get("data_capability_profile")
        or ""
    ).strip() or None
    profile = load_data_capability_profile(profile_name)
    primary_event_id = str(context.primary_event_id or context.event_family).strip().upper()
    if profile.killed(primary_event_id):
        return _reject_context(context=context, reason="composite_killed")
    if not profile.trade_candidate(primary_event_id):
        return _reject_context(context=context, reason="not_composite_trade_candidate")
    mode = _runtime_mode(context, policy_config)
    if mode in {"paper", "paper_trading", "simulation"} and not profile.paper_approved(primary_event_id):
        return _reject_context(context=context, reason="composite_not_paper_approved")
    if mode in {"trading", "live", "live_trading"} and not profile.live_approved(primary_event_id):
        return _reject_context(context=context, reason="composite_not_live_approved")
    if not bool(context.trade_eligible):
        return _reject_context(context=context, reason="event_not_trade_eligible")
    if str(context.data_quality_flag).strip().lower() == "invalid":
        return _reject_context(context=context, reason="invalid_event_data_quality")

    matches = retrieve_ranked_theses(
        thesis_store=thesis_store,
        context=context,
        include_pending=include_pending,
    )
    if not matches:
        reject = TradeIntent(
            action="reject",
            symbol=context.symbol,
            side="flat",
            thesis_id="",
            support_score=0.0,
            contradiction_penalty=0.0,
            confidence_band="none",
            size_fraction=0.0,
            reasons_for=[],
            reasons_against=["no_matching_thesis"],
            metadata={
                "primary_event_id": str(context.primary_event_id or context.event_family),
                "canonical_regime": str(
                    context.canonical_regime or context.regime_snapshot.get("canonical_regime", "")
                ),
                "active_event_ids": list(context.active_event_ids),
                "compat_active_event_families": list(context.active_event_families),
                "active_episode_ids": list(context.active_episode_ids),
                "compat_event_family": str(context.event_family),
            },
        )
        return DecisionOutcome(
            context=context,
            ranked_matches=[],
            top_score=None,
            trade_intent=reject,
        )

    ranked_ev = rank_decisions_by_expected_value(matches=matches, context=context)
    if ranked_ev:
        ev_ordered_ids = [item.match.thesis.thesis_id for item in ranked_ev]
        by_id = {match.thesis.thesis_id: match for match in matches}
        ineligible = [match for match in matches if not match.eligibility_passed]
        matches = [by_id[item] for item in ev_ordered_ids if item in by_id] + ineligible

    top_match = ranked_ev[0].match if ranked_ev else matches[0]
    if not top_match.eligibility_passed:
        reject = TradeIntent(
            action="reject",
            symbol=context.symbol,
            side="flat",
            thesis_id=top_match.thesis.thesis_id,
            support_score=0.0,
            contradiction_penalty=min(1.0, float(top_match.contradiction_penalty)),
            confidence_band="none",
            size_fraction=0.0,
            invalidation=dict(top_match.thesis.invalidation or {}),
            reasons_for=list(top_match.reasons_for),
            reasons_against=list(top_match.reasons_against),
            metadata={
                "primary_event_id": str(context.primary_event_id or context.event_family),
                "canonical_regime": str(
                    _thesis_canonical_regime(top_match.thesis)
                    or context.canonical_regime
                    or context.regime_snapshot.get("canonical_regime", "")
                ),
                "active_event_ids": list(context.active_event_ids),
                "compat_active_event_families": list(context.active_event_families),
                "active_episode_ids": list(context.active_episode_ids),
                "compat_event_family": str(context.event_family),
                "overlap_group_id": str(top_match.thesis.governance.overlap_group_id or ""),
                "governance_tier": str(top_match.thesis.governance.tier or ""),
                "operational_role": str(top_match.thesis.governance.operational_role or ""),
                "compat_thesis_event_family": str(
                    top_match.thesis.event_family or top_match.thesis.primary_event_id or ""
                ),
                "thesis_canonical_regime": _thesis_canonical_regime(top_match.thesis),
                "meta_rank_score": float(top_match.thesis.evidence.rank_score or 0.0),
                "ranking_model": "expected_value_v1",
            },
        )
        return DecisionOutcome(
            context=context,
            ranked_matches=matches,
            top_score=None,
            trade_intent=reject,
        )

    score, intent = build_trade_intent_for_match(
        context=context,
        match=top_match,
        policy_config=policy_config,
    )
    return DecisionOutcome(
        context=context,
        ranked_matches=matches,
        top_score=score,
        trade_intent=intent,
    )
