from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from project.live.contracts.live_trade_context import LiveTradeContext
from project.live.contracts.trade_intent import TradeIntent
from project.live.policy import score_to_action, thresholds_from_config
from project.live.retriever import ThesisMatch, retrieve_ranked_theses
from project.live.scoring import DecisionScore, build_decision_score
from project.live.thesis_store import ThesisStore


def _thesis_canonical_regime(thesis) -> str:
    return str(
        thesis.canonical_regime
        or (thesis.supportive_context or {}).get("canonical_regime", "")
    ).strip().upper()


@dataclass(frozen=True)
class DecisionOutcome:
    context: LiveTradeContext
    ranked_matches: List[ThesisMatch]
    top_score: DecisionScore | None
    trade_intent: TradeIntent


def decide_trade_intent(
    *,
    context: LiveTradeContext,
    thesis_store: ThesisStore,
    policy_config: Dict[str, Any] | None = None,
    include_pending: bool = True,
) -> DecisionOutcome:
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

    top_match = matches[0]
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
            },
        )
        return DecisionOutcome(
            context=context,
            ranked_matches=matches,
            top_score=None,
            trade_intent=reject,
        )

    score = build_decision_score(top_match, context)
    side = "flat"
    if top_match.thesis.event_side == "long":
        side = "buy"
    elif top_match.thesis.event_side == "short":
        side = "sell"
    elif context.event_side == "long":
        side = "buy"
    elif context.event_side == "short":
        side = "sell"

    intent = score_to_action(
        score=score,
        symbol=context.symbol,
        side=side,
        thesis_id=top_match.thesis.thesis_id,
        invalidation=top_match.thesis.invalidation,
        thresholds=thresholds_from_config(policy_config),
    )
    intent = intent.model_copy(
        update={
            "metadata": {
                **dict(intent.metadata),
                "expected_return_bps": float(top_match.thesis.evidence.estimate_bps or 0.0),
                "expected_adverse_bps": float(abs(top_match.thesis.expected_response.get("stop_value", 0.0) or 0.0) * 10_000.0),
                "overlap_group_id": str(top_match.thesis.governance.overlap_group_id or ""),
                "governance_tier": str(top_match.thesis.governance.tier or ""),
                "operational_role": str(top_match.thesis.governance.operational_role or ""),
                "active_episode_ids": list(context.active_episode_ids),
                "active_event_ids": list(context.active_event_ids),
                "primary_event_id": str(context.primary_event_id or context.event_family),
                "canonical_regime": str(
                    _thesis_canonical_regime(top_match.thesis)
                    or context.canonical_regime
                    or context.regime_snapshot.get("canonical_regime", "")
                ),
                "compat_active_event_families": list(context.active_event_families),
                "compat_event_family": str(context.event_family),
                "compat_thesis_event_family": str(
                    top_match.thesis.event_family or top_match.thesis.primary_event_id or ""
                ),
                "thesis_canonical_regime": _thesis_canonical_regime(top_match.thesis),
                "meta_rank_score": float(top_match.thesis.evidence.rank_score or 0.0),
            }
        }
    )
    return DecisionOutcome(
        context=context,
        ranked_matches=matches,
        top_score=score,
        trade_intent=intent,
    )
